"""
搜尋業務邏輯層（不依賴 FastAPI）。

職責：
- Stage 1 召回：search_source_ids_opensearch（composite agg 分頁收集 source_ids）
- Stage 2 target ranking：search_target_rankings_step_down（msm 階梯式 composite agg）

OpenSearch 查詢策略：
- clean_text / window_text_snippet 使用 ngram analyzer（2-gram）
- 每個 term 用 match_phrase（字元連續，等同 ILIKE）
- source_id / target_uid 以 composite aggregation 分頁收集
"""

import os
from typing import Any
from urllib.parse import urlparse


def _build_opensearch_statute_nested_query(
    law: str,
    article: str | None,
    sub_ref: str | None,
    *,
    path: str = "statutes",
) -> dict[str, Any]:
    nested_filter: list[dict[str, Any]] = [{"term": {f"{path}.law": law}}]
    if article is not None:
        nested_filter.append({"term": {f"{path}.article_raw": article}})
    if sub_ref is not None:
        # prefix 比對：搜尋「第1項」可命中「第1項前段」、「第1項第1款」等
        nested_filter.append({"prefix": {f"{path}.sub_ref": sub_ref}})
    return {"nested": {"path": path, "query": {"bool": {"filter": nested_filter}}}}

def build_opensearch_query(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str, str | None]],
) -> dict[str, Any]:
    # Stage 1 只用 composite agg 收 source_ids（字典序），不用 BM25。
    # 把所有 match 條件放 filter context 跳過 score 計算，並讓 OS 可以 cache 結果。
    filters: list[dict[str, Any]] = [
        {"match_phrase": {"clean_text": term}}
        for term in query_terms
    ]
    if case_types:
        filters.append({"terms": {"case_type": case_types}})
    for law, article, sub_ref in statute_filters:
        filters.append(
            _build_opensearch_statute_nested_query(law, article, sub_ref)
        )

    must_not: list[dict[str, Any]] = [
        {"match_phrase": {"clean_text": term}}
        for term in exclude_terms
    ]
    for law, article, sub_ref in exclude_statute_filters:
        must_not.append(
            _build_opensearch_statute_nested_query(law, article, sub_ref)
        )

    bool_query: dict[str, Any] = {}
    if filters:
        bool_query["filter"] = filters
    if must_not:
        bool_query["must_not"] = must_not

    return {"bool": bool_query}


def _build_source_target_relevance_bool_query(
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    minimum_should_match: int | None,
) -> dict[str, Any]:
    should: list[dict[str, Any]] = [
        {"match_phrase": {"window_text_snippet": term}}
        for term in query_terms
    ]
    for law, article, sub_ref in statute_filters:
        should.append(
            _build_opensearch_statute_nested_query(law, article, sub_ref)
        )

    bool_query: dict[str, Any] = {}
    if source_ids:
        bool_query["filter"] = [{"terms": {"source_id": source_ids}}]
    if should:
        bool_query["should"] = should
        if minimum_should_match is not None:
            bool_query["minimum_should_match"] = minimum_should_match
    return bool_query


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"true"}


def _get_opensearch_client():
    try:
        from opensearchpy import OpenSearch
    except Exception as exc:
        raise RuntimeError("缺少 opensearch-py 套件") from exc

    url = os.environ.get("OPENSEARCH_URL", "https://localhost:9200").strip()
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or 9200
    use_ssl = parsed.scheme == "https"
    verify_certs = _env_bool("OPENSEARCH_VERIFY_CERTS", False)

    username = os.environ.get("OPENSEARCH_USERNAME", "").strip()
    password = os.environ.get("OPENSEARCH_PASSWORD", "").strip()
    auth = (username, password) if username else None

    kwargs: dict[str, Any] = {
        "hosts": [{"host": host, "port": port}],
        "http_auth": auth,
        "use_ssl": use_ssl,
        "verify_certs": verify_certs,
    }
    if use_ssl and not verify_certs:
        kwargs["ssl_assert_hostname"] = False
    return OpenSearch(**kwargs)


def search_source_ids_opensearch(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str, str | None]],
) -> list[int]:
    client = _get_opensearch_client()
    index_name = "decisions_v3"
    page_size = 1000

    def collect_source_ids(bool_query: dict[str, Any]) -> list[int]:
        source_ids: list[int] = []
        seen: set[int] = set()
        after_key: dict[str, Any] | None = None
        while True:
            composite: dict[str, Any] = {
                "size": page_size,
                "sources": [{"source_id": {"terms": {"field": "source_id"}}}],
            }
            if after_key is not None:
                composite["after"] = after_key

            body = {
                "size": 0,
                "query": bool_query,
                "aggs": {"source_ids": {"composite": composite}},
            }
            response = client.search(index=index_name, body=body)
            agg = (response.get("aggregations") or {}).get("source_ids") or {}
            buckets = agg.get("buckets") or []

            for bucket in buckets:
                raw_id = (bucket.get("key") or {}).get("source_id")
                try:
                    source_id = int(raw_id)
                except Exception:
                    continue
                if source_id in seen:
                    continue
                seen.add(source_id)
                source_ids.append(source_id)

            after_key = agg.get("after_key")
            if not after_key:
                break

        return source_ids

    exact_query = build_opensearch_query(
        query_terms=query_terms,
        case_types=case_types,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
    )
    return collect_source_ids(exact_query)


def chunk_source_ids(source_ids: list[int], chunk_size: int) -> list[list[int]]:
    """依固定大小切 source_id chunks。"""
    if chunk_size <= 0:
        raise ValueError("chunk_size 必須 > 0")
    
    chunks = []

    for idx in range(0, len(source_ids), chunk_size):
        chunk = source_ids[idx: idx + chunk_size]
        chunks.append(chunk)

    return chunks


def _aggregate_targets_at_msm(
    *,
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    minimum_should_match: int | None,
) -> set[str]:
    """在某個 msm 下，所有 source chunks、所有 composite pages 合併後的 matched target_uid 集合。"""
    client = _get_opensearch_client()

    index_name = "source_target_windows_v2"
    page_size = 1000 # 設定 composite aggregation 每頁 1000 個 buckets
    source_chunk_size = 5000 # 每次查詢限制在 5000 個 source_ids 以內，避免 bool terms source_id query 太大導致效能問題，調大實測反而會更慢，因為OS 對 terms filter 內部用 set 比對，集合越大每個 doc 比對成本越高（HashSet 遍歷 + 記憶體 footprint）

    matched: set[str] = set()

    for source_id_chunk in chunk_source_ids(source_ids, source_chunk_size):
        bool_query = _build_source_target_relevance_bool_query(
            query_terms=query_terms,
            source_ids=source_id_chunk,
            statute_filters=statute_filters,
            minimum_should_match=minimum_should_match,
        )
        after_key: dict[str, Any] | None = None
        while True:
            composite: dict[str, Any] = {
                "size": page_size,
                "sources": [{"target_uid": {"terms": {"field": "target_uid"}}}],
            }
            if after_key is not None:
                composite["after"] = after_key

            body = {
                "size": 0,
                "query": {"bool": bool_query},
                "aggs": {"targets": {"composite": composite}},
            }

            response = client.search(index=index_name, body=body)

            agg = (response.get("aggregations") or {}).get("targets") or {}
            for bucket in agg.get("buckets") or []:
                target_uid = (bucket.get("key") or {}).get("target_uid")
                if isinstance(target_uid, str) and target_uid:
                    matched.add(target_uid)

            after_key = agg.get("after_key")
            if not after_key:
                break

    return matched


def search_target_rankings_step_down(
    *,
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    threshold: int = 200,
) -> list[dict[str, Any]]:
    """
    階梯式 step_down：msm=N → N-1 → ... → 1 → None（filter-only）。
    每階用 composite agg 抓 matched target_uid，pool 累積達 threshold 就停。
    每個 target 記錄首次入池的 msm 為 reached_at_msm（None fallback 記為 0）。
    """
    if not source_ids:
        return []

    should_count = len(query_terms) + len(statute_filters)
    msm_ladder: list[int | None] = list(range(should_count, 0, -1)) + [None]

    pool: dict[str, dict[str, Any]] = {}
    for msm in msm_ladder:
        matched_uids = _aggregate_targets_at_msm(
            query_terms=query_terms,
            source_ids=source_ids,
            statute_filters=statute_filters,
            minimum_should_match=msm,
        )
        for target_uid in matched_uids:
            if target_uid in pool:
                continue
            pool[target_uid] = {"reached_at_msm": msm if msm is not None else 0}
        if len(pool) >= threshold:
            break

    return [{"target_uid": tu, **row} for tu, row in pool.items()]
