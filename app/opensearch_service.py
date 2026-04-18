"""
搜尋業務邏輯層（不依賴 FastAPI）。

職責：
- OpenSearch 召回：search_source_ids_opensearch（composite agg 分頁收集 source_ids）
- Source-target retrieval：hits / target_uid counts

OpenSearch 查詢策略：
- clean_text 使用 ngram analyzer（2-gram）
- 每個 term 用 match_phrase（字元連續，等同 ILIKE）
- source_id 以 composite aggregation 分頁收集（純召回，不走 _score 排序）
"""

# ── Score 策略 ──────────────────────────────────────────────────────────
#
# /search relevance：
# - 每個 query term 在 snippet 命中 → +1
# - 每組 statute filter 命中 → +1
# - target score 由 Python 聚合 source-target hits 計算
#
# /citations：
# - 目前不再計算 citation-level score
# - 只保留法條命中布林判斷所需 SQL builder
# ────────────────────────────────────────────────────────────────────────

import os
import re
from typing import Any
from urllib.parse import urlparse

SOURCE_TARGET_WINDOW_CONFIGS = (
    ("window_text_snippet", "snippet", 1.0),
)
SOURCE_TARGET_WINDOW_BUCKET_WEIGHTS = {
    bucket: boost
    for _field, bucket, boost in SOURCE_TARGET_WINDOW_CONFIGS
}
SOURCE_TARGET_MATCH_NAME_RE = re.compile(
    r"^t(?P<term_idx>\d+):(?P<bucket>snippet)$"
)
SOURCE_TARGET_STATUTE_MATCH_NAME_RE = re.compile(r"^st(?P<filter_idx>\d+)$")


# ── OpenSearch ────────────────────────────────────────────────────────


def _build_opensearch_statute_nested_query(
    law: str,
    article: str | None,
    sub_ref: str | None,
    *,
    path: str = "statutes",
    query_name: str | None = None,
) -> dict[str, Any]:
    nested_must: list[dict[str, Any]] = [{"term": {f"{path}.law": law}}]
    if article is not None:
        nested_must.append({"term": {f"{path}.article_raw": article}})
    if sub_ref is not None:
        # prefix 比對：搜尋「第1項」可命中「第1項前段」、「第1項第1款」等
        nested_must.append({"prefix": {f"{path}.sub_ref": sub_ref}})
    nested_query: dict[str, Any] = {
        "nested": {"path": path, "query": {"bool": {"must": nested_must}}}
    }
    if query_name is not None:
        nested_query["nested"]["_name"] = query_name
    return nested_query

def build_opensearch_query(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str, str | None]],
) -> dict[str, Any]:
    must = [
        {"match_phrase": {"clean_text": term}}
        for term in query_terms
    ]

    filters: list[dict[str, Any]] = []
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
    if must:
        bool_query["must"] = must
    if filters:
        bool_query["filter"] = filters
    if must_not:
        bool_query["must_not"] = must_not

    return {"bool": bool_query}


def _format_textual_statute_article(article: str) -> str:
    if "之" not in article:
        return f"第{article}條"
    head, tail = article.split("之", 1)
    return f"第{head}條之{tail}"


def _build_opensearch_textual_statute_clause(
    law: str,
    article: str | None,
    sub_ref: str | None,
) -> dict[str, Any]:
    must: list[dict[str, Any]] = [
        {"match_phrase": {"clean_text": law}},
    ]
    if article is not None:
        must.append({"match_phrase": {"clean_text": _format_textual_statute_article(article)}})
    if sub_ref is not None:
        must.append({"match_phrase": {"clean_text": sub_ref}})
    return {"bool": {"must": must}}


def build_opensearch_textual_statute_query(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
) -> dict[str, Any]:
    must = [
        {"match_phrase": {"clean_text": term}}
        for term in query_terms
    ]
    must.extend(
        _build_opensearch_textual_statute_clause(law, article, sub_ref)
        for law, article, sub_ref in statute_filters
    )

    filters: list[dict[str, Any]] = []
    if case_types:
        filters.append({"terms": {"case_type": case_types}})

    must_not: list[dict[str, Any]] = [
        {"match_phrase": {"clean_text": term}}
        for term in exclude_terms
    ]
    must_not.extend(
        _build_opensearch_textual_statute_clause(law, article, sub_ref)
        for law, article, sub_ref in exclude_statute_filters
    )

    bool_query: dict[str, Any] = {}
    if must:
        bool_query["must"] = must
    if filters:
        bool_query["filter"] = filters
    if must_not:
        bool_query["must_not"] = must_not

    return {"bool": bool_query}


def build_source_target_rerank_query(
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    *,
    size: int = 200,
    minimum_should_match: int | None = None,
    exclude_terms: list[str] | None = None,
    exclude_statute_filters: list[tuple[str, str | None, str | None]] | None = None,
    target_ids: list[int] | None = None,
    target_authority_ids: list[int] | None = None,
) -> dict[str, Any]:
    bool_query = _build_source_target_relevance_bool_query(
        query_terms=query_terms,
        source_ids=source_ids,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms or [],
        exclude_statute_filters=exclude_statute_filters or [],
        target_ids=target_ids or [],
        target_authority_ids=target_authority_ids or [],
        minimum_should_match=minimum_should_match,
    )

    body = {
        "size": size,
        "_source": [
            "source_id",
            "target_id",
            "target_authority_id",
            "target_uid",
        ],
        "query": {"bool": bool_query},
        "sort": ["_doc"],
    }
    return body


def _build_source_target_relevance_bool_query(
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
    target_ids: list[int],
    target_authority_ids: list[int],
    minimum_should_match: int | None,
) -> dict[str, Any]:
    filters: list[dict[str, Any]] = []
    should: list[dict[str, Any]] = []
    if source_ids:
        filters.append({"terms": {"source_id": source_ids}})
    if target_ids:
        filters.append({"terms": {"target_id": target_ids}})
    if target_authority_ids:
        filters.append({"terms": {"target_authority_id": target_authority_ids}})

    if query_terms:
        for idx, term in enumerate(query_terms):
            for field, bucket, _boost in SOURCE_TARGET_WINDOW_CONFIGS:
                should.append(
                    {
                        "match_phrase": {
                            field: {
                                "query": term,
                                "_name": f"t{idx}:{bucket}",
                            }
                        }
                    }
                )

    bool_query: dict[str, Any] = {}
    if filters:
        bool_query["filter"] = filters
    if statute_filters:
        should.extend(
            _build_opensearch_statute_nested_query(
                law,
                article,
                sub_ref,
                query_name=f"st{idx}",
            )
            for idx, (law, article, sub_ref) in enumerate(statute_filters)
        )
    if should:
        bool_query["should"] = should
        if minimum_should_match is not None:
            bool_query["minimum_should_match"] = minimum_should_match
            
    return bool_query


def calculate_source_target_match_score(matched_queries: list[str] | None) -> float:
    """依 named query 命中結果重建 source-target relevance 分數。"""
    if not matched_queries:
        return 0.0

    best_weights: dict[int, float] = {}
    matched_statute_filters: set[int] = set()
    for raw_name in matched_queries:
        if not isinstance(raw_name, str):
            continue
        match = SOURCE_TARGET_MATCH_NAME_RE.match(raw_name)
        if match:
            term_idx = int(match.group("term_idx"))
            bucket = match.group("bucket")
            weight = float(SOURCE_TARGET_WINDOW_BUCKET_WEIGHTS[bucket])
            prev = best_weights.get(term_idx)
            if prev is None or weight > prev:
                best_weights[term_idx] = weight
            continue
        statute_match = SOURCE_TARGET_STATUTE_MATCH_NAME_RE.match(raw_name)
        if statute_match:
            matched_statute_filters.add(int(statute_match.group("filter_idx")))
    return float(sum(best_weights.values()) + len(matched_statute_filters))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


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
    source_limit: int | None,
) -> list[int]:
    client = _get_opensearch_client()
    index_name = os.environ.get("OPENSEARCH_INDEX", "decisions_v3")

    raw_page_size = (
        os.environ.get("OPENSEARCH_SOURCE_RECALL_COMPOSITE_PAGE_SIZE", "1000") or ""
    ).strip()
    try:
        page_size = max(1, int(raw_page_size))
    except Exception:
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
                if source_limit is not None and len(source_ids) >= source_limit:
                    return source_ids

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
    source_ids = collect_source_ids(exact_query)
    if source_ids or not query_terms or not statute_filters:
        return source_ids

    textual_query = build_opensearch_textual_statute_query(
        query_terms=query_terms,
        case_types=case_types,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
    )
    return collect_source_ids(textual_query)


def chunk_source_ids(source_ids: list[int], chunk_size: int) -> list[list[int]]:
    """依固定大小切 source_id chunks，保留原順序。"""
    if chunk_size <= 0:
        raise ValueError("chunk_size 必須 > 0")
    return [
        source_ids[idx: idx + chunk_size]
        for idx in range(0, len(source_ids), chunk_size)
    ]


def _iter_source_target_hits_opensearch(
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
    *,
    max_hits: int | None = None,
    minimum_should_match: int | None = None,
    target_ids: list[int] | None = None,
    target_authority_ids: list[int] | None = None,
) -> Any:
    """查 source-target window index，逐筆產出以 named query flags 重建分數的 pair hits。"""
    if not source_ids:
        return

    client = _get_opensearch_client()
    index_name = os.environ.get("OPENSEARCH_SOURCE_TARGET_INDEX", "source_target_windows_v2")

    raw_max_hits = (os.environ.get("OPENSEARCH_SOURCE_TARGET_MAX_HITS", "40000") or "").strip()
    raw_scroll_page_size = (
        os.environ.get("OPENSEARCH_SOURCE_TARGET_SCROLL_PAGE_SIZE", "5000") or ""
    ).strip()
    raw_source_chunk_size = (os.environ.get("OPENSEARCH_SOURCE_TARGET_SOURCE_CHUNK_SIZE", "5000") or "").strip()
    scroll_ttl = "1m"
    try:
        configured_max_hits = max(1, int(raw_max_hits))
    except Exception:
        configured_max_hits = 50000
    try:
        scroll_page_size = max(1, int(raw_scroll_page_size))
    except Exception:
        scroll_page_size = 5000
    try:
        source_chunk_size = max(1, int(raw_source_chunk_size))
    except Exception:
        source_chunk_size = 5000

    hit_limit = configured_max_hits if max_hits is None else max(1, max_hits)
    yielded = 0

    for source_id_chunk in chunk_source_ids(source_ids, source_chunk_size):
        if yielded >= hit_limit:
            break
        scroll_id: str | None = None
        try:
            body = build_source_target_rerank_query(
                query_terms=query_terms,
                source_ids=source_id_chunk,
                statute_filters=statute_filters,
                exclude_terms=exclude_terms,
                exclude_statute_filters=exclude_statute_filters,
                size=min(scroll_page_size, hit_limit - yielded),
                minimum_should_match=minimum_should_match,
                target_ids=target_ids or [],
                target_authority_ids=target_authority_ids or [],
            )
            response = client.search(index=index_name, body=body, scroll=scroll_ttl)
            scroll_id = response.get("_scroll_id")

            while yielded < hit_limit:
                response_hits = ((response.get("hits") or {}).get("hits") or [])
                if not response_hits:
                    break

                for hit in response_hits:
                    source = hit.get("_source") or {}
                    try:
                        source_id = int(source.get("source_id"))
                    except Exception:
                        continue
                    target_id = source.get("target_id")
                    target_authority_id = source.get("target_authority_id")
                    yield {
                        "source_id": source_id,
                        "target_id": int(target_id) if target_id is not None else None,
                        "target_authority_id": (
                            int(target_authority_id)
                            if target_authority_id is not None else None
                        ),
                        "score": calculate_source_target_match_score(
                            hit.get("matched_queries")
                        ),
                    }
                    yielded += 1
                    if yielded >= hit_limit:
                        break

                if yielded >= hit_limit or len(response_hits) < body["size"] or not scroll_id:
                    break

                response = client.scroll(scroll_id=scroll_id, scroll=scroll_ttl)
                scroll_id = response.get("_scroll_id") or scroll_id
        finally:
            if scroll_id:
                try:
                    client.clear_scroll(scroll_id=scroll_id)
                except Exception:
                    pass

def _search_target_uid_counts_opensearch(
    query_terms: list[str],
    source_ids: list[int],
) -> dict[str, dict[str, Any]]:
    if not source_ids:
        return {}

    client = _get_opensearch_client()
    index_name = os.environ.get("OPENSEARCH_SOURCE_TARGET_INDEX", "source_target_windows_v2")

    raw_page_size = (
        os.environ.get("OPENSEARCH_TARGET_AGG_COMPOSITE_PAGE_SIZE", "1000") or ""
    ).strip()
    raw_source_chunk_size = (
        os.environ.get("OPENSEARCH_SOURCE_TARGET_SOURCE_CHUNK_SIZE", "5000") or ""
    ).strip()
    try:
        page_size = max(1, int(raw_page_size))
    except Exception:
        page_size = 1000
    try:
        source_chunk_size = max(1, int(raw_source_chunk_size))
    except Exception:
        source_chunk_size = 5000

    counts: dict[str, dict[str, Any]] = {}
    for source_id_chunk in chunk_source_ids(source_ids, source_chunk_size):
        bool_query = _build_source_target_relevance_bool_query(
            query_terms=query_terms,
            source_ids=source_id_chunk,
            statute_filters=[],
            exclude_terms=[],
            exclude_statute_filters=[],
            target_ids=[],
            target_authority_ids=[],
            minimum_should_match=1,
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
                "aggs": {
                    "targets": {
                        "composite": composite,
                        "aggs": {
                            "preview_source_ids": {
                                "terms": {
                                    "field": "source_id",
                                    "size": 5,
                                    "order": {"_key": "asc"},
                                }
                            }
                        },
                    }
                },
            }
            response = client.search(index=index_name, body=body)
            agg = (response.get("aggregations") or {}).get("targets") or {}
            buckets = agg.get("buckets") or []
            for bucket in buckets:
                target_uid = (bucket.get("key") or {}).get("target_uid")
                if not isinstance(target_uid, str) or not target_uid:
                    continue
                row = counts.setdefault(
                    target_uid,
                    {
                        "matched_citation_count": 0,
                        "preview_source_ids": [],
                    },
                )
                row["matched_citation_count"] += int(bucket.get("doc_count") or 0)
                merged_source_ids = {
                    int(source_id)
                    for source_id in (row.get("preview_source_ids") or [])
                }
                source_buckets = (
                    (bucket.get("preview_source_ids") or {}).get("buckets") or []
                )
                for source_bucket in source_buckets:
                    try:
                        merged_source_ids.add(int(source_bucket.get("key")))
                    except Exception:
                        continue
                row["preview_source_ids"] = sorted(merged_source_ids)[:5]

            after_key = agg.get("after_key")
            if not after_key:
                break

    return counts
