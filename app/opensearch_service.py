"""
搜尋業務邏輯層（不依賴 FastAPI）。

職責：
- 參數解析與正規化：dedupe_query_terms、dedupe_statute_filters
- OpenSearch 召回：search_source_ids_opensearch（composite agg 分頁收集 source_ids）
- Target 排行：fetch_target_rankings_by_relevance（OpenSearch hits 聚合）
- Statute SQL builder：build_statute_score_sql
  供 citations.py 的 preview representative 選擇使用

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
from collections import Counter
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row
from etl.law_names import normalize_law_name


VALID_CASE_TYPES = {"民事", "刑事", "行政", "憲法"}
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
SOURCE_TARGET_STRICT_FILL_THRESHOLD = 200
SOURCE_TARGET_HOT_TERM_SOURCE_THRESHOLD = 10000
COURT_LEVEL_MAP = {
    "憲法法庭": 0,
    "最高法院": 1,
    "最高行政法院": 1,
    "高等法院": 2,
    "高等行政法院": 2,
    "智財商業法院": 2,
    "地方法院": 3,
    "少家法院": 3,
    "高等行政法院地方庭": 3,
    "地方法院簡易庭": 4,
}


def build_statute_score_sql(
    statute_filters: list[tuple], params: dict, citation_id_col: str = "c.id",
) -> str:
    """每組 law(+article)(+sub_ref) 在 css 命中 → +1。回傳 SQL expression。
    law-only filter 只查 law，即使 css 有多條也只 +1（EXISTS）。
    """
    if not statute_filters:
        return "0"
    parts = []
    for idx, (law, article, sub_ref) in enumerate(statute_filters):
        law_key = f"law_{idx}"
        params[law_key] = law
        inner = f"css.law = %({law_key})s"
        if article is not None:
            art_key = f"article_{idx}"
            inner += f" AND css.article_raw = %({art_key})s"
            params[art_key] = article
        if sub_ref is not None:
            sub_key = f"sub_ref_{idx}"
            inner += f" AND css.sub_ref = %({sub_key})s"
            params[sub_key] = sub_ref
        parts.append(
            f"(EXISTS (SELECT 1 FROM citation_snippet_statutes css"
            f" WHERE css.citation_id = {citation_id_col} AND {inner}))::int"
        )
    return " + ".join(parts)


# ── 參數解析 ──────────────────────────────────────────────────────────

def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def dedupe_query_terms(values: list[str]) -> list[str]:
    return _dedupe_keep_order([v.strip() for v in values if v and v.strip()])


def dedupe_statute_filters(
    values: list[tuple[str, str | None, str | None]]
) -> list[tuple[str, str | None, str | None]]:
    seen: set[tuple[str, str | None, str | None]] = set()
    out: list[tuple[str, str | None, str | None]] = []
    for law, article, sub_ref in values:
        norm_law = normalize_law_name(law)
        key = (norm_law, article, sub_ref)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def parse_case_types(case_type_csv: str | None) -> list[str]:
    if not case_type_csv:
        return []
    values = [v.strip() for v in case_type_csv.split(",") if v.strip()]
    invalid = [v for v in values if v not in VALID_CASE_TYPES]
    if invalid:
        raise ValueError("case_type 僅支援：民事,刑事,行政,憲法")
    return _dedupe_keep_order(values)


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

    bool_query: dict[str, Any] = {"must": must, "filter": filters}
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

    bool_query: dict[str, Any] = {"must": must, "filter": filters}
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

    bool_query: dict[str, Any] = {"filter": filters}
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

    raw_page_size = (os.environ.get("OPENSEARCH_COMPOSITE_PAGE_SIZE", "1000") or "").strip()
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

    raw_max_hits = (os.environ.get("OPENSEARCH_SOURCE_TARGET_MAX_HITS", "50000") or "").strip()
    raw_hits_per_chunk = (os.environ.get("OPENSEARCH_SOURCE_TARGET_HITS_PER_CHUNK", "5000") or "").strip()
    raw_source_chunk_size = (os.environ.get("OPENSEARCH_SOURCE_TARGET_SOURCE_CHUNK_SIZE", "5000") or "").strip()
    scroll_ttl = "1m"
    try:
        configured_max_hits = max(1, int(raw_max_hits))
    except Exception:
        configured_max_hits = 50000
    try:
        hits_per_chunk = max(1, int(raw_hits_per_chunk))
    except Exception:
        hits_per_chunk = 5000
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
                size=min(hits_per_chunk, hit_limit - yielded),
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

def search_source_target_hits_opensearch(
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
) -> list[dict[str, Any]]:
    hits = list(
        _iter_source_target_hits_opensearch(
            query_terms=query_terms,
            source_ids=source_ids,
            statute_filters=statute_filters,
            exclude_terms=exclude_terms,
            exclude_statute_filters=exclude_statute_filters,
            max_hits=max_hits,
            minimum_should_match=minimum_should_match,
            target_ids=target_ids or [],
            target_authority_ids=target_authority_ids or [],
        ) or []
    )

    hits.sort(
        key=lambda row: (
            -(row["score"] or 0),
            row["source_id"],
            row["target_id"] if row["target_id"] is not None else 2**63 - 1,
            row["target_authority_id"] if row["target_authority_id"] is not None else 2**63 - 1,
        )
    )
    return hits


def _should_use_target_uid_hot_term_aggregation(
    source_ids: list[int],
    query_terms: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
) -> bool:
    return (
        len(source_ids) >= SOURCE_TARGET_HOT_TERM_SOURCE_THRESHOLD
        and len(query_terms) == 1
        and not statute_filters
        and not exclude_terms
        and not exclude_statute_filters
    )


def _search_target_uid_counts_opensearch(
    query_terms: list[str],
    source_ids: list[int],
) -> dict[str, dict[str, Any]]:
    if not source_ids:
        return {}

    client = _get_opensearch_client()
    index_name = os.environ.get("OPENSEARCH_SOURCE_TARGET_INDEX", "source_target_windows_v2")

    raw_page_size = (os.environ.get("OPENSEARCH_COMPOSITE_PAGE_SIZE", "1000") or "").strip()
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
                            "ranked_source_ids": {
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
                        "ranked_source_ids": [],
                    },
                )
                row["matched_citation_count"] += int(bucket.get("doc_count") or 0)
                merged_source_ids = {
                    int(source_id)
                    for source_id in (row.get("ranked_source_ids") or [])
                }
                source_buckets = (
                    (bucket.get("ranked_source_ids") or {}).get("buckets") or []
                )
                for source_bucket in source_buckets:
                    try:
                        merged_source_ids.add(int(source_bucket.get("key")))
                    except Exception:
                        continue
                row["ranked_source_ids"] = sorted(merged_source_ids)[:5]

            after_key = agg.get("after_key")
            if not after_key:
                break

    return counts


def _fetch_decision_target_metadata(
    conn: psycopg.Connection,
    target_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not target_ids:
        return {}

    sql = """
        WITH raw_targets AS (
            SELECT DISTINCT UNNEST(%(target_ids)s::bigint[]) AS raw_target_id
        )
        SELECT
            td.id AS raw_target_id,
            canonical.id AS canonical_id,
            canonical.id AS target_id,
            NULL::bigint AS target_authority_id,
            canonical.root_norm AS court,
            canonical.jyear,
            canonical.jcase_norm,
            canonical.jno,
            canonical.display_title,
            COALESCE(canonical.canonical_doc_type, canonical.doc_type) AS doc_type,
            canonical.total_citation_count
        FROM raw_targets rt
        JOIN decisions td ON td.id = rt.raw_target_id
        JOIN decisions canonical ON canonical.id = COALESCE(td.canonical_id, td.id)
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"target_ids": target_ids})
        rows = cur.fetchall()

    return {
        int(row["raw_target_id"]): {
            "canonical_id": int(row["canonical_id"]),
            "target_id": int(row["target_id"]),
            "target_authority_id": None,
            "court": row["court"],
            "court_level": COURT_LEVEL_MAP.get(row["court"]),
            "jyear": row["jyear"],
            "jcase_norm": row["jcase_norm"],
            "jno": row["jno"],
            "display_title": row["display_title"],
            "doc_type": row["doc_type"],
            "total_citation_count": int(row["total_citation_count"] or 0),
        }
        for row in rows
    }


def _fetch_authority_target_metadata(
    conn: psycopg.Connection,
    authority_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not authority_ids:
        return {}

    sql = """
        SELECT
            a.id AS target_authority_id,
            a.root_norm AS court,
            a.display AS display_title,
            a.doc_type,
            a.total_citation_count
        FROM authorities a
        WHERE a.id = ANY(%(authority_ids)s::bigint[])
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"authority_ids": authority_ids})
        rows = cur.fetchall()

    return {
        int(row["target_authority_id"]): {
            "target_id": None,
            "target_authority_id": int(row["target_authority_id"]),
            "court": row["court"],
            "court_level": COURT_LEVEL_MAP.get(row["court"]),
            "jyear": None,
            "jcase_norm": None,
            "jno": None,
            "display_title": row["display_title"],
            "doc_type": row["doc_type"],
            "total_citation_count": int(row["total_citation_count"] or 0),
        }
        for row in rows
    }


def aggregate_source_target_hits_to_rankings(
    hits: list[dict[str, Any]],
    decision_meta: dict[int, dict[str, Any]],
    authority_meta: dict[int, dict[str, Any]],
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[dict[str, Any]]:
    """將 source-target hits 聚合成 search results。"""
    aggregated: dict[str, dict[str, Any]] = {}

    for hit in hits:
        target_id = hit.get("target_id")
        authority_id = hit.get("target_authority_id")
        if target_id is not None:
            meta = decision_meta.get(int(target_id))
            if not meta:
                continue
            target_key = f"decision:{int(meta['target_id'])}"
        else:
            meta = authority_meta.get(int(authority_id)) if authority_id is not None else None
            if not meta:
                continue
            target_key = f"authority:{int(meta['target_authority_id'])}"

        if doc_types and meta.get("doc_type") not in doc_types:
            continue
        if court_levels and meta.get("court_level") not in court_levels:
            continue

        row = aggregated.get(target_key)
        if row is None:
            row = {
                "target_id": meta.get("target_id"),
                "target_authority_id": meta.get("target_authority_id"),
                "court": meta.get("court"),
                "court_level": meta.get("court_level"),
                "jyear": meta.get("jyear"),
                "jcase_norm": meta.get("jcase_norm"),
                "jno": meta.get("jno"),
                "display_title": meta.get("display_title"),
                "doc_type": meta.get("doc_type"),
                "total_citation_count": int(meta.get("total_citation_count") or 0),
                "matched_citation_count": 0,
                "score": 0.0,
                "_source_scores": {},
                "_matched_source_ids": set(),
            }
            aggregated[target_key] = row

        source_id = int(hit["source_id"])
        row["_matched_source_ids"].add(source_id)
        row["_source_scores"][source_id] = float(hit.get("score") or 0)

    rankings = []
    for row in aggregated.values():
        row["matched_citation_count"] = len(row["_matched_source_ids"])
        ranked_source_rows = sorted(
            row["_source_scores"].items(),
            key=lambda item: (-(item[1] or 0), item[0]),
        )[:5]
        row["ranked_source_ids"] = [int(source_id) for source_id, _score in ranked_source_rows]
        top_scores = [float(score or 0) for _source_id, score in ranked_source_rows]
        row["score"] = float(sum(top_scores) / len(top_scores)) if top_scores else 0.0
        del row["_source_scores"]
        del row["_matched_source_ids"]
        rankings.append(row)

    rankings.sort(key=lambda row: (
        -(row["score"] or 0),
        -(row["matched_citation_count"] or 0),
        -(row["total_citation_count"] or 0),
        (row["court_level"] if row["court_level"] is not None else 99),
    ))
    return rankings


def _build_rankings_from_target_uid_counts(
    conn: psycopg.Connection,
    target_counts: dict[str, dict[str, Any]],
    *,
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[dict[str, Any]]:
    if not target_counts:
        return []

    decision_ids = sorted(
        int(target_uid.split(":", 1)[1])
        for target_uid in target_counts
        if target_uid.startswith("decision:")
    )
    authority_ids = sorted(
        int(target_uid.split(":", 1)[1])
        for target_uid in target_counts
        if target_uid.startswith("authority:")
    )
    decision_meta = _fetch_decision_target_metadata(conn, decision_ids)
    authority_meta = _fetch_authority_target_metadata(conn, authority_ids)

    rankings = []
    for target_uid, stats in target_counts.items():
        kind, raw_id = target_uid.split(":", 1)
        meta: dict[str, Any] | None
        if kind == "decision":
            meta = decision_meta.get(int(raw_id))
        else:
            meta = authority_meta.get(int(raw_id))
        if not meta:
            continue
        if doc_types and meta.get("doc_type") not in doc_types:
            continue
        if court_levels and meta.get("court_level") not in court_levels:
            continue

        rankings.append(
            {
                "target_id": meta.get("target_id"),
                "target_authority_id": meta.get("target_authority_id"),
                "court": meta.get("court"),
                "court_level": meta.get("court_level"),
                "jyear": meta.get("jyear"),
                "jcase_norm": meta.get("jcase_norm"),
                "jno": meta.get("jno"),
                "display_title": meta.get("display_title"),
                "doc_type": meta.get("doc_type"),
                "total_citation_count": int(meta.get("total_citation_count") or 0),
                "matched_citation_count": int(stats.get("matched_citation_count") or 0),
                "score": 1.0,
                "ranked_source_ids": sorted(
                    int(source_id)
                    for source_id in (stats.get("ranked_source_ids") or [])
                )[:5],
            }
        )

    rankings.sort(
        key=lambda row: (
            -(row["matched_citation_count"] or 0),
            -(row["total_citation_count"] or 0),
            row["court_level"] if row["court_level"] is not None else 99,
            _ranking_target_key(row),
        )
    )
    return rankings


def _build_rankings_from_hits(
    conn: psycopg.Connection,
    hits: list[dict[str, Any]],
    *,
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[dict[str, Any]]:
    if not hits:
        return []

    target_ids = sorted({
        int(hit["target_id"])
        for hit in hits
        if hit.get("target_id") is not None
    })
    authority_ids = sorted({
        int(hit["target_authority_id"])
        for hit in hits
        if hit.get("target_authority_id") is not None
    })
    decision_meta = _fetch_decision_target_metadata(conn, target_ids)
    authority_meta = _fetch_authority_target_metadata(conn, authority_ids)
    return aggregate_source_target_hits_to_rankings(
        hits,
        decision_meta,
        authority_meta,
        doc_types=doc_types,
        court_levels=court_levels,
    )


def _ranking_target_key(row: dict[str, Any]) -> tuple[str, int]:
    target_id = row.get("target_id")
    if target_id is not None:
        return ("decision", int(target_id))
    return ("authority", int(row["target_authority_id"]))


def _merge_rankings_keep_strict_first(
    primary_rankings: list[dict[str, Any]],
    fallback_rankings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged = list(primary_rankings)
    seen = {_ranking_target_key(row) for row in primary_rankings}
    for row in fallback_rankings:
        key = _ranking_target_key(row)
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)
    return merged


def fetch_target_rankings_by_relevance(
    conn: psycopg.Connection,
    source_ids: list[int],
    query_terms: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
    *,
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[dict[str, Any]]:
    """以 source-target window hits 聚合 target relevance 排行。"""
    if not source_ids:
        return []

    if _should_use_target_uid_hot_term_aggregation(
        source_ids,
        query_terms,
        statute_filters,
        exclude_terms,
        exclude_statute_filters,
    ):
        target_counts = _search_target_uid_counts_opensearch(
            query_terms=query_terms,
            source_ids=source_ids,
        )
        if target_counts:
            return _build_rankings_from_target_uid_counts(
                conn,
                target_counts,
                doc_types=doc_types,
                court_levels=court_levels,
            )

    strict_hits = search_source_target_hits_opensearch(
        query_terms=query_terms,
        source_ids=source_ids,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
        minimum_should_match=1,
    )
    if not strict_hits:
        fallback_hits = search_source_target_hits_opensearch(
            query_terms=query_terms,
            source_ids=source_ids,
            statute_filters=statute_filters,
            exclude_terms=exclude_terms,
            exclude_statute_filters=exclude_statute_filters,
            minimum_should_match=None,
        )
        return _build_rankings_from_hits(
            conn,
            fallback_hits,
            doc_types=doc_types,
            court_levels=court_levels,
        )

    strict_rankings = _build_rankings_from_hits(
        conn,
        strict_hits,
        doc_types=doc_types,
        court_levels=court_levels,
    )
    if len(strict_rankings) >= SOURCE_TARGET_STRICT_FILL_THRESHOLD:
        return strict_rankings

    fallback_hits = search_source_target_hits_opensearch(
        query_terms=query_terms,
        source_ids=source_ids,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
        minimum_should_match=None,
    )
    fallback_rankings = _build_rankings_from_hits(
        conn,
        fallback_hits,
        doc_types=doc_types,
        court_levels=court_levels,
    )
    return _merge_rankings_keep_strict_first(strict_rankings, fallback_rankings)
