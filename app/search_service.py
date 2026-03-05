"""
搜尋服務層（OpenSearch + PostgreSQL）。

用途：
1. 解析與驗證搜尋參數（q/case_type/law+article+sub_ref）
2. 組裝 OpenSearch bool+nested 查詢（q 為 AND、法條組合為全 AND，純召回）
3. 提供 PostgreSQL baseline 搜尋（ILIKE，純召回）
4. 依 source_ids 取 per-citation rows 並在 Python 聚合目標排行

OpenSearch 查詢策略（記錄）：
- clean_text 使用 ngram analyzer（預設 2-gram）
- 每個空白分詞的 term 用 match_phrase（字元連續，等同 ILIKE）
- source_id 以 composite aggregation 分頁收集（純召回，不走 _score 排序）
"""

# ── Score 策略 ──────────────────────────────────────────────────────────
#
# 公式：score = keyword_score + statute_score（權重全部 +1）
#   keyword_score：每個 query_term（已 dedup）在 snippet 命中 → +1
#   statute_score：每組 law(+article)(+sub_ref) 在 citation_snippet_statutes 命中 → +1
#     law-only filter 用 EXISTS，同一法律只計一次
#
# 設計：score 只在一個地方計算
#   - 搜尋時：fetch_search_citation_rows 回傳 per-citation rows（含 score）
#     Python 聚合出 target 排行（SUM score），同時保留 per-citation 明細
#     前端展開 matched sources 時直接使用搜尋結果帶回的明細，不再打 API
#   - 展開 others 時：decisions.py 的 _others_citation_rows 獨立計算
#     （不同 citation 集合，非重複計算）
#
# 排序（citations 展開時）：
#   1. matched sources 排前面
#   2. 同組內 court_level ASC（最高法院 → 高等 → 地方）
#   3. 同層級 score DESC
#   律師引用重視判例位階，法院層級優先於文字相關度。
# ────────────────────────────────────────────────────────────────────────

import os
import re
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row
from etl.law_names import normalize_law_name


VALID_CASE_TYPES = {"民事", "刑事", "行政", "憲法"}


# ── 共用 score SQL builder ─────────────────────────────────────────────

def build_keyword_score_sql(
    query_terms: list[str], params: dict, snippet_col: str = "c.snippet",
) -> str:
    """每個 query_term 在 snippet 命中 → +1。回傳 SQL expression。"""
    if not query_terms:
        return "0"
    parts = []
    for idx, term in enumerate(query_terms):
        key = f"kw_{idx}"
        params[key] = f"%{term}%"
        parts.append(f"({snippet_col} ILIKE %({key})s)::int")
    return " + ".join(parts)


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
        key = (law, article, sub_ref)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def tokenize_query(q: str | None) -> list[str]:
    if not q or not q.strip():
        return []
    return [t.strip() for t in re.split(r"\s+", q.strip()) if t.strip()]


def parse_case_types(case_type_csv: str | None) -> list[str]:
    if not case_type_csv:
        return []
    values = [v.strip() for v in case_type_csv.split(",") if v.strip()]
    invalid = [v for v in values if v not in VALID_CASE_TYPES]
    if invalid:
        raise ValueError("case_type 僅支援：民事,刑事,行政,憲法")
    return _dedupe_keep_order(values)


def build_statute_filters(
    laws: list[str],
    articles: list[str],
    sub_refs: list[str],
) -> list[tuple[str, str | None, str | None]]:
    clean_laws = [normalize_law_name(x) for x in laws if x and x.strip()]
    clean_articles = [x.strip() for x in articles if x and x.strip()]
    clean_sub_refs = [x.strip() for x in sub_refs if x is not None]

    if not clean_laws and not clean_articles and not clean_sub_refs:
        return []
    if clean_articles and len(clean_articles) != len(clean_laws):
        raise ValueError("article 數量必須與 law 一致")
    if clean_sub_refs and len(clean_sub_refs) != len(clean_laws):
        raise ValueError("sub_ref 數量必須與 law 一致")

    out: list[tuple[str, str | None, str | None]] = []
    for idx, law in enumerate(clean_laws):
        article: str | None = clean_articles[idx] if clean_articles else None
        sub_ref: str | None = None
        if clean_sub_refs:
            sub = clean_sub_refs[idx]
            sub_ref = sub if sub else None
        out.append((law, article, sub_ref))
    return out


# ── OpenSearch ────────────────────────────────────────────────────────

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
        nested_must: list[dict[str, Any]] = [{"term": {"statutes.law": law}}]
        if article is not None:
            nested_must.append({"term": {"statutes.article_raw": article}})
        if sub_ref is not None:
            nested_must.append({"term": {"statutes.sub_ref": sub_ref}})
        filters.append({"nested": {"path": "statutes", "query": {"bool": {"must": nested_must}}}})

    must_not: list[dict[str, Any]] = [
        {"match_phrase": {"clean_text": term}}
        for term in exclude_terms
    ]
    for law, article, sub_ref in exclude_statute_filters:
        excl_must: list[dict[str, Any]] = [{"term": {"statutes.law": law}}]
        if article is not None:
            excl_must.append({"term": {"statutes.article_raw": article}})
        if sub_ref is not None:
            excl_must.append({"term": {"statutes.sub_ref": sub_ref}})
        must_not.append({"nested": {"path": "statutes", "query": {"bool": {"must": excl_must}}}})

    bool_query: dict[str, Any] = {"must": must, "filter": filters}
    if must_not:
        bool_query["must_not"] = must_not
    return {"bool": bool_query}


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
    index_name = os.environ.get("OPENSEARCH_INDEX", "decisions_v2")
    bool_query = build_opensearch_query(
        query_terms=query_terms,
        case_types=case_types,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
    )

    raw_page_size = (os.environ.get("OPENSEARCH_COMPOSITE_PAGE_SIZE", "1000") or "").strip()
    try:
        page_size = max(1, int(raw_page_size))
    except Exception:
        page_size = 1000

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


def search_source_ids_baseline_pg(
    conn: psycopg.Connection,
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str, str | None]],
    source_limit: int | None,
) -> list[int]:
    params: dict[str, Any] = {}
    where_parts: list[str] = []

    for idx, term in enumerate(query_terms):
        key = f"kw_{idx}"
        where_parts.append(f"d.clean_text ILIKE %({key})s")
        params[key] = f"%{term}%"

    if case_types:
        where_parts.append("d.case_type = ANY(%(case_types)s)")
        params["case_types"] = case_types

    for idx, (law, article, sub_ref) in enumerate(statute_filters):
        law_key = f"law_{idx}"
        clause = f"""
            EXISTS (
                SELECT 1
                FROM decision_reason_statutes drs
                WHERE drs.decision_id = d.id
                  AND drs.law = %({law_key})s
            """
        params[law_key] = law
        if article is not None:
            article_key = f"article_{idx}"
            clause += f"\n                  AND drs.article_raw = %({article_key})s"
            params[article_key] = article
        if sub_ref is not None:
            sub_key = f"sub_ref_{idx}"
            clause += f"\n                  AND drs.sub_ref = %({sub_key})s"
            params[sub_key] = sub_ref
        clause += "\n            )"
        where_parts.append(clause)

    for idx, term in enumerate(exclude_terms):
        key = f"excl_kw_{idx}"
        where_parts.append(f"d.clean_text NOT ILIKE %({key})s")
        params[key] = f"%{term}%"

    for idx, (law, article, sub_ref) in enumerate(exclude_statute_filters):
        law_key = f"excl_law_{idx}"
        clause = f"""
            NOT EXISTS (
                SELECT 1
                FROM decision_reason_statutes drs
                WHERE drs.decision_id = d.id
                  AND drs.law = %({law_key})s
            """
        params[law_key] = law
        if article is not None:
            article_key = f"excl_article_{idx}"
            clause += f"\n                  AND drs.article_raw = %({article_key})s"
            params[article_key] = article
        if sub_ref is not None:
            sub_key = f"excl_sub_ref_{idx}"
            clause += f"\n                  AND drs.sub_ref = %({sub_key})s"
            params[sub_key] = sub_ref
        clause += "\n            )"
        where_parts.append(clause)

    sql = f"""
        SELECT d.id
        FROM decisions d
        WHERE {" AND ".join(where_parts)}
          AND EXISTS (SELECT 1 FROM citations c WHERE c.source_id = d.id)
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)
        ids = [int(row["id"]) for row in cur.fetchall()]
        if source_limit is not None:
            return ids[:source_limit]
        return ids


# ── 搜尋結果：per-citation rows + Python 聚合 ─────────────────────────
#
# 設計理由：
# SQL 回傳 per-citation rows（含 score），Python 做兩件事：
#   1. 聚合出 target 排行（SUM score, COUNT）
#   2. 保留 per-citation 明細（給前端展開 matched sources 用）
# 這樣 score 只在 SQL 算一次，不會在展開時重複計算。
# 前端展開 matched sources 直接使用搜尋結果帶的明細，不再打 API。
# 展開 others 時才打 decisions.py 的 endpoint（不同 citation 集合）。
# ──────────────────────────────────────────────────────────────────────

def fetch_search_citation_rows(
    conn: psycopg.Connection,
    source_ids: list[int],
    query_terms: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
) -> list[dict[str, Any]]:
    """從 source_ids 取 per-citation rows，含 score + target/source info。

    同一 (source, target) 可能有多筆 citation（不同 match 位置），
    DISTINCT ON 保留 score 最高的那筆。
    """
    if not source_ids:
        return []

    params: dict[str, Any] = {"source_ids": source_ids}
    keyword_score_sql = build_keyword_score_sql(query_terms, params, "c.snippet")
    statute_score_sql = build_statute_score_sql(statute_filters, params, "c.id")

    sql = f"""
        WITH src AS (
            SELECT UNNEST(%(source_ids)s::bigint[]) AS source_id
        ),
        scored AS (
            SELECT
                c.id,
                c.source_id,
                c.target_id,
                c.target_authority_id,
                c.snippet,
                c.raw_match,
                c.target_doc_type,
                ({keyword_score_sql}) AS keyword_score,
                ({statute_score_sql}) AS statute_score
            FROM citations c
            JOIN src s ON s.source_id = c.source_id
        ),
        deduped AS (
            SELECT DISTINCT ON (
                source_id,
                COALESCE(target_id, -1),
                COALESCE(target_authority_id, -1)
            ) *
            FROM scored
            ORDER BY source_id,
                     COALESCE(target_id, -1),
                     COALESCE(target_authority_id, -1),
                     (keyword_score + statute_score) DESC,
                     id DESC
        )
        SELECT
            d.id              AS citation_id,
            d.source_id,
            d.target_id,
            d.target_authority_id,
            d.keyword_score,
            d.statute_score,
            d.keyword_score + d.statute_score AS score,
            d.snippet,
            d.raw_match,
            d.target_doc_type,
            -- target info
            COALESCE(td.root_norm, a.root_norm) AS target_root_norm,
            tcu.level       AS target_level,
            td.jyear        AS target_jyear,
            td.jcase_norm   AS target_jcase_norm,
            td.jno          AS target_jno,
            a.doc_type      AS auth_type,
            a.display       AS display_title,
            -- source info
            sd.unit_norm    AS source_unit_norm,
            scu.level       AS source_court_level,
            sd.jyear        AS source_jyear,
            sd.jcase_norm   AS source_jcase_norm,
            sd.jno          AS source_jno,
            sd.doc_type     AS source_doc_type,
            sd.decision_date AS source_decision_date
        FROM deduped d
        LEFT JOIN decisions td    ON td.id = d.target_id
        LEFT JOIN court_units tcu ON tcu.id = td.court_unit_id
        LEFT JOIN authorities a   ON a.id = d.target_authority_id
        JOIN decisions sd         ON sd.id = d.source_id
        LEFT JOIN court_units scu ON scu.id = sd.court_unit_id
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def aggregate_target_rankings(
    citation_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """把 per-citation rows 聚合成 target 排行。"""
    targets: dict[tuple, dict] = {}

    for row in citation_rows:
        key = (row["target_id"], row["target_authority_id"])
        if key not in targets:
            targets[key] = {
                "target_id": row["target_id"],
                "target_authority_id": row["target_authority_id"],
                "target_root_norm": row["target_root_norm"],
                "target_level": row["target_level"],
                "jyear": row.get("target_jyear"),
                "jcase_norm": row.get("target_jcase_norm"),
                "jno": row.get("target_jno"),
                "auth_type": row.get("auth_type"),
                "display_title": row.get("display_title"),
                "doc_type": row.get("target_doc_type") or row.get("auth_type"),
                "citation_count": 0,
                "score": 0,
            }
        t = targets[key]
        t["citation_count"] += 1
        t["score"] += row["score"]
        if row.get("target_doc_type") and not t.get("doc_type"):
            t["doc_type"] = row["target_doc_type"]

    result = list(targets.values())
    result.sort(key=lambda x: (-x["score"], -x["citation_count"]))
    return result


def fetch_total_citation_counts(
    conn: psycopg.Connection,
    target_ids: list[int],
    authority_ids: list[int],
) -> dict[tuple, int]:
    """批次查詢 total citation count（跨所有 source，不受搜尋條件限制）。

    Returns: {(target_id, None): count, (None, authority_id): count}
    """
    result: dict[tuple, int] = {}
    if not target_ids and not authority_ids:
        return result

    parts = []
    params: dict[str, Any] = {}

    if target_ids:
        parts.append("""
            SELECT target_id, NULL::bigint AS target_authority_id,
                   COUNT(DISTINCT source_id) AS cnt
            FROM citations
            WHERE target_id = ANY(%(target_ids)s)
            GROUP BY target_id
        """)
        params["target_ids"] = target_ids

    if authority_ids:
        parts.append("""
            SELECT NULL::bigint AS target_id, target_authority_id,
                   COUNT(DISTINCT source_id) AS cnt
            FROM citations
            WHERE target_authority_id = ANY(%(authority_ids)s)
            GROUP BY target_authority_id
        """)
        params["authority_ids"] = authority_ids

    sql = " UNION ALL ".join(parts)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        for row in cur.fetchall():
            key = (row["target_id"], row["target_authority_id"])
            result[key] = int(row["cnt"])
    return result


def fetch_css_statutes_batch(
    conn: psycopg.Connection,
    citation_ids: list[int],
) -> dict[int, list[dict]]:
    """批次取 citation_snippet_statutes。"""
    if not citation_ids:
        return {}

    sql = """
        SELECT citation_id, law, article_raw AS article, sub_ref
        FROM citation_snippet_statutes
        WHERE citation_id = ANY(%(ids)s)
        ORDER BY citation_id, law, article_raw, sub_ref
    """
    result: dict[int, list[dict]] = defaultdict(list)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"ids": citation_ids})
        for row in cur.fetchall():
            cid = row.pop("citation_id")
            result[cid].append(row)
    return dict(result)


# ── Legacy：保留舊版聚合函式供 main.py legacy endpoint 使用 ───────────

def fetch_rankings_by_source_ids(
    conn: psycopg.Connection,
    source_ids: list[int],
    query_terms: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    limit: int,
) -> list[dict[str, Any]]:
    """Legacy: 回傳聚合後的 target 排行。新程式碼請用 fetch_search_citation_rows。"""
    rows = fetch_search_citation_rows(conn, source_ids, query_terms, statute_filters)
    rankings = aggregate_target_rankings(rows)

    target_ids = [r["target_id"] for r in rankings if r["target_id"]]
    auth_ids = [r["target_authority_id"] for r in rankings if r["target_authority_id"]]
    total_counts = fetch_total_citation_counts(conn, target_ids, auth_ids)

    for r in rankings:
        key = (r["target_id"], r["target_authority_id"])
        r["total_citation_count"] = total_counts.get(key, 0)

    return rankings[:limit]
