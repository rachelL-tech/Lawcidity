"""
搜尋業務邏輯層（不依賴 FastAPI）。

職責：
- 參數解析與正規化：dedupe_query_terms、dedupe_statute_filters、build_statute_filters
- OpenSearch 召回：search_source_ids_opensearch（composite agg 分頁收集 source_ids）
- PostgreSQL baseline 召回：search_source_ids_baseline_pg（ILIKE，供比對用）
- Target 排行：fetch_target_rankings（SQL 聚合，單次查詢）
- Score SQL builders：build_keyword_score_sql、build_statute_score_sql
  供 search.py（target 排行）與 citations.py（展開排序）共用

OpenSearch 查詢策略：
- clean_text 使用 ngram analyzer（2-gram）
- 每個 term 用 match_phrase（字元連續，等同 ILIKE）
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


def build_statute_hits_cte_sql(
    statute_filters: list[tuple[str, str | None, str | None]],
    params: dict[str, Any],
    source_alias: str = "c",
) -> dict[str, str]:
    """建立 search ranking 專用的 statute score CTE + JOIN SQL。

    將本次查詢的法條條件先聚合成 citation_id -> st_score，再 LEFT JOIN 回主查詢，
    避免在大查詢內重複執行 correlated EXISTS。
    """
    if not statute_filters:
        return {
            "cte_sql": "",
            "join_sql": "",
            "score_sql": "0",
        }

    filter_rows: list[str] = []
    for idx, (law, article, sub_ref) in enumerate(statute_filters):
        law_key = f"st_law_{idx}"
        article_key = f"st_article_{idx}"
        sub_ref_key = f"st_sub_ref_{idx}"
        params[law_key] = law
        params[article_key] = article
        if sub_ref is not None:
            params[sub_ref_key] = sub_ref
            filter_rows.append(
                "SELECT "
                f"{idx} AS filter_id, "
                f"%({law_key})s::text AS law, "
                f"%({article_key})s::text AS article_raw, "
                f"%({sub_ref_key})s::text AS sub_ref"
            )
        else:
            filter_rows.append(
                "SELECT "
                f"{idx} AS filter_id, "
                f"%({law_key})s::text AS law, "
                f"%({article_key})s::text AS article_raw, "
                "NULL::text AS sub_ref"
            )

    filters_sql = "\n            UNION ALL\n            ".join(filter_rows)
    return {
        "cte_sql": f"""
        statute_filter_inputs AS (
            {filters_sql}
        ),
        statute_hits AS (
            SELECT
                css.citation_id,
                COUNT(DISTINCT fi.filter_id) AS st_score
            FROM citation_snippet_statutes css
            JOIN statute_filter_inputs fi
              ON fi.law = css.law
             AND fi.article_raw = css.article_raw
             AND (
                 fi.sub_ref IS NULL
                 OR css.sub_ref = fi.sub_ref
             )
            GROUP BY css.citation_id
        ),
        """,
        "join_sql": f"LEFT JOIN statute_hits sh ON sh.citation_id = {source_alias}.id",
        "score_sql": "COALESCE(sh.st_score, 0)",
    }


def build_base_citations_cte_sql(
    keyword_score_sql: str,
    statute_score_sql: str,
    statute_join_sql: str,
) -> str:
    """建立 ranking 共用的 citations base CTE。

    將 citations 掃描、keyword score、statute score 集中在同一層，
    後續 decision / authority branch 只做分流與聚合。
    """
    join_sql = f"\n            {statute_join_sql}" if statute_join_sql else ""
    return f"""
        base_citations AS (
            SELECT
                c.id,
                c.source_id,
                c.target_id,
                c.target_authority_id,
                {keyword_score_sql} AS kw_score,
                {statute_score_sql} AS st_score
            FROM citations c
            JOIN src s ON s.source_id = c.source_id{join_sql}
        ),
    """


def build_decision_doc_type_sql(canonical_alias: str = "canonical") -> str:
    """優先讀取 canonical 層級的 doc_type 衍生欄位，未回填時退回原始 doc_type。"""
    return f"COALESCE({canonical_alias}.canonical_doc_type, {canonical_alias}.doc_type)"


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
            # prefix 比對：搜尋「第1項」可命中「第1項前段」、「第1項第1款」等
            nested_must.append({"prefix": {"statutes.sub_ref": sub_ref}})
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
            excl_must.append({"prefix": {"statutes.sub_ref": sub_ref}})
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


# ── 搜尋結果：SQL 聚合 target 排行 ───────────────────────────────────
#
# fetch_target_rankings() 單次查詢完成：
#   scored  → 每筆 citation 計算 score（keyword + statute）
#   deduped → 同一 (source, target) 保留 score 最高那筆
#   ranked  → GROUP BY target：COUNT 代表有幾個符合搜尋條件 source 引用這個 target、SUM 代表所有 source 的分數加總
#   最後 JOIN target 資訊，correlated subquery 取 total_citation_count
#
# Score 在兩處計算，但 citation 集合不同，不是重複計算：
#   1. fetch_target_rankings()：所有 source_ids 的 citations → target 排行
#   2. decisions._citation_rows()：特定 target 的 citations → 展開時排序
# ──────────────────────────────────────────────────────────────────────

COURT_LEVEL_SQL = """
    CASE COALESCE(td.root_norm, a.root_norm)
        WHEN '憲法法庭' THEN 0
        WHEN '最高法院' THEN 1  WHEN '最高行政法院' THEN 1
        WHEN '高等法院' THEN 2  WHEN '高等行政法院' THEN 2  WHEN '智財商業法院' THEN 2
        WHEN '地方法院' THEN 3  WHEN '少家法院' THEN 3      WHEN '高等行政法院地方庭' THEN 3
        WHEN '地方法院簡易庭' THEN 4
    END
"""


def fetch_target_rankings(
    conn: psycopg.Connection,
    source_ids: list[int],
    query_terms: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[dict[str, Any]]:
    """依 source_ids 取 target 排行，單次 SQL 查詢完成聚合。
    以 canonical_id 分組合併同字號不同 doc_type；total_citation_count 直接讀 decisions 欄位。
    """
    if not source_ids:
        return []

    params: dict[str, Any] = {"source_ids": source_ids}
    keyword_score_sql = build_keyword_score_sql(query_terms, params, "c.snippet")
    statute_hits_sql = build_statute_hits_cte_sql(statute_filters, params, "c")
    base_citations_cte_sql = build_base_citations_cte_sql(
        keyword_score_sql=keyword_score_sql,
        statute_score_sql=statute_hits_sql["score_sql"],
        statute_join_sql=statute_hits_sql["join_sql"],
    )
    decision_doc_type_sql = build_decision_doc_type_sql("canonical")

    target_where = ""
    target_filters = []
    if doc_types:
        params["doc_types"] = doc_types
        target_filters.append("doc_type = ANY(%(doc_types)s)")
    if court_levels:
        params["court_levels"] = court_levels
        target_filters.append("court_level = ANY(%(court_levels)s)")
    if target_filters:
        target_where = "WHERE " + " AND ".join(target_filters)

    cte_parts = [
        """
        src AS (
            SELECT UNNEST(%(source_ids)s::bigint[]) AS source_id
        )
        """.strip()
    ]
    if statute_hits_sql["cte_sql"]:
        cte_parts.append(statute_hits_sql["cte_sql"].strip().rstrip(","))
    cte_parts.append(base_citations_cte_sql.strip().rstrip(","))
    with_clause = ",\n        ".join(cte_parts)

    sql = f"""
        WITH {with_clause},
        -- 判決引用：計算 score，並將 target 映射到 canonical_id
        scored_decisions AS (
            SELECT
                bc.source_id,
                COALESCE(td.canonical_id, bc.target_id) AS canonical_id,
                bc.kw_score,
                bc.st_score
            FROM base_citations bc
            JOIN decisions td ON td.id = bc.target_id
        ),
        -- 非裁判引用：計算 score
        scored_auth AS (
            SELECT
                bc.source_id,
                bc.target_authority_id,
                bc.kw_score,
                bc.st_score
            FROM base_citations bc
            WHERE bc.target_authority_id IS NOT NULL
        ),
        -- 同 (source, canonical) 保留最高分
        deduped_decisions AS (
            SELECT DISTINCT ON (source_id, canonical_id)
                source_id, canonical_id,
                kw_score, st_score, kw_score + st_score AS score
            FROM scored_decisions
            ORDER BY source_id, canonical_id, (kw_score + st_score) DESC
        ),
        -- 同 (source, authority) 保留最高分
        deduped_auth AS (
            SELECT DISTINCT ON (source_id, target_authority_id)
                source_id, target_authority_id,
                kw_score, st_score, kw_score + st_score AS score
            FROM scored_auth
            ORDER BY source_id, target_authority_id, (kw_score + st_score) DESC
        ),
        -- 按 canonical_id 聚合
        ranked_decisions AS (
            SELECT
                canonical_id,
                COUNT(*)       AS matched_citation_count,
                SUM(score)     AS score,
                SUM(kw_score)  AS keyword_score_sum,
                SUM(st_score)  AS statute_score_sum
            FROM deduped_decisions
            GROUP BY canonical_id
        ),
        -- 按 authority 聚合
        ranked_auth AS (
            SELECT
                target_authority_id,
                COUNT(*)       AS matched_citation_count,
                SUM(score)     AS score,
                SUM(kw_score)  AS keyword_score_sum,
                SUM(st_score)  AS statute_score_sum
            FROM deduped_auth
            GROUP BY target_authority_id
        ),
        joined AS (
            -- 判決 branch（以 canonical 為代表）
            SELECT
                rd.canonical_id                                    AS target_id,
                NULL::bigint                                       AS target_authority_id,
                rd.matched_citation_count,
                rd.score,
                rd.keyword_score_sum,
                rd.statute_score_sum,
                canonical.total_citation_count,
                canonical.root_norm                                AS court,
                CASE canonical.root_norm
                    WHEN '憲法法庭'       THEN 0
                    WHEN '最高法院'       THEN 1  WHEN '最高行政法院'       THEN 1
                    WHEN '高等法院'       THEN 2  WHEN '高等行政法院'       THEN 2  WHEN '智財商業法院' THEN 2
                    WHEN '地方法院'       THEN 3  WHEN '少家法院'           THEN 3  WHEN '高等行政法院地方庭' THEN 3
                    WHEN '地方法院簡易庭' THEN 4
                END                                                AS court_level,
                canonical.unit_norm,
                canonical.jyear,
                canonical.jcase_norm,
                canonical.jno,
                canonical.display_title,
                {decision_doc_type_sql}                            AS doc_type
            FROM ranked_decisions rd
            JOIN decisions canonical           ON canonical.id    = rd.canonical_id
            UNION ALL
            -- 非裁判 branch
            SELECT
                NULL::bigint                                       AS target_id,
                ra.target_authority_id,
                ra.matched_citation_count,
                ra.score,
                ra.keyword_score_sum,
                ra.statute_score_sum,
                (SELECT COUNT(DISTINCT c.source_id)
                 FROM citations c WHERE c.target_authority_id = ra.target_authority_id
                )                                                  AS total_citation_count,
                a.root_norm                                        AS court,
                CASE a.root_norm
                    WHEN '憲法法庭'       THEN 0
                    WHEN '最高法院'       THEN 1  WHEN '最高行政法院'       THEN 1
                    WHEN '高等法院'       THEN 2  WHEN '高等行政法院'       THEN 2  WHEN '智財商業法院' THEN 2
                    WHEN '地方法院'       THEN 3  WHEN '少家法院'           THEN 3  WHEN '高等行政法院地方庭' THEN 3
                    WHEN '地方法院簡易庭' THEN 4
                END                                                AS court_level,
                NULL                                               AS unit_norm,
                NULL                                               AS jyear,
                NULL                                               AS jcase_norm,
                NULL                                               AS jno,
                a.display                                          AS display_title,
                a.doc_type
            FROM ranked_auth ra
            JOIN authorities a ON a.id = ra.target_authority_id
        )
        SELECT * FROM joined
        {target_where}
        ORDER BY score DESC, statute_score_sum DESC, keyword_score_sum DESC, court_level ASC
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()
