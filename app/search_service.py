"""
搜尋服務層（OpenSearch + PostgreSQL）。

用途：
1. 解析與驗證搜尋參數（q/case_type/law+article+sub_ref）
2. 組裝 OpenSearch bool+nested 查詢（q 為 AND、法條組合為全 AND）
3. 提供 PostgreSQL baseline 搜尋（ILIKE）
4. 依 source_ids 回 PostgreSQL 聚合目標排行與分數（回傳 target_level/target_root_norm）

OpenSearch 查詢策略（記錄）：
- clean_text 使用 ik_smart（index/search）
- 每個空白分詞的 term 用 match_phrase（字元連續，等同 ILIKE，無 false positive）
- 不使用 analyze_query_terms 預處理（IK 會把複合詞切成字元，破壞 match_phrase 片語語意）
"""

import os
import re
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg.rows import dict_row
from etl.law_names import normalize_law_name


VALID_CASE_TYPES = {"民事", "刑事", "行政", "憲法"}

STATUTE_MATCH_SCORE = 3

# score = citation_count + keyword_score_sum + statute_score_sum
# keyword_score：每個查詢詞各自計分（命中+1），多詞查詢可得部分分數
# statute_score：每組 law+article 命中 citation snippet +3

# 去重但保留原順序
def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out

# 把 q 切成 term；q 為 None 或空字串時回傳 []
def tokenize_query(q: str | None) -> list[str]:
    if not q or not q.strip():
        return []
    return [t.strip() for t in re.split(r"\s+", q.strip()) if t.strip()]

# 解析 case_type=民事,刑事 類型字串並驗證
def parse_case_types(case_type_csv: str | None) -> list[str]:
    if not case_type_csv:
        return []
    values = [v.strip() for v in case_type_csv.split(",") if v.strip()]
    invalid = [v for v in values if v not in VALID_CASE_TYPES]
    if invalid:
        raise ValueError("case_type 僅支援：民事,刑事,行政,憲法")
    return _dedupe_keep_order(values)

# 把 law/article/sub_ref 做成一對一條件，長度不符就報錯
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

    # article 若有值，必須與 law 一一對應
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

# 把 q 轉成 must（AND），法條條件用 nested filter（每組 law+article(+sub_ref) 全 AND）
def build_opensearch_query(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str, str | None]],
    size: int,
) -> dict[str, Any]:
    must = [
        {"match_phrase": {"clean_text": term}}
        for term in query_terms
    ]

    filters: list[dict[str, Any]] = []
    if case_types:
        filters.append({"terms": {"case_type": case_types}})

    # statutes 全 AND：每一組 law(+article)(+sub_ref) 都必須命中
    for law, article, sub_ref in statute_filters:
        nested_must: list[dict[str, Any]] = [{"term": {"statutes.law": law}}]
        if article is not None:
            nested_must.append({"term": {"statutes.article_raw": article}})
        if sub_ref is not None:
            nested_must.append({"term": {"statutes.sub_ref": sub_ref}})

        filters.append(
            {
                "nested": {
                    "path": "statutes",
                    "query": {"bool": {"must": nested_must}},
                }
            }
        )

    # 排除條件
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
        must_not.append(
            {
                "nested": {
                    "path": "statutes",
                    "query": {"bool": {"must": excl_must}},
                }
            }
        )

    bool_query: dict[str, Any] = {"must": must, "filter": filters}
    if must_not:
        bool_query["must_not"] = must_not

    return {
        "size": size,
        "_source": ["source_id"],
        "query": {"bool": bool_query},
    }

# 讀環境變數
def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

# 組 OpenSearch 連線
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


# 送查詢到 OpenSearch，回傳 source_id 列表
def search_source_ids_opensearch(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str, str | None]],
    source_limit: int,
) -> list[int]:
    query_terms = _dedupe_keep_order([t for t in query_terms if t])
    client = _get_opensearch_client()
    index_name = os.environ.get("OPENSEARCH_INDEX", "decisions_v1")
    body = build_opensearch_query(
        query_terms=query_terms,
        case_types=case_types,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
        size=source_limit,
    )

    response = client.search(index=index_name, body=body)
    hits = response.get("hits", {}).get("hits", [])

    source_ids: list[int] = []
    seen: set[int] = set()
    for hit in hits:
        raw_id = (hit.get("_source") or {}).get("source_id", hit.get("_id"))
        try:
            source_id = int(raw_id)
        except Exception:
            continue
        if source_id not in seen:
            seen.add(source_id)
            source_ids.append(source_id)

    return source_ids

# 用 ILIKE 做 q 的 AND 查詢；法條用 EXISTS 子查詢
def search_source_ids_baseline_pg(
    conn: psycopg.Connection,
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str, str | None]],
    source_limit: int,
) -> list[int]:
    params: dict[str, Any] = {"source_limit": source_limit}
    where_parts: list[str] = []

    # q=AND：每個 term 都要命中 clean_text
    for idx, term in enumerate(query_terms):
        key = f"kw_{idx}"
        where_parts.append(f"d.clean_text ILIKE %({key})s")
        params[key] = f"%{term}%"

    if case_types:
        where_parts.append("d.case_type = ANY(%(case_types)s)")
        params["case_types"] = case_types

    # 法條全 AND：每一組 law(+article)(+sub_ref) 都必須存在
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

    # 排除關鍵字：NOT ILIKE
    for idx, term in enumerate(exclude_terms):
        key = f"excl_kw_{idx}"
        where_parts.append(f"d.clean_text NOT ILIKE %({key})s")
        params[key] = f"%{term}%"

    # 排除法條：NOT EXISTS
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
        ORDER BY d.id DESC
        LIMIT %(source_limit)s
    """

    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [int(row["id"]) for row in cur.fetchall()]

# 把 OpenSearch/PG 找到的 source_ids 當集合，從 citations 找 target 並計分（snippet 命中加分 + 法條匹配加分）。
# target 包含 decisions（target_id）與 authorities（target_authority_id）兩種，UNION ALL 合併後統一排序。
# 回傳欄位：citation_type（'decision'|'authority'）+ 各自識別欄位；score/citation_count 共用。
def fetch_rankings_by_source_ids(
    conn: psycopg.Connection,
    source_ids: list[int],
    query_terms: list[str],
    statute_filters: list[tuple[str, str, str | None]],
    limit: int,
) -> list[dict[str, Any]]:
    if not source_ids:
        return []

    clean_terms = _dedupe_keep_order([t.strip() for t in query_terms if t and t.strip()])
    params: dict[str, Any] = {"source_ids": source_ids, "limit": limit}

    if clean_terms:
        # 每個查詢詞各自計分：命中 +1，多詞查詢可得部分分數
        term_parts: list[str] = []
        for idx, term in enumerate(clean_terms):
            key = f"kw_{idx}"
            params[key] = f"%{term}%"
            term_parts.append(f"(b.snippet ILIKE %({key})s)::int")
        snippet_score_sql = " + ".join(term_parts)
    else:
        snippet_score_sql = "0"

    if statute_filters:
        statute_clauses: list[str] = []
        for idx, (law, article, sub_ref) in enumerate(statute_filters):
            law_key = f"law_{idx}"
            clause = f"(css.law = %({law_key})s"
            params[law_key] = law
            if article is not None:
                article_key = f"article_{idx}"
                clause += f" AND css.article_raw = %({article_key})s"
                params[article_key] = article
            if sub_ref is not None:
                sub_key = f"sub_ref_{idx}"
                clause += f" AND css.sub_ref = %({sub_key})s"
                params[sub_key] = sub_ref
            clause += ")"
            statute_clauses.append(clause)

        params["statute_score"] = STATUTE_MATCH_SCORE
        statute_score_sql = f"""
            CASE WHEN EXISTS (
                SELECT 1
                FROM citation_snippet_statutes css
                WHERE css.citation_id = b.id
                  AND ({" OR ".join(statute_clauses)})
            ) THEN %(statute_score)s ELSE 0 END
        """
    else:
        statute_score_sql = "0"

    sql = f"""
        WITH src AS (
            SELECT UNNEST(%(source_ids)s::bigint[]) AS source_id
        ),
        base AS (
            SELECT c.id,
                   c.source_id,
                   c.target_id,
                   NULL::bigint  AS target_authority_id,
                   c.snippet,
                   c.target_doc_type
            FROM citations c
            JOIN src s ON s.source_id = c.source_id
            WHERE c.target_id IS NOT NULL
            UNION ALL
            SELECT c.id,
                   c.source_id,
                   NULL::bigint  AS target_id,
                   c.target_authority_id,
                   c.snippet,
                   NULL::text    AS target_doc_type
            FROM citations c
            JOIN src s ON s.source_id = c.source_id
            WHERE c.target_authority_id IS NOT NULL
        ),
        scored AS (
            SELECT
                b.id,
                b.source_id,
                b.target_id,
                b.target_authority_id,
                b.target_doc_type,
                {snippet_score_sql} AS keyword_score,
                {statute_score_sql} AS statute_score
            FROM base b
        ),
        deduped AS (
            SELECT DISTINCT ON (source_id, target_id, target_authority_id)
                id,
                source_id,
                target_id,
                target_authority_id,
                target_doc_type,
                keyword_score,
                statute_score
            FROM scored
            ORDER BY source_id,
                     target_id NULLS LAST,
                     target_authority_id NULLS LAST,
                     (keyword_score + statute_score) DESC,
                     id DESC
        ),
        enriched AS (
            SELECT
                s.id,
                s.target_id,
                s.target_authority_id,
                s.target_doc_type,
                s.keyword_score,
                s.statute_score,
                COALESCE(d.root_norm, a.root_norm) AS target_root_norm,
                cu.level       AS target_level,
                d.jyear,
                d.jcase_norm,
                d.jno,
                a.doc_type     AS auth_type,
                a.display      AS display_title
            FROM deduped s
            LEFT JOIN decisions d    ON d.id = s.target_id
            LEFT JOIN court_units cu ON cu.id = d.court_unit_id
            LEFT JOIN authorities a  ON a.id = s.target_authority_id
        )
        SELECT
            CASE WHEN e.target_id IS NOT NULL THEN 'decision' ELSE 'authority' END
                                  AS citation_type,
            e.target_id,
            e.target_authority_id,
            e.target_root_norm,
            e.target_level,
            e.jyear,
            e.jcase_norm,
            e.jno,
            (ARRAY_REMOVE(ARRAY_AGG(e.target_doc_type ORDER BY e.id DESC), NULL))[1]
                                  AS doc_type,
            e.auth_type,
            e.display_title,
            COUNT(*)              AS citation_count,
            SUM(e.keyword_score) AS keyword_score_sum,
            SUM(e.statute_score) AS statute_score_sum,
            COUNT(*) + SUM(e.keyword_score) + SUM(e.statute_score) AS score
        FROM enriched e
        GROUP BY
            e.target_id,
            e.target_authority_id,
            e.target_root_norm,
            e.target_level,
            e.jyear,
            e.jcase_norm,
            e.jno,
            e.auth_type,
            e.display_title
        ORDER BY score DESC, citation_count DESC,
                 e.target_id DESC NULLS LAST,
                 e.target_authority_id DESC NULLS LAST
        LIMIT %(limit)s
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            sql,
            {
                **params,
            },
        )
        return cur.fetchall()
