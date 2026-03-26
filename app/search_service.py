"""
搜尋業務邏輯層（不知道 HTTP，不依賴 FastAPI）。

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
    statute_score_sql = build_statute_score_sql(statute_filters, params, "c.id")

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

    sql = f"""
        WITH src AS (
            SELECT UNNEST(%(source_ids)s::bigint[]) AS source_id
        ),
        -- 判決引用：計算 score，並將 target 映射到 canonical_id
        scored_decisions AS (
            SELECT
                c.source_id,
                COALESCE(td.canonical_id, c.target_id) AS canonical_id,
                ({keyword_score_sql})                   AS kw_score,
                ({statute_score_sql})                   AS st_score
            FROM citations c
            JOIN src s        ON s.source_id = c.source_id
            JOIN decisions td ON td.id       = c.target_id
        ),
        -- 非裁判引用：計算 score
        scored_auth AS (
            SELECT
                c.source_id,
                c.target_authority_id,
                ({keyword_score_sql})  AS kw_score,
                ({statute_score_sql})  AS st_score
            FROM citations c
            JOIN src s ON s.source_id = c.source_id
            WHERE c.target_authority_id IS NOT NULL
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
        -- 每個 canonical 群的聚合 doc_type（涵蓋所有 sibling）
        canonical_doc_types AS (
            SELECT
                d.canonical_id,
                CASE
                  WHEN COUNT(DISTINCT d.doc_type) FILTER (WHERE d.doc_type IS NOT NULL) > 1 THEN '裁判'
                  ELSE MAX(d.doc_type)
                END AS doc_type
            FROM decisions d
            WHERE d.canonical_id IN (SELECT canonical_id FROM ranked_decisions)
            GROUP BY d.canonical_id
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
                COALESCE(cdt.doc_type, canonical.doc_type)        AS doc_type
            FROM ranked_decisions rd
            JOIN decisions canonical           ON canonical.id    = rd.canonical_id
            LEFT JOIN canonical_doc_types cdt  ON cdt.canonical_id = rd.canonical_id
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


# ── 語意搜尋 ─────────────────────────────────────────────────────────

# CHUNK_INDEX_NAME = "chunks_v1"
# QWEN3_QUERY_INSTRUCTION = "Instruct: 給定法律查詢，找出引用相關判決的法院判決段落\nQuery: "

# MLX_MODEL = "mlx-community/Qwen3-Embedding-0.6B-8bit"
# CF_AI_MODEL = "@cf/qwen/qwen3-embedding-0.6b"
# EMBED_DIMS = 512

# _embed_model = None
# _embed_tokenizer = None


# def _get_embed_model():
#     global _embed_model, _embed_tokenizer
#     if _embed_model is None:
#         try:
#             from mlx_embeddings.utils import load as mlx_load
#         except ImportError:
#             raise RuntimeError("缺少 mlx-embeddings，請執行 pip install mlx-embeddings")
#         _embed_model, _embed_tokenizer = mlx_load(MLX_MODEL)
#     return _embed_model, _embed_tokenizer


# def _embed_texts_mlx(texts: list[str]) -> list[list[float]]:
#     import mlx.core as mx
#     import numpy as np
#     model, tokenizer = _get_embed_model()
#     encoded = [tokenizer.encode(t, max_length=512, truncation=True) for t in texts]
#     max_len = max(len(e) for e in encoded)
#     pad_id = tokenizer.pad_token_id or 0
#     padded = [e + [pad_id] * (max_len - len(e)) for e in encoded]
#     mask   = [[1] * len(e) + [0] * (max_len - len(e)) for e in encoded]
#     out = model(mx.array(padded), attention_mask=mx.array(mask))
#     embeds = np.array(out.text_embeds)[:, :EMBED_DIMS]
#     norms = np.linalg.norm(embeds, axis=1, keepdims=True)
#     norms[norms == 0] = 1
#     return (embeds / norms).tolist()


# def _embed_texts_cf(texts: list[str]) -> list[list[float]]:
#     import json
#     import urllib.request
#     import numpy as np

#     account_id = os.environ.get("CF_ACCOUNT_ID", "").strip()
#     token = os.environ.get("CF_AI_TOKEN", "").strip()
#     if not account_id or not token:
#         raise RuntimeError("缺少 CF_ACCOUNT_ID 或 CF_AI_TOKEN 環境變數")

#     url = (
#         f"https://api.cloudflare.com/client/v4/accounts/{account_id}"
#         f"/ai/run/{CF_AI_MODEL}"
#     )
#     payload = json.dumps({"text": texts}).encode("utf-8")
#     req = urllib.request.Request(
#         url, data=payload,
#         headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
#     )
#     with urllib.request.urlopen(req) as resp:
#         data = json.loads(resp.read())

#     if not data.get("success"):
#         raise RuntimeError(f"Cloudflare AI 錯誤：{data.get('errors')}")

#     result = []
#     for vec in data["result"]["data"]:
#         emb = np.array(vec[:EMBED_DIMS], dtype=np.float32)
#         emb = emb / (np.linalg.norm(emb) or 1)
#         result.append(emb.tolist())
#     return result


# def _embed_texts(texts: list[str]) -> list[list[float]]:
#     backend = os.environ.get("EMBED_BACKEND", "mlx").lower()
#     if backend == "cf":
#         return _embed_texts_cf(texts)
#     return _embed_texts_mlx(texts)


# def semantic_chunk_search(
#     query: str,
#     case_type: str | None,
#     k: int = 200,
# ) -> list[dict[str, Any]]:
#     """Embed query → knn on chunks_v1 → return scored chunk hits."""
#     vecs = _embed_texts([QWEN3_QUERY_INSTRUCTION + query])
#     vec = vecs[0]

#     knn_clause: dict[str, Any] = {"vector": vec, "k": k}
#     if case_type:
#         knn_clause["filter"] = {"term": {"case_type": case_type}}

#     body = {
#         "size": k,
#         "query": {"knn": {"embedding": knn_clause}},
#         "_source": [
#             "decision_id", "case_type",
#             "target_ids", "target_authority_ids",
#             "chunk_index", "start_offset", "end_offset",
#         ],
#     }

#     client = _get_opensearch_client()
#     resp = client.search(index=CHUNK_INDEX_NAME, body=body)
#     hits = resp.get("hits", {}).get("hits", [])

#     return [
#         {
#             "decision_id":          int(h["_source"]["decision_id"]),
#             "chunk_index":          h["_source"].get("chunk_index"),
#             "case_type":            h["_source"].get("case_type"),
#             "target_ids":           h["_source"].get("target_ids") or [],
#             "target_authority_ids": h["_source"].get("target_authority_ids") or [],
#             "start_offset":         h["_source"].get("start_offset"),
#             "end_offset":           h["_source"].get("end_offset"),
#             "score":                float(h.get("_score", 0.0)),
#         }
#         for h in hits
#         if h.get("_source", {}).get("decision_id") is not None
#     ]


# def fetch_semantic_source_rankings(
#     conn: psycopg.Connection,
#     chunks: list[dict[str, Any]],
# ) -> list[dict[str, Any]]:
#     """
#     Aggregate knn chunks by decision_id → rank sources by max semantic score.
#     Each source carries cited targets (from matched chunks) as metadata.
#     """
#     if not chunks:
#         return []

#     # ── Aggregate by decision_id ──────────────────────────────────────
#     agg: dict[int, dict[str, Any]] = {}
#     for c in chunks:
#         did = c["decision_id"]
#         if did not in agg:
#             agg[did] = {
#                 "max_score":            0.0,
#                 "chunk_count":          0,
#                 "target_ids":           set(),
#                 "target_authority_ids": set(),
#             }
#         e = agg[did]
#         e["max_score"]   = max(e["max_score"], c["score"])
#         e["chunk_count"] += 1
#         e["target_ids"].update(c["target_ids"])
#         e["target_authority_ids"].update(c["target_authority_ids"])

#     sorted_dids = sorted(
#         agg, key=lambda d: (-agg[d]["max_score"], -agg[d]["chunk_count"])
#     )

#     # ── Fetch source decision metadata ────────────────────────────────
#     with conn.cursor(row_factory=dict_row) as cur:
#         cur.execute("""
#             SELECT id,
#                    root_norm           AS court,
#                    doc_type,
#                    decision_date::text AS decision_date,
#                    display_title
#             FROM decisions
#             WHERE id = ANY(%(ids)s::bigint[])
#         """, {"ids": sorted_dids})
#         src_map: dict[int, dict] = {row["id"]: dict(row) for row in cur.fetchall()}

#     # ── Fetch target metadata ─────────────────────────────────────────
#     all_target_ids    = {tid for e in agg.values() for tid in e["target_ids"]}
#     all_authority_ids = {aid for e in agg.values() for aid in e["target_authority_ids"]}

#     decision_targets: dict[int, dict] = {}
#     if all_target_ids:
#         with conn.cursor(row_factory=dict_row) as cur:
#             cur.execute("""
#                 SELECT id,
#                        root_norm  AS court,
#                        doc_type,
#                        display_title
#                 FROM decisions
#                 WHERE id = ANY(%(ids)s::bigint[])
#             """, {"ids": list(all_target_ids)})
#             decision_targets = {row["id"]: dict(row) for row in cur.fetchall()}

#     authority_targets: dict[int, dict] = {}
#     if all_authority_ids:
#         with conn.cursor(row_factory=dict_row) as cur:
#             cur.execute("""
#                 SELECT id,
#                        root_norm  AS court,
#                        doc_type,
#                        display    AS display_title
#                 FROM authorities
#                 WHERE id = ANY(%(ids)s::bigint[])
#             """, {"ids": list(all_authority_ids)})
#             authority_targets = {row["id"]: dict(row) for row in cur.fetchall()}

#     # ── Assemble results ──────────────────────────────────────────────
#     results = []
#     for did in sorted_dids:
#         if did not in src_map:
#             continue
#         src = src_map[did]
#         e   = agg[did]

#         cited: list[dict] = []
#         for tid in sorted(e["target_ids"]):
#             if tid in decision_targets:
#                 t = decision_targets[tid]
#                 cited.append({
#                     "target_id":    tid,
#                     "authority_id": None,
#                     "case_ref":     t.get("display_title") or "",
#                     "court":        t.get("court") or "",
#                     "doc_type":     t.get("doc_type"),
#                 })
#         for aid in sorted(e["target_authority_ids"]):
#             if aid in authority_targets:
#                 t = authority_targets[aid]
#                 cited.append({
#                     "target_id":    None,
#                     "authority_id": aid,
#                     "case_ref":     t.get("display_title") or "",
#                     "court":        t.get("court") or "",
#                     "doc_type":     t.get("doc_type"),
#                 })

#         results.append({
#             "source_id":     did,
#             "case_ref":      src.get("display_title") or "",
#             "court":         src.get("court") or "",
#             "doc_type":      src.get("doc_type"),
#             "decision_date": src.get("decision_date"),
#             "score":         e["max_score"],
#             "chunk_count":   e["chunk_count"],
#             "cited_targets": cited,
#         })

#     return results
