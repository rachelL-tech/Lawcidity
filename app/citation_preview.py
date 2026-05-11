"""
citation preview SQL/service。

職責：
- counts 查詢
- matched preview rows 查詢
- others preview rows 查詢
"""

from psycopg.rows import dict_row


CITATIONS_PREVIEW_LIMIT = 5


def build_keyword_score_sql(
    query_terms: list[str],
    params: dict,
    snippet_col: str = "c.snippet",
) -> str:
    """每個不同 query term 命中 snippet → +1。"""
    if not query_terms:
        return "0"

    parts = []
    for idx, term in enumerate(query_terms):
        key = f"kw_{idx}"
        params[key] = f"%{term}%"
        parts.append(f"({snippet_col} ILIKE %({key})s)::int")
    return " + ".join(parts)


def build_statute_score_sql(
    statute_filters: list[tuple],
    params: dict,
    citation_id_col: str = "c.id",
) -> str:
    """每組 law(+article)(+sub_ref) 在 css 命中 → +1。"""
    if not statute_filters:
        return "0"

    parts = []
    for idx, (law, article, sub_ref) in enumerate(statute_filters):
        law_key = f"law_{idx}"
        params[law_key] = law
        inner = f"css.law = %({law_key})s"
        if article is not None:
            art_key = f"article_{idx}"
            params[art_key] = article
            inner += f" AND css.article_raw = %({art_key})s"
        if sub_ref is not None:
            sub_key = f"sub_ref_{idx}"
            params[sub_key] = sub_ref
            inner += f" AND css.sub_ref = %({sub_key})s"
        parts.append(
            f"(EXISTS (SELECT 1 FROM citation_snippet_statutes css "
            f"WHERE css.citation_id = {citation_id_col} AND {inner}))::int"
        )
    return " + ".join(parts)


def fetch_citation_counts(
    conn,
    target_col: str,
    target_val: int,
    source_ids: list[int],
) -> tuple[int, int]:
    """回傳 (matched_total, others_total)。"""
    params = {
        "target_val": target_val,
        "source_ids": source_ids,
    }
    target_filter = f"{target_col} = %(target_val)s"
    source_filter = "c.source_id = ANY(%(source_ids)s::bigint[])"

    matched_total_sql = f"""
        SELECT COUNT(DISTINCT c.source_id)::int AS matched_total
        FROM citations c
        WHERE {target_filter}
          AND {source_filter}
    """
    if target_col in {"c.target_id", "c.target_canonical_id"}:
        totals_sql = """
            SELECT COALESCE(MAX(d.total_citation_count), 0)::int AS total_citation_count
            FROM decisions d
            WHERE d.id = %(target_val)s
        """
    else:
        totals_sql = """
            SELECT COALESCE(a.total_citation_count, 0)::int AS total_citation_count
            FROM authorities a
            WHERE a.id = %(target_val)s
        """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(totals_sql, params)
        total_row = cur.fetchone() or {}
        cur.execute(matched_total_sql, params)
        matched_row = cur.fetchone() or {}

    total_citation_count = int(total_row.get("total_citation_count") or 0)
    matched_total = int(matched_row.get("matched_total") or 0)
    others_total = max(total_citation_count - matched_total, 0)
    return matched_total, others_total


def fetch_matched_preview_rows(
    conn,
    target_col: str,
    target_val: int,
    query_terms: list[str],
    statute_filters: list[tuple],
    preview_source_ids: list[int],
) -> list[dict]:
    """查詢 matched preview rows。"""
    params = {
        "target_val": target_val,
        "preview_source_ids": [
            int(source_id) for source_id in preview_source_ids[:CITATIONS_PREVIEW_LIMIT]
        ],
        "limit": CITATIONS_PREVIEW_LIMIT,
    }
    target_filter = f"{target_col} = %(target_val)s"
    keyword_score_sql = build_keyword_score_sql(query_terms, params)
    statute_score_sql = build_statute_score_sql(statute_filters, params, "c.id")

    sql = f"""
        WITH scored AS (
            SELECT
                c.id                            AS citation_id,
                c.source_id,
                {keyword_score_sql}            AS keyword_score,
                {statute_score_sql}            AS statute_score
            FROM citations c
            WHERE {target_filter}
              AND c.source_id = ANY(%(preview_source_ids)s::bigint[])
        ),
        picked AS (
            SELECT DISTINCT ON (source_id)
                citation_id,
                source_id,
                (keyword_score + statute_score) AS hit_score
            FROM scored
            ORDER BY source_id, hit_score DESC, citation_id ASC
        )
        SELECT
            c.id                            AS citation_id,
            c.source_id,
            src.display_title,
            src.unit_norm                   AS source_court_raw,
            cu.level                        AS source_court_level,
            src.doc_type,
            src.decision_date,
            c.snippet,
            c.raw_match,
            COALESCE(
                json_agg(
                    json_build_object('law', css.law, 'article', css.article_raw, 'sub_ref', css.sub_ref)
                    ORDER BY css.law, css.article_raw, css.sub_ref
                ) FILTER (WHERE css.id IS NOT NULL),
                '[]'::json
            )                               AS statutes
        FROM picked
        JOIN citations c ON c.id = picked.citation_id
        JOIN decisions src ON src.id = c.source_id
        LEFT JOIN court_units cu ON cu.id = src.court_unit_id
        LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
        GROUP BY picked.hit_score, c.id, c.source_id, src.display_title, src.unit_norm,
                 cu.level, src.doc_type, src.decision_date, c.snippet, c.raw_match
        ORDER BY picked.hit_score DESC, c.id ASC
        LIMIT %(limit)s
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_other_preview_rows(
    conn,
    target_col: str,
    target_val: int,
    source_ids: list[int],
) -> list[dict]:
    """查詢 others preview rows。"""
    params = {
        "target_val": target_val,
        "source_ids": source_ids,
        "limit": CITATIONS_PREVIEW_LIMIT,
    }
    target_filter = f"{target_col} = %(target_val)s"

    sql = f"""
        WITH excluded AS MATERIALIZED (
            SELECT DISTINCT unnest(%(source_ids)s::bigint[]) AS source_id
        ),
        picked AS (
            SELECT DISTINCT ON (c.source_id)
                c.source_id,
                c.id AS citation_id
            FROM citations c
            LEFT JOIN excluded e ON e.source_id = c.source_id
            WHERE {target_filter}
              AND e.source_id IS NULL
            ORDER BY c.source_id, c.id ASC
        ),
        limited AS (
            SELECT citation_id
            FROM picked
            ORDER BY citation_id ASC
            LIMIT %(limit)s
        )
        SELECT
            c.id                            AS citation_id,
            c.source_id,
            src.display_title,
            src.unit_norm                   AS source_court_raw,
            cu.level                        AS source_court_level,
            src.doc_type,
            src.decision_date,
            c.snippet,
            c.raw_match,
            COALESCE(
                json_agg(
                    json_build_object('law', css.law, 'article', css.article_raw, 'sub_ref', css.sub_ref)
                    ORDER BY css.law, css.article_raw, css.sub_ref
                ) FILTER (WHERE css.id IS NOT NULL),
                '[]'::json
            )                               AS statutes
        FROM limited
        JOIN citations c ON c.id = limited.citation_id
        JOIN decisions src ON src.id = c.source_id
        LEFT JOIN court_units cu ON cu.id = src.court_unit_id
        LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
        GROUP BY c.id, c.source_id, src.display_title, src.unit_norm,
                 cu.level, src.doc_type, src.decision_date, c.snippet, c.raw_match
        ORDER BY c.id ASC
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fetch_more_preview_rows(
    conn,
    target_col: str,
    target_val: int,
    query_terms: list[str],
    statute_filters: list[tuple],
    resolved_source_ids: list[int],
    exclude_source_ids: list[int],
    page_size: int,
) -> list[dict]:
    """為 /more endpoint：rank resolved 範圍（排除已載入），取前 page_size 個 source 並 hydrate snippet。
    一條 SQL 完成 ranking + hydration，避免拆兩階段的重複 scored CTE。"""
    params: dict = {
        "target_val": target_val,
        "resolved": resolved_source_ids,
        "excluded": exclude_source_ids,
        "limit": page_size,
    }
    target_filter = f"{target_col} = %(target_val)s"
    keyword_score_sql = build_keyword_score_sql(query_terms, params)
    statute_score_sql = build_statute_score_sql(statute_filters, params, "c.id")

    sql = f"""
        WITH scored AS (
            SELECT c.id, c.source_id,
                   {keyword_score_sql} AS keyword_score,
                   {statute_score_sql} AS statute_score
            FROM citations c
            WHERE {target_filter}
              AND c.source_id = ANY(%(resolved)s::bigint[])
              AND NOT (c.source_id = ANY(%(excluded)s::bigint[]))
        ),
        picked AS (
            SELECT DISTINCT ON (source_id)
                source_id,
                id AS citation_id,
                (keyword_score + statute_score) AS hit_score
            FROM scored
            ORDER BY source_id, (keyword_score + statute_score) DESC, id ASC
        ),
        top_n AS (
            SELECT citation_id
            FROM picked
            ORDER BY hit_score DESC, source_id ASC
            LIMIT %(limit)s
        )
        SELECT
            c.id                            AS citation_id,
            c.source_id,
            src.display_title,
            src.unit_norm                   AS source_court_raw,
            cu.level                        AS source_court_level,
            src.doc_type,
            src.decision_date,
            c.snippet,
            c.raw_match,
            COALESCE(
                json_agg(
                    json_build_object('law', css.law, 'article', css.article_raw, 'sub_ref', css.sub_ref)
                    ORDER BY css.law, css.article_raw, css.sub_ref
                ) FILTER (WHERE css.id IS NOT NULL),
                '[]'::json
            )                               AS statutes
        FROM top_n
        JOIN citations c ON c.id = top_n.citation_id
        JOIN decisions src ON src.id = c.source_id
        LEFT JOIN court_units cu ON cu.id = src.court_unit_id
        LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
        GROUP BY c.id, c.source_id, src.display_title, src.unit_norm,
                 cu.level, src.doc_type, src.decision_date, c.snippet, c.raw_match
        ORDER BY c.id ASC
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()
