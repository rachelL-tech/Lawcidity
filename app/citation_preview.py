"""
citation preview SQL/service。

職責：
- representative citation SQL builder
- matched / others preview rows 查詢
- preview related constants
"""

from psycopg.rows import dict_row


CITATIONS_PREVIEW_LIMIT = 5


def build_statute_score_sql(
    statute_filters: list[tuple], params: dict, citation_id_col: str = "c.id",
) -> str:
    """每組 law(+article)(+sub_ref) 在 css 命中 → +1。回傳 SQL expression。"""
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


def fetch_citation_preview_rows(
    conn,
    target_col: str,
    target_val: int,
    query_terms: list[str],
    statute_filters: list[tuple],
    source_ids: list[int],
    matched: bool,
    *,
    shared_counts: tuple[int, int] | None = None,
    preview_source_ids: list[int] | None = None,
) -> tuple[list[dict], int, int]:
    """查詢 target 的 citations preview。"""
    params = {
        "target_val": target_val,
        "source_ids": source_ids,
        "limit": CITATIONS_PREVIEW_LIMIT,
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

    if matched:
        if preview_source_ids:
            keyword_hit_sql = "0"
            if query_terms:
                keyword_parts = []
                for idx, term in enumerate(query_terms):
                    key = f"kw_{idx}"
                    params[key] = f"%{term}%"
                    keyword_parts.append(f"c.snippet ILIKE %({key})s")
                keyword_hit_sql = f"(({' OR '.join(keyword_parts)})::int)"
            statute_hit_sql = f"(({build_statute_score_sql(statute_filters, params, 'c.id')}) > 0)::int"
            params["preview_source_ids"] = [
                int(source_id) for source_id in preview_source_ids[:CITATIONS_PREVIEW_LIMIT]
            ]
            candidate_sql = f"""
                WITH preview_sources AS (
                    SELECT source_id, ordinality AS source_ord
                    FROM unnest(%(preview_source_ids)s::bigint[]) WITH ORDINALITY AS t(source_id, ordinality)
                ),
                scored AS (
                    SELECT
                        preview_sources.source_id,
                        preview_sources.source_ord,
                        c.id                            AS citation_id,
                        {keyword_hit_sql}              AS keyword_hit,
                        {statute_hit_sql}              AS statute_hit
                    FROM preview_sources
                    JOIN citations c ON c.source_id = preview_sources.source_id
                    WHERE {target_filter}
                ),
                ranked AS (
                    SELECT
                        ranked_rows.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY ranked_rows.source_id
                            ORDER BY keyword_hit DESC, statute_hit DESC, citation_id DESC
                        ) AS citation_rank
                    FROM scored AS ranked_rows
                )
                SELECT citation_id
                FROM ranked
                WHERE citation_rank = 1
                ORDER BY source_ord
            """
        else:
            candidate_sql = f"""
                WITH deduped AS (
                    SELECT DISTINCT ON (c.source_id)
                        c.source_id,
                        cu.level                        AS source_court_level,
                        src.decision_date,
                        c.id                            AS citation_id
                    FROM citations c
                    JOIN decisions src ON c.source_id = src.id
                    LEFT JOIN court_units cu ON cu.id = src.court_unit_id
                    WHERE {target_filter}
                      AND {source_filter}
                    ORDER BY c.source_id, c.id
                )
                SELECT citation_id
                FROM deduped
                ORDER BY source_court_level ASC NULLS LAST, decision_date DESC NULLS LAST, source_id DESC
                LIMIT %(limit)s
            """
        rows_sql = """
            WITH candidate_ids AS (
                SELECT citation_id, ordinality AS candidate_ord
                FROM unnest(%(candidate_ids)s::bigint[]) WITH ORDINALITY AS t(citation_id, ordinality)
            )
            SELECT
                c.id                            AS citation_id,
                c.source_id,
                src.unit_norm                   AS source_court_raw,
                cu.level                        AS source_court_level,
                src.jyear, src.jcase_norm, src.jno,
                src.doc_type, src.decision_date,
                c.snippet, c.raw_match,
                COALESCE(
                    json_agg(
                        json_build_object('law', css.law, 'article', css.article_raw, 'sub_ref', css.sub_ref)
                        ORDER BY css.law, css.article_raw, css.sub_ref
                    ) FILTER (WHERE css.id IS NOT NULL),
                    '[]'::json
                )                               AS statutes,
                TRUE                            AS is_matched,
                0::double precision             AS score,
                candidate_ord
            FROM candidate_ids
            JOIN citations c ON c.id = candidate_ids.citation_id
            JOIN decisions src ON c.source_id = src.id
            LEFT JOIN court_units cu ON cu.id = src.court_unit_id
            LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
            WHERE c.id = ANY(%(candidate_ids)s::bigint[])
            GROUP BY candidate_ord, c.id, c.source_id, src.unit_norm, cu.level,
                     src.jyear, src.jcase_norm, src.jno,
                     src.doc_type, src.decision_date,
                     c.snippet, c.raw_match
            ORDER BY candidate_ord
        """
    else:
        candidate_sql = f"""
            WITH excluded AS MATERIALIZED (
                SELECT DISTINCT unnest(%(source_ids)s::bigint[]) AS source_id
            ),
            deduped AS MATERIALIZED (
                SELECT DISTINCT ON (c.source_id)
                    c.source_id,
                    c.id                            AS citation_id
                FROM citations c
                LEFT JOIN excluded e ON e.source_id = c.source_id
                WHERE {target_filter}
                  AND e.source_id IS NULL
                ORDER BY c.source_id, c.id
            )
            SELECT deduped.citation_id
            FROM deduped
            JOIN decisions src ON src.id = deduped.source_id
            LEFT JOIN court_units cu ON cu.id = src.court_unit_id
            ORDER BY cu.level ASC NULLS LAST, deduped.source_id DESC
            LIMIT %(limit)s
        """
        rows_sql = """
            WITH candidate_ids AS (
                SELECT citation_id, ordinality AS candidate_ord
                FROM unnest(%(candidate_ids)s::bigint[]) WITH ORDINALITY AS t(citation_id, ordinality)
            )
            SELECT
                c.id                            AS citation_id,
                c.source_id,
                src.unit_norm                   AS source_court_raw,
                cu.level                        AS source_court_level,
                src.jyear, src.jcase_norm, src.jno,
                src.doc_type, src.decision_date,
                c.snippet, c.raw_match,
                COALESCE(
                    json_agg(
                        json_build_object('law', css.law, 'article', css.article_raw, 'sub_ref', css.sub_ref)
                        ORDER BY css.law, css.article_raw, css.sub_ref
                    ) FILTER (WHERE css.id IS NOT NULL),
                    '[]'::json
                )                               AS statutes,
                FALSE                           AS is_matched,
                0::double precision             AS score,
                candidate_ord
            FROM candidate_ids
            JOIN citations c ON c.id = candidate_ids.citation_id
            JOIN decisions src ON c.source_id = src.id
            LEFT JOIN court_units cu ON cu.id = src.court_unit_id
            LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
            WHERE c.id = ANY(%(candidate_ids)s::bigint[])
            GROUP BY candidate_ord, c.id, c.source_id, src.unit_norm, cu.level,
                     src.jyear, src.jcase_norm, src.jno,
                     src.doc_type, src.decision_date,
                     c.snippet, c.raw_match
            ORDER BY candidate_ord
        """
    with conn.cursor(row_factory=dict_row) as cur:
        if shared_counts is None:
            cur.execute(totals_sql, params)
            total_row = cur.fetchone() or {}
            cur.execute(matched_total_sql, params)
            matched_row = cur.fetchone() or {}
            total_citation_count = int(total_row.get("total_citation_count") or 0)
            matched_total = int(matched_row.get("matched_total") or 0)
        else:
            total_citation_count, matched_total = shared_counts
        others_total = max(total_citation_count - matched_total, 0)
        cur.execute(candidate_sql, params)
        candidate_rows = cur.fetchall()
        candidate_ids = [int(row["citation_id"]) for row in candidate_rows]
        if not candidate_ids:
            rows = []
        else:
            params["candidate_ids"] = candidate_ids
            cur.execute(rows_sql, params)
            rows = cur.fetchall()
    return rows, matched_total, others_total
