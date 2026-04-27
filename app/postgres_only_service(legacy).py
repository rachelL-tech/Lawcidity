"""
僅供 benchmark / 比對用的 PostgreSQL-only service。

目前只保留 baseline 召回：
- search_source_ids_baseline_pg：直接用 PostgreSQL ILIKE + decision_reason_statutes
  找出符合條件的 source_ids。
"""

from typing import Any

import psycopg


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
