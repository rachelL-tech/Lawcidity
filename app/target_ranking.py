"""
target ranking 業務邏輯。

職責：
- 呼叫 opensearch step_down 拿 target 級統計
- PostgreSQL 補 target metadata
- 套 doc_type / court_level filter，最終排序
"""

from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.opensearch_service import search_target_rankings_step_down


SOURCE_TARGET_STEP_DOWN_THRESHOLD = 200
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


def _fetch_decision_target_metadata(
    conn: psycopg.Connection,
    target_ids: list[int],
    source_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not target_ids:
        return {}

    sql = """
        WITH raw_targets AS (
            SELECT DISTINCT UNNEST(%(target_ids)s::bigint[]) AS raw_target_id
        )
        SELECT
            td.id AS raw_target_id,
            canonical.id AS target_id,
            canonical.root_norm AS court,
            canonical.jyear,
            canonical.jcase_norm,
            canonical.jno,
            canonical.display_title,
            canonical.canonical_doc_type AS doc_type,
            canonical.total_citation_count,
            COALESCE(mc.cnt, 0) AS matched_citation_count
        FROM raw_targets rt
        JOIN decisions td ON td.id = rt.raw_target_id
        JOIN decisions canonical ON canonical.id = COALESCE(td.canonical_id, td.id)
        LEFT JOIN LATERAL (
            SELECT COUNT(DISTINCT c.source_id)::int AS cnt
            FROM citations c
            WHERE c.target_canonical_id = canonical.id
              AND c.source_id = ANY(%(source_ids)s::bigint[])
        ) mc ON true
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"target_ids": target_ids, "source_ids": source_ids})
        rows = cur.fetchall()

    return {
        int(row["raw_target_id"]): {
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
            "matched_citation_count": int(row["matched_citation_count"]),
        }
        for row in rows
    }


def _fetch_authority_target_metadata(
    conn: psycopg.Connection,
    authority_ids: list[int],
    source_ids: list[int],
) -> dict[int, dict[str, Any]]:
    if not authority_ids:
        return {}

    sql = """
        SELECT
            a.id AS target_authority_id,
            a.root_norm AS court,
            a.display AS display_title,
            a.doc_type,
            a.total_citation_count,
            COALESCE(mc.cnt, 0) AS matched_citation_count
        FROM authorities a
        LEFT JOIN LATERAL (
            SELECT COUNT(DISTINCT c.source_id)::int AS cnt
            FROM citations c
            WHERE c.target_authority_id = a.id
              AND c.source_id = ANY(%(source_ids)s::bigint[])
        ) mc ON true
        WHERE a.id = ANY(%(authority_ids)s::bigint[])
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, {"authority_ids": authority_ids, "source_ids": source_ids})
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
            "matched_citation_count": int(row["matched_citation_count"]),
        }
        for row in rows
    }


def fetch_target_rankings_by_relevance(
    conn: psycopg.Connection,
    source_ids: list[int],
    query_terms: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
) -> list[dict[str, Any]]:
    if not source_ids:
        return []

    raw_rows = search_target_rankings_step_down(
        query_terms=query_terms,
        source_ids=source_ids,
        statute_filters=statute_filters,
        threshold=SOURCE_TARGET_STEP_DOWN_THRESHOLD,
    )
    if not raw_rows:
        return []

    parsed_raw_rows: list[tuple[dict[str, Any], str, int]] = []
    decision_ids: list[int] = []
    authority_ids: list[int] = []
    for raw in raw_rows:
        kind, raw_id_str = raw["target_uid"].split(":", 1)
        raw_id = int(raw_id_str)
        parsed_raw_rows.append((raw, kind, raw_id))
        if kind == "decision":
            decision_ids.append(raw_id)
        elif kind == "authority":
            authority_ids.append(raw_id)

    decision_meta = _fetch_decision_target_metadata(conn, decision_ids, source_ids)
    authority_meta = _fetch_authority_target_metadata(conn, authority_ids, source_ids)

    rankings: list[dict[str, Any]] = []
    for raw, kind, raw_id in parsed_raw_rows:
        meta = (
            decision_meta.get(raw_id) if kind == "decision"
            else authority_meta.get(raw_id)
        )
        if not meta:
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
                "matched_citation_count": int(meta.get("matched_citation_count") or 0),
                "reached_at_msm": int(raw["reached_at_msm"]),
            }
        )

    rankings.sort(
        key=lambda row: (
            -row["reached_at_msm"],
            -row["matched_citation_count"],
            -row["total_citation_count"],
            row["court_level"] if row["court_level"] is not None else 99,
        )
    )
    return rankings
