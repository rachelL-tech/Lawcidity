"""
target ranking 業務邏輯。

職責：
- hot-term / hit-iter 路徑選擇
- PostgreSQL 補 target metadata
- source-target hits 聚合
- 最終 ranking rows 組裝
"""

from typing import Any

import psycopg
from psycopg.rows import dict_row

from app.opensearch_service import (
    _iter_source_target_hits_opensearch,
    _search_target_uid_counts_opensearch,
)


SOURCE_TARGET_STRICT_FILL_THRESHOLD = 200
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


def _should_use_target_uid_hot_term_aggregation(
    query_terms: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
) -> bool:
    return (
        len(query_terms) == 1
        and not statute_filters
        and not exclude_terms
        and not exclude_statute_filters
    )


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


def _accumulate_target_hits(
    hits: Any,
) -> dict[str, dict[str, Any]]:
    aggregated: dict[str, dict[str, Any]] = {}

    for hit in hits:
        target_id = hit.get("target_id")
        authority_id = hit.get("target_authority_id")
        if target_id is not None:
            target_key = f"decision:{int(target_id)}"
        elif authority_id is not None:
            target_key = f"authority:{int(authority_id)}"
        else:
            continue

        row = aggregated.get(target_key)
        if row is None:
            row = {
                "target_id": int(target_id) if target_id is not None else None,
                "target_authority_id": int(authority_id) if authority_id is not None else None,
                "_source_scores": {},
                "_matched_source_ids": set(),
            }
            aggregated[target_key] = row

        source_id = int(hit["source_id"])
        row["_matched_source_ids"].add(source_id)
        row["_source_scores"][source_id] = float(hit.get("score") or 0)

    return aggregated


def _build_rankings_from_aggregated_target_hits(
    aggregated: dict[str, dict[str, Any]],
    decision_meta: dict[int, dict[str, Any]],
    authority_meta: dict[int, dict[str, Any]],
    *,
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[dict[str, Any]]:
    rankings = []

    for row in aggregated.values():
        target_id = row.get("target_id")
        authority_id = row.get("target_authority_id")
        if target_id is not None:
            meta = decision_meta.get(int(target_id))
        else:
            meta = authority_meta.get(int(authority_id)) if authority_id is not None else None
        if not meta:
            continue

        if doc_types and meta.get("doc_type") not in doc_types:
            continue
        if court_levels and meta.get("court_level") not in court_levels:
            continue

        ranked_source_rows = sorted(
            row["_source_scores"].items(),
            key=lambda item: (-(item[1] or 0), item[0]),
        )[:5]
        top_scores = [float(score or 0) for _source_id, score in ranked_source_rows]
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
                "matched_citation_count": len(row["_matched_source_ids"]),
                "score": float(sum(top_scores) / len(top_scores)) if top_scores else 0.0,
                "preview_source_ids": [int(source_id) for source_id, _score in ranked_source_rows],
            }
        )

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
                "preview_source_ids": sorted(
                    int(source_id)
                    for source_id in (stats.get("preview_source_ids") or [])
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


def _build_rankings_from_hit_iter(
    conn: psycopg.Connection,
    hits: Any,
    *,
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[dict[str, Any]]:
    aggregated = _accumulate_target_hits(hits)
    if not aggregated:
        return []

    target_ids = sorted({
        int(row["target_id"])
        for row in aggregated.values()
        if row.get("target_id") is not None
    })
    authority_ids = sorted({
        int(row["target_authority_id"])
        for row in aggregated.values()
        if row.get("target_authority_id") is not None
    })
    decision_meta = _fetch_decision_target_metadata(conn, target_ids)
    authority_meta = _fetch_authority_target_metadata(conn, authority_ids)
    return _build_rankings_from_aggregated_target_hits(
        aggregated,
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
    if not source_ids:
        return []

    if _should_use_target_uid_hot_term_aggregation(
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

    strict_rankings = _build_rankings_from_hit_iter(
        conn,
        _iter_source_target_hits_opensearch(
            query_terms=query_terms,
            source_ids=source_ids,
            statute_filters=statute_filters,
            exclude_terms=exclude_terms,
            exclude_statute_filters=exclude_statute_filters,
            minimum_should_match=1,
        ) or [],
        doc_types=doc_types,
        court_levels=court_levels,
    )
    if not strict_rankings:
        return _build_rankings_from_hit_iter(
            conn,
            _iter_source_target_hits_opensearch(
                query_terms=query_terms,
                source_ids=source_ids,
                statute_filters=statute_filters,
                exclude_terms=exclude_terms,
                exclude_statute_filters=exclude_statute_filters,
                minimum_should_match=None,
            ) or [],
            doc_types=doc_types,
            court_levels=court_levels,
        )
    if len(strict_rankings) >= SOURCE_TARGET_STRICT_FILL_THRESHOLD:
        return strict_rankings

    fallback_rankings = _build_rankings_from_hit_iter(
        conn,
        _iter_source_target_hits_opensearch(
            query_terms=query_terms,
            source_ids=source_ids,
            statute_filters=statute_filters,
            exclude_terms=exclude_terms,
            exclude_statute_filters=exclude_statute_filters,
            minimum_should_match=None,
        ) or [],
        doc_types=doc_types,
        court_levels=court_levels,
    )
    return _merge_rankings_keep_strict_first(strict_rankings, fallback_rankings)
