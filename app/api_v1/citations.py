"""
展開單一 target 的 citation 列表。

職責：
- GET /decisions/{id}/citations  — 展開用 preview，一次回 matched / others
- GET /authorities/{id}/citations
- 使用 PostgreSQL 組裝 matched / others 展開資料。
"""

from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from app.db import get_conn
from app.search_cache import get_search_source_ids
from app.opensearch_service import (
    dedupe_query_terms,
    dedupe_statute_filters,
    build_statute_score_sql,
    parse_case_types,
    search_source_ids_opensearch,
)
from app.api_v1.schemas import (
    CitationsResponse,
    CitationTargetInfo,
    CitationSource,
)

router = APIRouter()
CITATIONS_PREVIEW_LIMIT = 5


def _fmt_case_ref(jyear, jcase_norm, jno):
    if jyear is None or jcase_norm is None or jno is None:
        return ""
    return f"{jyear}年度{jcase_norm}字第{jno}號"


def _simplify_court(unit_norm: str) -> str:
    """簡易庭截到上一層地方法院。"""
    if not unit_norm:
        return unit_norm
    if "簡易庭" in unit_norm:
        idx = unit_norm.find("簡易庭")
        prefix = unit_norm[:idx]
        court_idx = prefix.rfind("法院")
        if court_idx != -1:
            return prefix[:court_idx + 2]
    return unit_norm


def _resolve_source_ids_for_citations(
    query_terms: list[str],
    statute_filters: list[tuple],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple],
    case_types: list[str],
    search_cache_key: str | None,
) -> list[int]:
    cached_source_ids = get_search_source_ids(search_cache_key)
    if cached_source_ids:
        return cached_source_ids

    try:
        return search_source_ids_opensearch(
            query_terms=query_terms,
            case_types=case_types,
            statute_filters=statute_filters,
            exclude_terms=exclude_terms,
            exclude_statute_filters=exclude_statute_filters,
            source_limit=None,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"搜尋服務失敗：{exc}") from exc


def _parse_ranked_source_ids(raw: str | None) -> list[int] | None:
    if raw is None or not isinstance(raw, str) or not raw:
        return None

    ranked_source_ids: list[int] = []
    seen: set[int] = set()
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        try:
            source_id = int(value)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="ranked_source_ids 格式錯誤") from exc
        if source_id in seen:
            continue
        seen.add(source_id)
        ranked_source_ids.append(source_id)
        if len(ranked_source_ids) >= CITATIONS_PREVIEW_LIMIT:
            break
    return ranked_source_ids or None


# ── Citation 查詢 ─────────────────────────────────────────────────────
#
# 單一展開 endpoint：回 matched（符合全部搜尋條件）與 others（未全部符合）。
# 內部共用 _citation_rows()，差異只在 WHERE 的 is_matched 條件正反。
# keywords/statutes 擇一必填，未帶搜尋條件回 400。
# ──────────────────────────────────────────────────────────────────────

def _citation_rows(
    conn,
    target_col: str,
    target_val: int,
    query_terms: list[str],
    statute_filters: list[tuple],
    source_ids: list[int],
    matched: bool,
    *,
    shared_counts: tuple[int, int] | None = None,
    ranked_source_ids: list[int] | None = None,
) -> tuple[list[dict], int, int]:
    """查詢 target 的 citations，回傳全部結果。

    matched=True  → 這次 search source_ids 內引用該 target 的來源
    matched=False → 歷史上引用該 target、但不在這次 search source_ids 內的來源

    decision target 會合併同 canonical family 的 citations。
    """
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
        if ranked_source_ids:
            keyword_hit_sql = "0"
            if query_terms:
                keyword_parts = []
                for idx, term in enumerate(query_terms):
                    key = f"kw_{idx}"
                    params[key] = f"%{term}%"
                    keyword_parts.append(f"c.snippet ILIKE %({key})s")
                keyword_hit_sql = f"(({' OR '.join(keyword_parts)})::int)"
            statute_hit_sql = f"(({build_statute_score_sql(statute_filters, params, 'c.id')}) > 0)::int"
            params["ranked_source_ids"] = [int(source_id) for source_id in ranked_source_ids[:CITATIONS_PREVIEW_LIMIT]]
            candidate_sql = f"""
                WITH ranked_sources AS (
                    SELECT source_id, ordinality AS source_ord
                    FROM unnest(%(ranked_source_ids)s::bigint[]) WITH ORDINALITY AS t(source_id, ordinality)
                ),
                scored AS (
                    SELECT
                        ranked_sources.source_id,
                        ranked_sources.source_ord,
                        c.id                            AS citation_id,
                        {keyword_hit_sql}              AS keyword_hit,
                        {statute_hit_sql}              AS statute_hit
                    FROM ranked_sources
                    JOIN citations c ON c.source_id = ranked_sources.source_id
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
            ORDER BY cu.level ASC NULLS LAST, src.decision_date DESC NULLS LAST, deduped.source_id DESC
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


# ── 共用回應組裝 ──────────────────────────────────────────────────────

def _parse_citation_params(keywords, statutes):
    """解析 citations endpoint 的 keywords/statutes query params。"""
    import json as _json

    query_terms = dedupe_query_terms(
        keywords.split(",") if keywords else []
    )
    try:
        statute_list: list[tuple] = []
        if statutes:
            parsed = _json.loads(statutes)
            statute_list = dedupe_statute_filters([
                (s.get("law", ""), s.get("article"), s.get("sub_ref"))
                for s in parsed
            ])
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"statutes 格式錯誤：{e}")

    if not query_terms and not statute_list:
        raise HTTPException(status_code=400, detail="keywords 和 statutes 至少填一個")

    return query_terms, statute_list


def _parse_exclude_and_case_type_params(exclude_keywords, exclude_statutes, case_types):
    import json as _json

    exclude_terms = dedupe_query_terms(
        exclude_keywords.split(",") if exclude_keywords else []
    )
    try:
        exclude_statute_list: list[tuple] = []
        if exclude_statutes:
            parsed = _json.loads(exclude_statutes)
            exclude_statute_list = dedupe_statute_filters([
                (s.get("law", ""), s.get("article"), s.get("sub_ref"))
                for s in parsed
            ])
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"exclude_statutes 格式錯誤：{e}")

    parsed_case_types = parse_case_types(case_types) if case_types else []
    return exclude_terms, exclude_statute_list, parsed_case_types


def _build_citations_response(
    target_info: CitationTargetInfo,
    matched_total: int,
    others_total: int,
    matched_rows: list[dict],
    others_rows: list[dict],
) -> CitationsResponse:
    def to_source(r: dict) -> CitationSource:
        return CitationSource(
            citation_id=r["citation_id"],
            source_id=r["source_id"],
            source_court=_simplify_court(r["source_court_raw"] or ""),
            source_court_level=r["source_court_level"],
            case_ref=_fmt_case_ref(r["jyear"], r["jcase_norm"], r["jno"]),
            doc_type=r["doc_type"],
            decision_date=str(r["decision_date"]) if r["decision_date"] else None,
            snippet=r["snippet"],
            raw_match=r["raw_match"],
            statutes=list(r["statutes"]) if r["statutes"] else [],
        )

    return CitationsResponse(
        target=target_info,
        matched_total=matched_total,
        others_total=others_total,
        matched_sources=[to_source(r) for r in matched_rows],
        others_sources=[to_source(r) for r in others_rows],
    )


# ── 取 target 基本資訊 ────────────────────────────────────────────────

def _get_decision_target(conn, target_id: int) -> CitationTargetInfo:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT root_norm, jyear, jcase_norm, jno, doc_type FROM decisions WHERE id = %s",
            (target_id,),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="判決不存在")
    return CitationTargetInfo(
        id=target_id,
        target_type="decision",
        court=row["root_norm"],
        case_ref=_fmt_case_ref(row["jyear"], row["jcase_norm"], row["jno"]),
        doc_type=row["doc_type"],
    )


def _get_authority_target(conn, authority_id: int) -> CitationTargetInfo:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            "SELECT root_norm, doc_type, display FROM authorities WHERE id = %s",
            (authority_id,),
        )
        row = cur.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="權威資料不存在")
    return CitationTargetInfo(
        id=authority_id,
        target_type="authority",
        court=row["root_norm"],
        case_ref=row["display"] or "",
        doc_type=row["doc_type"],
    )


# ── Decision citations ────────────────────────────────────────────────

@router.get("/decisions/{target_id}/citations", response_model=CitationsResponse)
def get_decision_citations_matched(
    target_id: int,
    keywords: str | None = Query(None, description="逗號分隔"),
    statutes: str | None = Query(None, description="JSON array string"),
    exclude_keywords: str | None = Query(None, description="逗號分隔"),
    exclude_statutes: str | None = Query(None, description="JSON array string"),
    case_types: str | None = Query(None, description="逗號分隔"),
    search_cache_key: str | None = Query(None, description="由 /search 回傳；對應此次搜尋的 source_ids 快取 key"),
    ranked_source_ids: str | None = Query(None, description="逗號分隔；/search 回傳的 preview source ids"),
):
    query_terms, statute_list = _parse_citation_params(keywords, statutes)
    exclude_terms, exclude_statute_list, parsed_case_types = _parse_exclude_and_case_type_params(
        exclude_keywords, exclude_statutes, case_types
    )
    parsed_ranked_source_ids = _parse_ranked_source_ids(ranked_source_ids)
    with get_conn() as conn:
        target_info = _get_decision_target(conn, target_id)
        resolved_source_ids = _resolve_source_ids_for_citations(
            query_terms,
            statute_list,
            exclude_terms,
            exclude_statute_list,
            parsed_case_types,
            search_cache_key,
        )
        matched_rows, matched_total, others_total = _citation_rows(
            conn,
            "c.target_canonical_id",
            target_id,
            query_terms,
            statute_list,
            resolved_source_ids,
            True,
            ranked_source_ids=parsed_ranked_source_ids,
        )
        others_rows, _matched_total, _others_total = _citation_rows(
            conn,
            "c.target_canonical_id",
            target_id,
            query_terms,
            statute_list,
            resolved_source_ids,
            False,
            shared_counts=(matched_total + others_total, matched_total),
        )
    return _build_citations_response(
        target_info,
        matched_total=matched_total,
        others_total=others_total,
        matched_rows=matched_rows,
        others_rows=others_rows,
    )

# ── Authority citations ───────────────────────────────────────────────

@router.get("/authorities/{authority_id}/citations", response_model=CitationsResponse)
def get_authority_citations_matched(
    authority_id: int,
    keywords: str | None = Query(None, description="逗號分隔"),
    statutes: str | None = Query(None, description="JSON array string"),
    exclude_keywords: str | None = Query(None, description="逗號分隔"),
    exclude_statutes: str | None = Query(None, description="JSON array string"),
    case_types: str | None = Query(None, description="逗號分隔"),
    search_cache_key: str | None = Query(None, description="由 /search 回傳；對應此次搜尋的 source_ids 快取 key"),
    ranked_source_ids: str | None = Query(None, description="逗號分隔；/search 回傳的 preview source ids"),
):
    query_terms, statute_list = _parse_citation_params(keywords, statutes)
    exclude_terms, exclude_statute_list, parsed_case_types = _parse_exclude_and_case_type_params(
        exclude_keywords, exclude_statutes, case_types
    )
    parsed_ranked_source_ids = _parse_ranked_source_ids(ranked_source_ids)
    with get_conn() as conn:
        target_info = _get_authority_target(conn, authority_id)
        resolved_source_ids = _resolve_source_ids_for_citations(
            query_terms,
            statute_list,
            exclude_terms,
            exclude_statute_list,
            parsed_case_types,
            search_cache_key,
        )
        matched_rows, matched_total, others_total = _citation_rows(
            conn,
            "c.target_authority_id",
            authority_id,
            query_terms,
            statute_list,
            resolved_source_ids,
            True,
            ranked_source_ids=parsed_ranked_source_ids,
        )
        others_rows, _matched_total, _others_total = _citation_rows(
            conn,
            "c.target_authority_id",
            authority_id,
            query_terms,
            statute_list,
            resolved_source_ids,
            False,
            shared_counts=(matched_total + others_total, matched_total),
        )
    return _build_citations_response(
        target_info,
        matched_total=matched_total,
        others_total=others_total,
        matched_rows=matched_rows,
        others_rows=others_rows,
    )
