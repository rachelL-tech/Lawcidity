"""
展開單一 target 的 citation 列表。

職責：
- GET /decisions/{id}/citations  — 展開用 preview，一次回 matched / others
- GET /authorities/{id}/citations
- 使用 PostgreSQL 組裝 matched / others 展開資料。
"""

import json

from fastapi import APIRouter, Depends, HTTPException
from app.citation_preview import (
    CITATIONS_PREVIEW_LIMIT,
    fetch_citation_preview_rows,
)
from app.db import get_conn
from app.query_normalization import (
    dedupe_query_terms,
    dedupe_statute_filters,
    parse_case_types,
)
from app.search_cache import get_cached_source_ids
from app.opensearch_service import (
    search_source_ids_opensearch,
)
from app.api_v1.schemas import (
    CitationQueryParams,
    CitationsResponse,
    ParsedCitationQuery,
    CitationSource,
)

router = APIRouter()


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
    cached_source_ids = get_cached_source_ids(search_cache_key)
    if cached_source_ids:
        return cached_source_ids

    try:
        return search_source_ids_opensearch(
            query_terms=query_terms,
            case_types=case_types,
            statute_filters=statute_filters,
            exclude_terms=exclude_terms,
            exclude_statute_filters=exclude_statute_filters,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"搜尋服務失敗：{exc}") from exc


def _parse_citation_query(params: CitationQueryParams) -> ParsedCitationQuery:
    query_terms = dedupe_query_terms(
        params.keywords.split(",") if params.keywords else []
    )
    try:
        statute_list: list[tuple[str, str | None, str | None]] = []
        if params.statutes:
            parsed = json.loads(params.statutes)
            statute_list = dedupe_statute_filters([
                (s.get("law", ""), s.get("article"), s.get("sub_ref"))
                for s in parsed
            ])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"statutes 格式錯誤：{exc}") from exc

    if not query_terms and not statute_list:
        raise HTTPException(status_code=400, detail="keywords 和 statutes 至少填一個")

    exclude_terms = dedupe_query_terms(
        params.exclude_keywords.split(",") if params.exclude_keywords else []
    )
    try:
        exclude_statute_list: list[tuple[str, str | None, str | None]] = []
        if params.exclude_statutes:
            parsed = json.loads(params.exclude_statutes)
            exclude_statute_list = dedupe_statute_filters([
                (s.get("law", ""), s.get("article"), s.get("sub_ref"))
                for s in parsed
            ])
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"exclude_statutes 格式錯誤：{exc}") from exc

    preview_source_ids: list[int] | None = None
    if params.preview_source_ids:
        preview_source_ids = []
        seen: set[int] = set()
        for part in params.preview_source_ids.split(","):
            value = part.strip()
            if not value:
                continue
            try:
                source_id = int(value)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail="preview_source_ids 格式錯誤") from exc
            if source_id in seen:
                continue
            seen.add(source_id)
            preview_source_ids.append(source_id)
            if len(preview_source_ids) >= CITATIONS_PREVIEW_LIMIT:
                break
        preview_source_ids = preview_source_ids or None

    return ParsedCitationQuery(
        query_terms=query_terms,
        statute_list=statute_list,
        exclude_terms=exclude_terms,
        exclude_statute_list=exclude_statute_list,
        case_types=parse_case_types(params.case_types) if params.case_types else [],
        search_cache_key=params.search_cache_key,
        preview_source_ids=preview_source_ids,
    )


def _build_citations_response(
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
        matched_total=matched_total,
        others_total=others_total,
        matched_sources=[to_source(r) for r in matched_rows],
        others_sources=[to_source(r) for r in others_rows],
    )


# ── Decision citations ────────────────────────────────────────────────

@router.get("/decisions/{target_id}/citations", response_model=CitationsResponse)
def get_decision_citations_matched(
    target_id: int,
    params: CitationQueryParams = Depends(),
):
    parsed = _parse_citation_query(params)
    with get_conn() as conn:
        resolved_source_ids = _resolve_source_ids_for_citations(
            parsed.query_terms,
            parsed.statute_list,
            parsed.exclude_terms,
            parsed.exclude_statute_list,
            parsed.case_types,
            parsed.search_cache_key,
        )
        matched_rows, matched_total, others_total = fetch_citation_preview_rows(
            conn,
            "c.target_canonical_id",
            target_id,
            parsed.query_terms,
            parsed.statute_list,
            resolved_source_ids,
            True,
            preview_source_ids=parsed.preview_source_ids,
        )
        others_rows, _matched_total, _others_total = fetch_citation_preview_rows(
            conn,
            "c.target_canonical_id",
            target_id,
            parsed.query_terms,
            parsed.statute_list,
            resolved_source_ids,
            False,
            shared_counts=(matched_total + others_total, matched_total),
        )
    return _build_citations_response(
        matched_total=matched_total,
        others_total=others_total,
        matched_rows=matched_rows,
        others_rows=others_rows,
    )

# ── Authority citations ───────────────────────────────────────────────

@router.get("/authorities/{authority_id}/citations", response_model=CitationsResponse)
def get_authority_citations_matched(
    authority_id: int,
    params: CitationQueryParams = Depends(),
):
    parsed = _parse_citation_query(params)
    with get_conn() as conn:
        resolved_source_ids = _resolve_source_ids_for_citations(
            parsed.query_terms,
            parsed.statute_list,
            parsed.exclude_terms,
            parsed.exclude_statute_list,
            parsed.case_types,
            parsed.search_cache_key,
        )
        matched_rows, matched_total, others_total = fetch_citation_preview_rows(
            conn,
            "c.target_authority_id",
            authority_id,
            parsed.query_terms,
            parsed.statute_list,
            resolved_source_ids,
            True,
            preview_source_ids=parsed.preview_source_ids,
        )
        others_rows, _matched_total, _others_total = fetch_citation_preview_rows(
            conn,
            "c.target_authority_id",
            authority_id,
            parsed.query_terms,
            parsed.statute_list,
            resolved_source_ids,
            False,
            shared_counts=(matched_total + others_total, matched_total),
        )
    return _build_citations_response(
        matched_total=matched_total,
        others_total=others_total,
        matched_rows=matched_rows,
        others_rows=others_rows,
    )
