"""
搜尋排行榜 endpoint。

職責：
- POST /search — 接收搜尋條件，回傳被引用 target 的排行列表

流程：
1. 打 OpenSearch 拿符合搜尋條件的 source_ids（引用方判決）
2. 依 source_ids 查 target 排行（fetch_target_rankings）
3. 回傳分頁排行，含 matched_citation_count、total_citation_count、score

不包含 citation 明細，前端展開 target 時另打 citations.py 的 endpoint。
"""
from fastapi import APIRouter, HTTPException
from app.db import get_conn
from app.search_service import (
    dedupe_query_terms,
    dedupe_statute_filters,
    parse_case_types,
    search_source_ids_opensearch,
    fetch_target_rankings,
)
from app.api_v1.schemas import (
    SearchRequest,
    SearchResponse,
    SearchResultItem,
    SearchContext,
    StatuteFilter,
)

router = APIRouter()


def _fmt_case_ref(jyear, jcase_norm, jno, display_title=None):
    if display_title:
        return display_title
    if jyear is None or jcase_norm is None or jno is None:
        return ""
    return f"{jyear}年度{jcase_norm}字第{jno}號"


def _to_statute_filter_objs(statute_filters: list[tuple]) -> list[StatuteFilter]:
    return [
        StatuteFilter(law=law, article=article, sub_ref=sub_ref)
        for law, article, sub_ref in statute_filters
    ]


@router.post("/search", response_model=SearchResponse)
def search(req: SearchRequest):
    try:
        query_terms = dedupe_query_terms(req.keywords)
        statute_filters = dedupe_statute_filters([
            (s.law, s.article, s.sub_ref) for s in req.statutes
        ])
        exclude_terms = dedupe_query_terms(req.exclude_keywords)
        exclude_statute_filters = dedupe_statute_filters([
            (s.law, s.article, s.sub_ref) for s in req.exclude_statutes
        ])
        case_types = parse_case_types(",".join(req.case_types)) if req.case_types else []
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    with get_conn() as conn:
        try:
            source_ids = search_source_ids_opensearch(
                query_terms=query_terms,
                case_types=case_types,
                statute_filters=statute_filters,
                exclude_terms=exclude_terms,
                exclude_statute_filters=exclude_statute_filters,
                source_limit=None,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"搜尋服務失敗：{e}")

        all_rankings = fetch_target_rankings(
            conn, source_ids, query_terms, statute_filters,
            doc_types=req.doc_types or None,
            court_levels=req.court_levels or None,
        )

    if req.sort == "total_citation_count":
        all_rankings.sort(key=lambda x: (-(x["total_citation_count"] or 0), -(x["matched_citation_count"] or 0)))

    total = len(all_rankings)
    start = (req.page - 1) * req.page_size
    page_rankings = all_rankings[start:start + req.page_size]

    results = [
        SearchResultItem(
            target_id=row.get("target_id"),
            authority_id=row.get("target_authority_id"),
            court=row.get("court") or "",
            court_level=row.get("court_level"),
            jyear=row.get("jyear"),
            jcase_norm=row.get("jcase_norm"),
            jno=row.get("jno"),
            case_ref=_fmt_case_ref(
                row.get("jyear"), row.get("jcase_norm"),
                row.get("jno"), row.get("display_title"),
            ),
            doc_type=row.get("doc_type"),
            total_citation_count=int(row.get("total_citation_count") or 0),
            matched_citation_count=int(row.get("matched_citation_count") or 0),
            score=float(row.get("score") or 0),
        )
        for row in page_rankings
    ]

    return SearchResponse(
        total=total,
        page=req.page,
        page_size=req.page_size,
        source_count=len(source_ids),
        results=results,
        search_context=SearchContext(
            keywords=query_terms,
            statutes=_to_statute_filter_objs(statute_filters),
            exclude_keywords=exclude_terms,
            exclude_statutes=_to_statute_filter_objs(exclude_statute_filters),
        ),
    )
