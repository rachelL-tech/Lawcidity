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
    semantic_chunk_search,
    fetch_semantic_source_rankings,
)
from app.api_v1.schemas import (
    SearchRequest,
    SearchResponse,
    SearchResultItem,
    SearchContext,
    StatuteFilter,
    RerankRequest,
    SemanticSearchRequest,
    SemanticSearchResponse,
    SemanticSourceItem,
    SemanticTarget,
    RagSearchRequest,
    RagSearchResponse,
    RagResultItem,
    RagResultTarget,
)
from app.rag_service import rag_search

router = APIRouter()


def _fmt_case_ref(display_title):
    return display_title or ""


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
        )

    if req.sort == "matched_citation_count":
        all_rankings.sort(key=lambda x: (
            -(x["matched_citation_count"] or 0),
            -(x["score"] or 0),
            (x["court_level"] if x["court_level"] is not None else 99),
        ))
    elif req.sort == "total_citation_count":
        all_rankings.sort(key=lambda x: (
            -(x["total_citation_count"] or 0),
            -(x["score"] or 0),
            (x["court_level"] if x["court_level"] is not None else 99),
        ))

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
            case_ref=_fmt_case_ref(row.get("display_title")),
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
        source_ids=source_ids,
        results=results,
        search_context=SearchContext(
            keywords=query_terms,
            statutes=_to_statute_filter_objs(statute_filters),
            exclude_keywords=exclude_terms,
            exclude_statutes=_to_statute_filter_objs(exclude_statute_filters),
        ),
    )


@router.post("/search/semantic", response_model=SemanticSearchResponse)
def search_semantic(req: SemanticSearchRequest):
    try:
        chunks = semantic_chunk_search(
            query=req.query,
            case_type=req.case_type,
            k=req.k,
        )
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"語意搜尋失敗：{e}")

    with get_conn() as conn:
        all_results = fetch_semantic_source_rankings(conn, chunks)

    total = len(all_results)
    start = (req.page - 1) * req.page_size
    page_results = all_results[start:start + req.page_size]

    return SemanticSearchResponse(
        total=total,
        page=req.page,
        page_size=req.page_size,
        results=[
            SemanticSourceItem(
                **{**r, "cited_targets": [SemanticTarget(**t) for t in r["cited_targets"]]}
            )
            for r in page_results
        ],
    )


@router.post("/search/rag", response_model=RagSearchResponse)
def search_rag(req: RagSearchRequest):
    statutes = [(s.law, s.article) for s in req.statutes]

    with get_conn() as conn:
        try:
            results = rag_search(
                conn,
                req.query,
                case_type=req.case_type,
                statutes=statutes,
                boost=req.boost,
                authority_boost=req.authority_boost,
                top=req.top,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    return RagSearchResponse(
        total=len(results),
        results=[
            RagResultItem(
                type=r["type"],
                decision_id=r["decision_id"],
                root_norm=r["root_norm"],
                display_title=r["display_title"],
                doc_type=r["doc_type"],
                decision_date=r["decision_date"],
                case_type=r["case_type"],
                score=r["score"],
                sim=r["sim"],
                statute_hit=r["statute_hit"],
                chunk_count=r["chunk_count"],
                chunk_types=r["chunk_types"],
                best_chunk_text=r["best_chunk_text"],
                targets=[
                    RagResultTarget(
                        id=t["id"],
                        display_title=t["display_title"],
                        root_norm=t["root_norm"],
                        total_citation_count=t["total_citation_count"],
                    )
                    for t in r["targets"]
                ],
            )
            for r in results
        ],
    )


@router.post("/search/rerank", response_model=SearchResponse)
def rerank(req: RerankRequest):
    """只重跑 PostgreSQL target ranking，不重打 OpenSearch。"""
    if not req.source_ids:
        return SearchResponse(
            total=0, page=req.page, page_size=req.page_size,
            source_count=0, source_ids=[],
            results=[], search_context=SearchContext(
                keywords=req.keywords,
                statutes=[StatuteFilter(law=s.law, article=s.article, sub_ref=s.sub_ref) for s in req.statutes],
                exclude_keywords=[], exclude_statutes=[],
            ),
        )

    query_terms = dedupe_query_terms(req.keywords)
    statute_filters = dedupe_statute_filters([
        (s.law, s.article, s.sub_ref) for s in req.statutes
    ])

    with get_conn() as conn:
        all_rankings = fetch_target_rankings(
            conn, req.source_ids, query_terms, statute_filters,
            doc_types=req.doc_types or None,
            court_levels=req.court_levels or None,
        )

    if req.sort == "matched_citation_count":
        all_rankings.sort(key=lambda x: (
            -(x["matched_citation_count"] or 0),
            -(x["score"] or 0),
            (x["court_level"] if x["court_level"] is not None else 99),
        ))
    elif req.sort == "total_citation_count":
        all_rankings.sort(key=lambda x: (
            -(x["total_citation_count"] or 0),
            -(x["score"] or 0),
            (x["court_level"] if x["court_level"] is not None else 99),
        ))

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
            case_ref=_fmt_case_ref(row.get("display_title")),
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
        source_count=len(req.source_ids),
        source_ids=req.source_ids,
        results=results,
        search_context=SearchContext(
            keywords=query_terms,
            statutes=_to_statute_filter_objs(statute_filters),
            exclude_keywords=[], exclude_statutes=[],
        ),
    )
