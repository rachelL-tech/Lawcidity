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
from app.opensearch_service import (
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
    RerankRequest,
    RagResultItem,
    RagResultTarget,
    AnalyzeRequest,
    AnalyzeResponse,
    AnalyzeStatute,
    GenerateRequest,
    GenerateResponse,
)
from app.rag_service import rag_search
from app.gemini_service import extract_issues_and_statutes, generate_analysis

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

@router.post("/analyze", response_model=AnalyzeResponse)
def analyze(req: AnalyzeRequest):
    """Gemini 爭點 / 法條提取。"""
    if not req.text.strip():
        raise HTTPException(status_code=400, detail="text 不可為空")
    try:
        result = extract_issues_and_statutes(req.text)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Gemini 分析失敗：{e}")

    return AnalyzeResponse(
        issues=result.get("issues", []),
        statutes=[
            AnalyzeStatute(law=s.get("law", ""), article=s.get("article", ""))
            for s in result.get("statutes", [])
        ],
    )


@router.post("/analyze/generate", response_model=GenerateResponse)
def analyze_generate(req: GenerateRequest):
    """RAG 搜尋 + Gemini 全文分析。"""
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="query 不可為空")

    statutes = [(s.law, s.article) for s in req.statutes]

    with get_conn() as conn:
        try:
            rag_results = rag_search(
                conn,
                req.query,
                issues=req.issues,
                case_type=req.case_type,
                statutes=statutes,
                top=req.top,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    # 轉成 dict 給 Gemini
    try:
        analysis_text = generate_analysis(
            query=req.query,
            issues=req.issues,
            statutes=[{"law": s.law, "article": s.article} for s in req.statutes],
            rag_results=rag_results,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Gemini 生成失敗：{e}")

    items = [
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
                    target_type=t.get("target_type", "decision"),
                )
                for t in r["targets"]
            ],
        )
        for r in rag_results
    ]

    return GenerateResponse(analysis=analysis_text, rag_results=items)


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
