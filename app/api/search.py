"""
搜尋排行榜 endpoint。

職責：
- POST /search — 接收搜尋條件，回傳被引用 target 的排行列表

流程：
1. 打 OpenSearch 拿符合搜尋條件的 source_ids（引用方判決）
2. 依 source_ids 查 source-target OpenSearch 分數，再用 PostgreSQL 補 target metadata
4. 回傳分頁排行，含 total_citation_count 與前 5 名 matched source ids（preview 用）

不包含 citation 明細，前端展開 target 時另打 citations.py 的 endpoint。
"""
from dataclasses import dataclass

from fastapi import APIRouter, HTTPException
from app.db import get_conn
from app.search_cache import (
    create_search_cache,
    get_cached_rankings,
    get_cached_source_ids,
    update_cached_rankings,
)
from app.target_ranking import fetch_target_rankings_by_relevance
from app.query_normalization import (
    dedupe_query_terms,
    dedupe_statute_filters,
    parse_case_types,
)
from app.opensearch_service import (
    search_source_ids_opensearch,
)
from app.api.schemas import (
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


@dataclass
class _NormalizedSearchInput:
    query_terms: list[str]
    statute_filters: list[tuple[str, str | None, str | None]]
    exclude_terms: list[str]
    exclude_statute_filters: list[tuple[str, str | None, str | None]]
    case_types: list[str]

def _ensure_ordered_indexes(
    rows: list[dict],
    ordered_indexes: dict[str, list[int]],
    sort: str,
) -> tuple[list[int], bool]:
    indexes = ordered_indexes.get(sort)
    if indexes is not None:
        return indexes, False

    if sort == "total_citation_count":
        indexes = sorted(
            range(len(rows)),
            key=lambda idx: (
                -(rows[idx].get("total_citation_count") or 0),
                -(rows[idx].get("matched_citation_count") or 0),
                rows[idx].get("court_level") if rows[idx].get("court_level") is not None else 99,
            ),
        )
    else:
        indexes = list(range(len(rows)))
    if sort != "relevance":
        ordered_indexes[sort] = indexes
    return indexes, True


def _filter_rankings(
    rows: list[dict],
    ordered_indexes: list[int],
    doc_types: list[str] | None = None,
    court_levels: list[int] | None = None,
) -> list[int]:
    filtered = ordered_indexes
    if doc_types:
        filtered = [idx for idx in filtered if rows[idx].get("doc_type") in doc_types]
    if court_levels:
        filtered = [idx for idx in filtered if rows[idx].get("court_level") in court_levels]
    return filtered


def _normalize_search_request(
    req: SearchRequest | RerankRequest,
) -> _NormalizedSearchInput:
    query_terms = dedupe_query_terms(req.keywords)
    statute_filters = dedupe_statute_filters([
        (s.law, s.article, s.sub_ref) for s in req.statutes
    ])
    exclude_terms = dedupe_query_terms(req.exclude_keywords)
    exclude_statute_filters = dedupe_statute_filters([
        (s.law, s.article, s.sub_ref) for s in req.exclude_statutes
    ])
    case_types = parse_case_types(",".join(req.case_types)) if req.case_types else []
    return _NormalizedSearchInput(
        query_terms=query_terms,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
        case_types=case_types,
    )


@router.post("/search", response_model=SearchResponse)
def search(req: SearchRequest):
    try:
        normalized = _normalize_search_request(req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    with get_conn() as conn:
        try:
            source_ids = search_source_ids_opensearch(
                query_terms=normalized.query_terms,
                case_types=normalized.case_types,
                statute_filters=normalized.statute_filters,
                exclude_terms=normalized.exclude_terms,
                exclude_statute_filters=normalized.exclude_statute_filters,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"搜尋服務失敗：{e}")

        rows = fetch_target_rankings_by_relevance(
            conn,
            source_ids,
            normalized.query_terms,
            normalized.statute_filters,
        )
    ordered_indexes: dict[str, list[int]] = {}
    search_cache_key = create_search_cache(
        source_ids,
        rows=rows,
        ordered_indexes=ordered_indexes,
    )
    start = (req.page - 1) * req.page_size
    page_rankings = rows[start:start + req.page_size]

    results = [
        SearchResultItem(
            target_id=row.get("target_id"),
            authority_id=row.get("target_authority_id"),
            court=row.get("court") or "",
            court_level=row.get("court_level"),
            jyear=row.get("jyear"),
            jcase_norm=row.get("jcase_norm"),
            jno=row.get("jno"),
            case_ref=row.get("display_title") or "",
            doc_type=row.get("doc_type"),
            total_citation_count=int(row.get("total_citation_count") or 0),
            preview_source_ids=[int(source_id) for source_id in (row.get("preview_source_ids") or [])],
        )
        for row in page_rankings
    ]

    return SearchResponse(
        total=len(rows),
        page=req.page,
        page_size=req.page_size,
        source_count=len(source_ids),
        search_cache_key=search_cache_key,
        results=results,
        search_context=SearchContext(
            keywords=normalized.query_terms,
            statutes=[
                StatuteFilter(law=law, article=article, sub_ref=sub_ref)
                for law, article, sub_ref in normalized.statute_filters
            ],
            exclude_keywords=normalized.exclude_terms,
            exclude_statutes=[
                StatuteFilter(law=law, article=article, sub_ref=sub_ref)
                for law, article, sub_ref in normalized.exclude_statute_filters
            ],
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
    """使用 search_cache_key 重跑 target ranking；cache miss 時重打第一階段 OpenSearch 召回。"""
    try:
        normalized = _normalize_search_request(req)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    source_ids = get_cached_source_ids(req.search_cache_key)
    source_ids_from_cache = source_ids is not None
    cached_rankings = get_cached_rankings(req.search_cache_key)
    search_cache_key = req.search_cache_key

    if source_ids is None:
        try:
            source_ids = search_source_ids_opensearch(
                query_terms=normalized.query_terms,
                case_types=normalized.case_types,
                statute_filters=normalized.statute_filters,
                exclude_terms=normalized.exclude_terms,
                exclude_statute_filters=normalized.exclude_statute_filters,
            )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"搜尋服務失敗：{e}")
        cached_rankings = None
        search_cache_key = None

    if not source_ids:
        return SearchResponse(
            total=0, page=req.page, page_size=req.page_size,
            source_count=0,
            search_cache_key=search_cache_key,
            results=[], 
            search_context=SearchContext(
                keywords=normalized.query_terms,
                statutes=[
                    StatuteFilter(law=law, article=article, sub_ref=sub_ref)
                    for law, article, sub_ref in normalized.statute_filters
                ],
                exclude_keywords=normalized.exclude_terms,
                exclude_statutes=[
                    StatuteFilter(law=law, article=article, sub_ref=sub_ref)
                    for law, article, sub_ref in normalized.exclude_statute_filters
                ],
            ),
        )

    if cached_rankings is None:
        with get_conn() as conn:
            rows = fetch_target_rankings_by_relevance(
                conn,
                source_ids,
                normalized.query_terms,
                normalized.statute_filters,
            )
        ordered_indexes = {}
        if source_ids_from_cache:
            update_cached_rankings(req.search_cache_key, rows, ordered_indexes)
        else:
            search_cache_key = create_search_cache(
                source_ids,
                rows=rows,
                ordered_indexes=ordered_indexes,
            )
    else:
        rows = cached_rankings["rows"]
        ordered_indexes = cached_rankings["ordered_indexes"] or {}

    sort_indexes, indexes_updated = _ensure_ordered_indexes(rows, ordered_indexes, req.sort)
    if indexes_updated and req.sort != "relevance":
        update_cached_rankings(search_cache_key, rows, ordered_indexes)

    filtered_indexes = _filter_rankings(
        rows,
        sort_indexes,
        doc_types=req.doc_types or None,
        court_levels=req.court_levels or None,
    )
    total = len(filtered_indexes)
    start = (req.page - 1) * req.page_size
    page_rankings = [rows[idx] for idx in filtered_indexes[start:start + req.page_size]]

    results = [
        SearchResultItem(
            target_id=row.get("target_id"),
            authority_id=row.get("target_authority_id"),
            court=row.get("court") or "",
            court_level=row.get("court_level"),
            jyear=row.get("jyear"),
            jcase_norm=row.get("jcase_norm"),
            jno=row.get("jno"),
            case_ref=row.get("display_title") or "",
            doc_type=row.get("doc_type"),
            total_citation_count=int(row.get("total_citation_count") or 0),
            preview_source_ids=[int(source_id) for source_id in (row.get("preview_source_ids") or [])],
        )
        for row in page_rankings
    ]

    return SearchResponse(
        total=total,
        page=req.page,
        page_size=req.page_size,
        source_count=len(source_ids),
        search_cache_key=search_cache_key,
        results=results,
        search_context=SearchContext(
            keywords=normalized.query_terms,
            statutes=[
                StatuteFilter(law=law, article=article, sub_ref=sub_ref)
                for law, article, sub_ref in normalized.statute_filters
            ],
            exclude_keywords=normalized.exclude_terms,
            exclude_statutes=[
                StatuteFilter(law=law, article=article, sub_ref=sub_ref)
                for law, article, sub_ref in normalized.exclude_statute_filters
            ],
        ),
    )
