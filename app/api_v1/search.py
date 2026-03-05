from collections import defaultdict
from fastapi import APIRouter, HTTPException
from app.db import get_conn
from app.search_service import (
    dedupe_query_terms,
    dedupe_statute_filters,
    parse_case_types,
    search_source_ids_opensearch,
    fetch_search_citation_rows,
    aggregate_target_rankings,
    fetch_total_citation_counts,
    fetch_css_statutes_batch,
)
from app.api_v1.schemas import (
    SearchRequest,
    SearchResponse,
    SearchResultItem,
    SearchContext,
    StatuteFilter,
    MatchedCitationSource,
)

router = APIRouter()


def _fmt_case_ref(jyear, jcase_norm, jno, display_title=None):
    if display_title:
        return display_title
    if jyear is None or jcase_norm is None or jno is None:
        return ""
    return f"{jyear}年度{jcase_norm}字第{jno}號"


def _simplify_court(unit_norm: str) -> str:
    if not unit_norm:
        return unit_norm
    if "簡易庭" in unit_norm:
        idx = unit_norm.find("簡易庭")
        prefix = unit_norm[:idx]
        court_idx = prefix.rfind("法院")
        if court_idx != -1:
            return prefix[:court_idx + 2]
    return unit_norm


def _target_key(row: dict) -> str:
    """產生 target 唯一 key：d:{id} 或 a:{id}。"""
    if row.get("target_id"):
        return f"d:{row['target_id']}"
    return f"a:{row['target_authority_id']}"


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

        # per-citation rows（score 只算這一次）
        citation_rows = fetch_search_citation_rows(
            conn, source_ids, query_terms, statute_filters,
        )

        # 聚合 target 排行
        all_rankings = aggregate_target_rankings(citation_rows)

        # 批次查 total citation count
        target_ids = [r["target_id"] for r in all_rankings if r["target_id"]]
        auth_ids = [r["target_authority_id"] for r in all_rankings if r["target_authority_id"]]
        total_counts = fetch_total_citation_counts(conn, target_ids, auth_ids)
        for r in all_rankings:
            key = (r["target_id"], r["target_authority_id"])
            r["total_citation_count"] = total_counts.get(key, 0)

        # 排序
        if req.sort == "total_citation_count":
            all_rankings.sort(key=lambda x: (-(x["total_citation_count"]), -(x["citation_count"])))

        # 分頁
        total = len(all_rankings)
        start = (req.page - 1) * req.page_size
        page_rankings = all_rankings[start:start + req.page_size]

        # 當前頁 target keys
        page_target_keys = set()
        for r in page_rankings:
            if r["target_id"]:
                page_target_keys.add(("d", r["target_id"]))
            else:
                page_target_keys.add(("a", r["target_authority_id"]))

        # 從 citation_rows 篩出當前頁 target 的 matched citations
        citation_ids = []
        matched_by_target: dict[str, list[dict]] = defaultdict(list)
        for row in citation_rows:
            if row["target_id"]:
                tk = ("d", row["target_id"])
                tkey_str = f"d:{row['target_id']}"
            else:
                tk = ("a", row["target_authority_id"])
                tkey_str = f"a:{row['target_authority_id']}"
            if tk in page_target_keys:
                matched_by_target[tkey_str].append(row)
                citation_ids.append(row["citation_id"])

        # 批次查 citation statutes
        css_map = fetch_css_statutes_batch(conn, citation_ids)

    # 組裝 results
    results = [
        SearchResultItem(
            target_id=row.get("target_id"),
            authority_id=row.get("target_authority_id"),
            court=row.get("target_root_norm") or "",
            court_level=row.get("target_level"),
            jyear=row.get("jyear"),
            jcase_norm=row.get("jcase_norm"),
            jno=row.get("jno"),
            case_ref=_fmt_case_ref(
                row.get("jyear"), row.get("jcase_norm"),
                row.get("jno"), row.get("display_title"),
            ),
            doc_type=row.get("doc_type"),
            total_citation_count=int(row.get("total_citation_count") or 0),
            matched_citation_count=int(row.get("citation_count") or 0),
            score=float(row.get("score") or 0),
        )
        for row in page_rankings
    ]

    # 組裝 matched_citations：per-target sorted citation list
    matched_citations: dict[str, list[MatchedCitationSource]] = {}
    for tkey, rows in matched_by_target.items():
        # 排序：court_level ASC, score DESC
        rows.sort(key=lambda r: (
            r["source_court_level"] if r["source_court_level"] is not None else 99,
            -(r["score"] or 0),
        ))
        matched_citations[tkey] = [
            MatchedCitationSource(
                citation_id=r["citation_id"],
                source_id=r["source_id"],
                source_court=_simplify_court(r["source_unit_norm"] or ""),
                source_court_level=r["source_court_level"],
                case_ref=_fmt_case_ref(
                    r["source_jyear"], r["source_jcase_norm"], r["source_jno"],
                ),
                doc_type=r["source_doc_type"],
                decision_date=str(r["source_decision_date"]) if r["source_decision_date"] else None,
                snippet=r["snippet"],
                raw_match=r["raw_match"],
                statutes=css_map.get(r["citation_id"], []),
                score=float(r["score"] or 0),
            )
            for r in rows
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
        matched_citations=matched_citations,
    )
