"""
展開單一 target 的 citation 列表、查看判決全文。

職責：
- GET /decisions/{id}/citations/matched  — 符合全部搜尋條件的引用來源
- GET /decisions/{id}/citations/others   — 至少一個條件不符合的引用來源
- GET /authorities/{id}/citations/matched
- GET /authorities/{id}/citations/others
- GET /decisions/{id}                    — 判決詳情（全文、法條、引用總數）

使用 search_service 的 score SQL builders 計算 snippet 命中分數，
用於 matched / others 內部的法院層級 + 分數排序。
"""
from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from app.db import get_conn
from app.search_service import (
    dedupe_query_terms,
    dedupe_statute_filters,
    build_keyword_score_sql,
    build_statute_score_sql,
)
from app.api_v1.schemas import (
    CitationsResponse,
    CitationTargetInfo,
    CitationSource,
    DecisionDetail,
    DecisionStatute,
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


# ── Citation 查詢 ─────────────────────────────────────────────────────
#
# 兩支 endpoint：matched（符合全部搜尋條件）、others（未全部符合）。
# 內部共用 _citation_rows()，差異只在 WHERE 的 is_matched 條件正反。
# keywords/statutes 擇一必填，未帶搜尋條件回 400。
# ──────────────────────────────────────────────────────────────────────


def _get_sibling_target_ids(conn, target_id: int) -> list[int]:
    """找同案號（unit_norm/jyear/jcase_norm/jno）下所有 target_id，含自身。
    用於合併不同 doc_type placeholder 的 citations。"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM decisions
            WHERE (unit_norm, jyear, jcase_norm, jno) = (
                SELECT unit_norm, jyear, jcase_norm, jno
                FROM decisions WHERE id = %s
            )
        """, (target_id,))
        rows = cur.fetchall()
    return [r["id"] for r in rows] if rows else [target_id]


def _citation_rows(
    conn,
    target_col: str,
    target_val: int,
    query_terms: list[str],
    statute_filters: list[tuple],
    matched: bool,
) -> list[dict]:
    """查詢 target 的 citations，回傳全部結果。

    matched=True  → 所有搜尋條件都符合（AND）的 citations
    matched=False → 至少一個條件不符合的 citations（others）

    排序：score DESC → court_level ASC（相關性優先，同分才看位階）

    decision target 會自動合併同案號所有 doc_type 的 citations。
    """
    # decision target：找同案號兄弟 ids，合併所有 doc_type 的 citations
    if target_col == "c.target_id":
        sibling_ids = _get_sibling_target_ids(conn, target_val)
        params: dict = {"target_vals": sibling_ids}
        target_filter = "c.target_id = ANY(%(target_vals)s)"
    else:
        params = {"target_val": target_val}
        target_filter = f"{target_col} = %(target_val)s"

    # 建 is_matched 條件（AND 串接）
    match_conds: list[str] = []
    for idx, term in enumerate(query_terms):
        k = f"m_kw_{idx}"
        match_conds.append(
            f"EXISTS (SELECT 1 FROM decisions d2"
            f" WHERE d2.id = c.source_id AND d2.clean_text ILIKE %({k})s)"
        )
        params[k] = f"%{term}%"
    for idx, (law, article, sub_ref) in enumerate(statute_filters):
        lk = f"m_law_{idx}"
        inner = f"drs.law = %({lk})s"
        params[lk] = law
        if article:
            ak = f"m_art_{idx}"
            inner += f" AND drs.article_raw = %({ak})s"
            params[ak] = article
        if sub_ref:
            sk = f"m_sub_{idx}"
            inner += f" AND drs.sub_ref = %({sk})s"
            params[sk] = sub_ref
        match_conds.append(
            f"EXISTS (SELECT 1 FROM decision_reason_statutes drs"
            f" WHERE drs.decision_id = c.source_id AND {inner})"
        )

    is_matched_sql = " AND ".join(match_conds)
    where_filter = is_matched_sql if matched else f"NOT ({is_matched_sql})"

    keyword_score_sql = build_keyword_score_sql(query_terms, params, "c.snippet")
    statute_score_sql = build_statute_score_sql(statute_filters, params, "c.id")

    sql = f"""
        WITH scored AS (
            SELECT
                c.id                            AS citation_id,
                c.source_id,
                src.unit_norm                   AS source_court_raw,
                cu.level                        AS source_court_level,
                src.jyear, src.jcase_norm, src.jno,
                src.doc_type, src.decision_date,
                c.snippet, c.match_start, c.match_end, c.raw_match,
                COALESCE(
                    json_agg(
                        json_build_object('law', css.law, 'article', css.article_raw, 'sub_ref', css.sub_ref)
                        ORDER BY css.law, css.article_raw, css.sub_ref
                    ) FILTER (WHERE css.id IS NOT NULL),
                    '[]'::json
                )                               AS statutes,
                {str(matched).upper()}          AS is_matched,
                ({keyword_score_sql}) + ({statute_score_sql}) AS score
            FROM citations c
            JOIN decisions src ON c.source_id = src.id
            LEFT JOIN court_units cu ON cu.id = src.court_unit_id
            LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
            WHERE {target_filter}
              AND {where_filter}
            GROUP BY c.id, c.source_id, src.unit_norm, cu.level,
                     src.jyear, src.jcase_norm, src.jno,
                     src.doc_type, src.decision_date, c.match_start, c.match_end
        )
        SELECT *
        FROM scored
        ORDER BY score DESC, source_court_level ASC NULLS LAST, citation_id
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


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


def _build_citations_response(
    rows: list[dict],
    target_info: CitationTargetInfo,
    matched_total: int,
) -> CitationsResponse:
    """SQL 已排序（score DESC → court_level ASC），直接組裝回傳。"""
    sources = [
        CitationSource(
            citation_id=r["citation_id"],
            source_id=r["source_id"],
            source_court=_simplify_court(r["source_court_raw"] or ""),
            source_court_level=r["source_court_level"],
            case_ref=_fmt_case_ref(r["jyear"], r["jcase_norm"], r["jno"]),
            doc_type=r["doc_type"],
            decision_date=str(r["decision_date"]) if r["decision_date"] else None,
            snippet=r["snippet"],
            match_start=r["match_start"],
            match_end=r["match_end"],
            raw_match=r["raw_match"],
            statutes=list(r["statutes"]) if r["statutes"] else [],
            is_matched=bool(r["is_matched"]),
            score=float(r["score"] or 0),
        )
        for r in rows
    ]
    return CitationsResponse(
        target=target_info,
        total=len(rows),
        matched_total=matched_total,
        sources=sources,
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

@router.get("/decisions/{target_id}/citations/matched", response_model=CitationsResponse)
def get_decision_citations_matched(
    target_id: int,
    keywords: str | None = Query(None, description="逗號分隔"),
    statutes: str | None = Query(None, description="JSON array string"),
):
    query_terms, statute_list = _parse_citation_params(keywords, statutes)
    with get_conn() as conn:
        target_info = _get_decision_target(conn, target_id)
        rows = _citation_rows(conn, "c.target_id", target_id, query_terms, statute_list, matched=True)
    return _build_citations_response(rows, target_info, matched_total=len(rows))


@router.get("/decisions/{target_id}/citations/others", response_model=CitationsResponse)
def get_decision_citations_others(
    target_id: int,
    keywords: str | None = Query(None, description="逗號分隔"),
    statutes: str | None = Query(None, description="JSON array string"),
):
    query_terms, statute_list = _parse_citation_params(keywords, statutes)
    with get_conn() as conn:
        target_info = _get_decision_target(conn, target_id)
        rows = _citation_rows(conn, "c.target_id", target_id, query_terms, statute_list, matched=False)
    return _build_citations_response(rows, target_info, matched_total=0)


# ── Authority citations ───────────────────────────────────────────────

@router.get("/authorities/{authority_id}/citations/matched", response_model=CitationsResponse)
def get_authority_citations_matched(
    authority_id: int,
    keywords: str | None = Query(None, description="逗號分隔"),
    statutes: str | None = Query(None, description="JSON array string"),
):
    query_terms, statute_list = _parse_citation_params(keywords, statutes)
    with get_conn() as conn:
        target_info = _get_authority_target(conn, authority_id)
        rows = _citation_rows(conn, "c.target_authority_id", authority_id, query_terms, statute_list, matched=True)
    return _build_citations_response(rows, target_info, matched_total=len(rows))


@router.get("/authorities/{authority_id}/citations/others", response_model=CitationsResponse)
def get_authority_citations_others(
    authority_id: int,
    keywords: str | None = Query(None, description="逗號分隔"),
    statutes: str | None = Query(None, description="JSON array string"),
):
    query_terms, statute_list = _parse_citation_params(keywords, statutes)
    with get_conn() as conn:
        target_info = _get_authority_target(conn, authority_id)
        rows = _citation_rows(conn, "c.target_authority_id", authority_id, query_terms, statute_list, matched=False)
    return _build_citations_response(rows, target_info, matched_total=0)


# ── Decision detail ───────────────────────────────────────────────────

@router.get("/decisions/{id}", response_model=DecisionDetail)
def get_decision(id: int):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    d.id, d.unit_norm, d.root_norm,
                    cu.level            AS court_level,
                    d.jyear, d.jcase_norm, d.jno,
                    d.doc_type, d.decision_date, d.title, d.clean_text,
                    (
                        SELECT COUNT(DISTINCT c.source_id)
                        FROM citations c WHERE c.target_id = d.id
                    )                   AS total_citation_count
                FROM decisions d
                LEFT JOIN court_units cu ON cu.id = d.court_unit_id
                WHERE d.id = %s
                """,
                (id,),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="判決不存在")

            cur.execute(
                """
                SELECT law, article_raw AS article, sub_ref, COUNT(*) AS count
                FROM decision_reason_statutes
                WHERE decision_id = %s
                GROUP BY law, article_raw, sub_ref
                ORDER BY law, article_raw, sub_ref
                """,
                (id,),
            )
            statute_rows = cur.fetchall()

    return DecisionDetail(
        id=row["id"],
        court=row["unit_norm"],
        court_root=row["root_norm"],
        court_level=row["court_level"],
        case_ref=_fmt_case_ref(row["jyear"], row["jcase_norm"], row["jno"]),
        doc_type=row["doc_type"],
        decision_date=str(row["decision_date"]) if row["decision_date"] else None,
        title=row["title"],
        clean_text=row["clean_text"],
        total_citation_count=int(row["total_citation_count"] or 0),
        statutes=[
            DecisionStatute(
                law=s["law"], article=s["article"],
                sub_ref=s["sub_ref"], count=int(s["count"]),
            )
            for s in statute_rows
        ],
    )
