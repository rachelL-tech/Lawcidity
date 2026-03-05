from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from app.db import get_conn
from app.search_service import (
    tokenize_query,
    dedupe_query_terms,
    dedupe_statute_filters,
    build_statute_filters,
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
        # e.g. 臺灣板橋地方法院板橋簡易庭 → 臺灣板橋地方法院
        idx = unit_norm.find("簡易庭")
        # Find the last occurrence of "法院" before 簡易庭
        prefix = unit_norm[:idx]
        court_idx = prefix.rfind("法院")
        if court_idx != -1:
            return prefix[:court_idx + 2]
    return unit_norm


def _citation_rows_v1(
    conn,
    target_col: str,
    target_val: int,
    query_terms: list[str],
    statute_filters: list[tuple],
) -> list[dict]:
    """查詢 citation rows，帶 court_level、is_matched、score。"""
    params: dict = {"target_val": target_val}

    # is_matched 條件
    if query_terms or statute_filters:
        conds: list[str] = []
        for idx, term in enumerate(query_terms):
            k = f"m_kw_{idx}"
            conds.append(f"src.clean_text ILIKE %({k})s")
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
            conds.append(
                f"EXISTS (SELECT 1 FROM decision_reason_statutes drs"
                f" WHERE drs.decision_id = src.id AND {inner})"
            )
        match_cond = " AND ".join(conds)
        is_matched_sql = f"({match_cond})"
    else:
        is_matched_sql = "TRUE"

    # score：keyword snippets + statute
    if query_terms:
        kw_parts = []
        for idx, term in enumerate(query_terms):
            k = f"sc_kw_{idx}"
            params[k] = f"%{term}%"
            kw_parts.append(f"(c.snippet ILIKE %({k})s)::int")
        keyword_score_sql = " + ".join(kw_parts)
    else:
        keyword_score_sql = "0"

    sql = f"""
        SELECT
            c.id                            AS citation_id,
            c.source_id,
            src.unit_norm                   AS source_court_raw,
            cu.level                        AS source_court_level,
            src.jyear                       AS jyear,
            src.jcase_norm                  AS jcase_norm,
            src.jno                         AS jno,
            src.doc_type                    AS doc_type,
            src.decision_date               AS decision_date,
            c.snippet,
            c.raw_match,
            COALESCE(
                json_agg(
                    json_build_object('law', css.law, 'article', css.article_raw, 'sub_ref', css.sub_ref)
                    ORDER BY css.law, css.article_raw, css.sub_ref
                ) FILTER (WHERE css.id IS NOT NULL),
                '[]'::json
            )                               AS statutes,
            ({is_matched_sql})              AS is_matched,
            {keyword_score_sql}             AS score
        FROM citations c
        JOIN decisions src ON c.source_id = src.id
        LEFT JOIN court_units cu ON cu.id = src.court_unit_id
        LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
        WHERE {target_col} = %(target_val)s
        GROUP BY c.id, c.source_id, src.unit_norm, cu.level,
                 src.jyear, src.jcase_norm, src.jno,
                 src.doc_type, src.decision_date
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


@router.get("/decisions/{target_id}/citations", response_model=CitationsResponse)
def get_citations(
    target_id: int,
    keywords: str | None = Query(None, description="逗號分隔"),
    statutes: str | None = Query(None, description="JSON array string"),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
):
    import json as _json

    query_terms = dedupe_query_terms(
        tokenize_query(keywords.replace(",", " ") if keywords else None)
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

    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """SELECT d.unit_norm, d.root_norm, d.jyear, d.jcase_norm, d.jno
                   FROM decisions d WHERE d.id = %s""",
                (target_id,),
            )
            target_row = cur.fetchone()
        if not target_row:
            raise HTTPException(status_code=404, detail="判決不存在")

        rows = _citation_rows_v1(conn, "c.target_id", target_id, query_terms, statute_list)

    # Sort: matched first (court_level ASC, score DESC), then others
    matched = [r for r in rows if r["is_matched"]]
    others = [r for r in rows if not r["is_matched"]]

    def sort_key(r):
        level = r["source_court_level"] if r["source_court_level"] is not None else 99
        return (level, -(r["score"] or 0))

    matched.sort(key=sort_key)
    others.sort(key=sort_key)
    sorted_rows = matched + others

    total = len(sorted_rows)
    matched_total = len(matched)
    start = (page - 1) * page_size
    page_rows = sorted_rows[start:start + page_size]

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
            raw_match=r["raw_match"],
            statutes=list(r["statutes"]) if r["statutes"] else [],
            is_matched=bool(r["is_matched"]),
            score=float(r["score"] or 0),
        )
        for r in page_rows
    ]

    return CitationsResponse(
        target=CitationTargetInfo(
            id=target_id,
            court=target_row["root_norm"],
            case_ref=_fmt_case_ref(
                target_row["jyear"], target_row["jcase_norm"], target_row["jno"]
            ),
        ),
        total=total,
        matched_total=matched_total,
        sources=sources,
    )


@router.get("/decisions/{id}", response_model=DecisionDetail)
def get_decision(id: int):
    with get_conn() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT
                    d.id,
                    d.unit_norm,
                    d.root_norm,
                    cu.level            AS court_level,
                    d.jyear,
                    d.jcase_norm,
                    d.jno,
                    d.doc_type,
                    d.decision_date,
                    d.title,
                    d.clean_text,
                    (
                        SELECT COUNT(DISTINCT c.source_id)
                        FROM citations c
                        WHERE c.target_id = d.id
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

    statutes = [
        DecisionStatute(
            law=s["law"],
            article=s["article"],
            sub_ref=s["sub_ref"],
            count=int(s["count"]),
        )
        for s in statute_rows
    ]

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
        statutes=statutes,
    )
