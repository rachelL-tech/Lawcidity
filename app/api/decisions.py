from fastapi import APIRouter, HTTPException
from psycopg.rows import dict_row

from app.api.schemas import DecisionDetail, DecisionStatute
from app.db import get_conn

router = APIRouter()


def _fmt_case_ref(jyear, jcase_norm, jno):
    if jyear is None or jcase_norm is None or jno is None:
        return ""
    return f"{jyear}年度{jcase_norm}字第{jno}號"


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
                    d.total_citation_count
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
                law=s["law"],
                article=s["article"],
                sub_ref=s["sub_ref"],
                count=int(s["count"]),
            )
            for s in statute_rows
        ],
    )
