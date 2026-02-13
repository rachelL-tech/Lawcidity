import os
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
import psycopg
from psycopg.rows import dict_row

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/citations"
)

app = FastAPI(title="Citation Rankings")


def get_conn():
    return psycopg.connect(DB_URL, row_factory=dict_row)


@app.get("/api/rankings")
def rankings(limit: int = 100):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    d.id,
                    d.court_root_norm  AS target_court,
                    d.jyear,
                    d.jcase_norm,
                    d.jno,
                    COUNT(c.id)        AS citation_count
                FROM decisions d
                JOIN citations c ON c.target_id = d.id
                GROUP BY d.id
                ORDER BY citation_count DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()


@app.get("/api/citations/{target_id}")
def citations(target_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT court_root_norm, jyear, jcase_norm, jno FROM decisions WHERE id = %s",
                (target_id,)
            )
            target = cur.fetchone()
            if not target:
                raise HTTPException(status_code=404, detail="Not found")

            cur.execute("""
                SELECT
                    c.id                 AS citation_id,
                    src.court_root_norm  AS source_court,
                    src.jyear,
                    src.jcase_norm,
                    src.jno,
                    c.snippet,
                    c.raw_match,
                    COALESCE(
                        json_agg(
                            json_build_object('law', css.law, 'article', css.article_raw, 'sub', css.sub_ref)
                            ORDER BY css.law, css.article_raw, css.sub_ref
                        ) FILTER (WHERE css.id IS NOT NULL),
                        '[]'
                    ) AS statutes
                FROM citations c
                JOIN decisions src ON c.source_id = src.id
                LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
                WHERE c.target_id = %s
                GROUP BY c.id, src.court_root_norm, src.jyear, src.jcase_norm,
                         src.jno, src.decision_date
                ORDER BY src.decision_date DESC NULLS LAST
            """, (target_id,))
            return {"target": target, "sources": cur.fetchall()}


# 靜態檔案（index.html 等）
app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
