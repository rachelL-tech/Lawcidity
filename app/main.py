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
                    'decision'           AS citation_type,
                    NULL::TEXT           AS auth_type,
                    d.id                 AS target_id,
                    NULL::BIGINT         AS authority_id,
                    d.root_norm          AS target_court,
                    d.jyear,
                    d.jcase_norm,
                    d.jno,
                    (ARRAY_REMOVE(ARRAY_AGG(c.target_doc_type ORDER BY c.id DESC), NULL))[1] AS doc_type,
                    NULL::TEXT           AS display_title,
                    COUNT(c.id)          AS citation_count
                FROM decisions d
                JOIN citations c ON c.target_id = d.id
                GROUP BY d.id
                UNION ALL
                SELECT
                    'authority'          AS citation_type,
                    a.doc_type           AS auth_type,
                    NULL::BIGINT         AS target_id,
                    a.id                 AS authority_id,
                    CASE a.doc_type
                        WHEN '決議'       THEN '最高法院'
                        WHEN '釋字'       THEN '司法院'
                        ELSE split_part(a.ref_key, '|', 1)
                    END                  AS target_court,
                    NULL::SMALLINT       AS jyear,
                    NULL::TEXT           AS jcase_norm,
                    NULL::INT            AS jno,
                    NULL::TEXT           AS case_type,
                    a.display            AS display_title,
                    COUNT(c.id)          AS citation_count
                FROM authorities a
                JOIN citations c ON c.target_authority_id = a.id
                GROUP BY a.id
                ORDER BY citation_count DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()


@app.get("/api/decisions/{target_id}/citations")
def citations(target_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT root_norm AS court_root_norm, jyear, jcase_norm, jno FROM decisions WHERE id = %s",
                (target_id,)
            )
            target = cur.fetchone()
            if not target:
                raise HTTPException(status_code=404, detail="Not found")

            cur.execute("""
                SELECT
                    c.id                 AS citation_id,
                    src.unit_norm        AS source_court,
                    src.jyear,
                    src.jcase_norm,
                    src.jno,
                    src.decision_date,
                    c.snippet,
                    c.raw_match,
                    COALESCE(
                        json_agg(
                            json_build_object('law', css.law, 'article', css.article_raw, 'sub', css.sub_ref)
                            ORDER BY css.law, css.article_raw, css.sub_ref
                        ) FILTER (WHERE css.id IS NOT NULL),
                        '[]'::json
                    ) AS statutes
                FROM citations c
                JOIN decisions src ON c.source_id = src.id
                LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
                WHERE c.target_id = %s
                GROUP BY c.id, src.unit_norm, src.jyear, src.jcase_norm,
                         src.jno, src.decision_date
                ORDER BY src.decision_date DESC NULLS LAST
            """, (target_id,))
            return {"target": target, "sources": cur.fetchall()}


@app.get("/api/authorities/{authority_id}/citations")
def authority_citations(authority_id: int):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT doc_type, ref_key, display FROM authorities WHERE id = %s",
                (authority_id,)
            )
            target = cur.fetchone()
            if not target:
                raise HTTPException(status_code=404, detail="Not found")

            cur.execute("""
                SELECT
                    c.id                 AS citation_id,
                    src.unit_norm        AS source_court,
                    src.jyear,
                    src.jcase_norm,
                    src.jno,
                    src.decision_date,
                    c.snippet,
                    c.raw_match,
                    COALESCE(
                        json_agg(
                            json_build_object('law', css.law, 'article', css.article_raw, 'sub', css.sub_ref)
                            ORDER BY css.law, css.article_raw, css.sub_ref
                        ) FILTER (WHERE css.id IS NOT NULL),
                        '[]'::json
                    ) AS statutes
                FROM citations c
                JOIN decisions src ON c.source_id = src.id
                LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
                WHERE c.target_authority_id = %s
                GROUP BY c.id, src.unit_norm, src.jyear, src.jcase_norm,
                         src.jno, src.decision_date
                ORDER BY src.decision_date DESC NULLS LAST
            """, (authority_id,))
            return {"target": target, "sources": cur.fetchall()}


# 靜態檔案（index.html 等）
app.mount("/", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static"), html=True), name="static")
