import os
from typing import Literal
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg
from psycopg.rows import dict_row
from app.search_service import (
    tokenize_query,
    dedupe_query_terms,
    dedupe_statute_filters,
    parse_case_types,
    build_statute_filters,
    search_source_ids_opensearch,
    search_source_ids_baseline_pg,
    fetch_rankings_by_source_ids,
)
from app.api_v1.router import router as v1_router

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/citations"
)

app = FastAPI(title="Lawcidity API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# v1 API
app.include_router(v1_router)


def get_conn():
    return psycopg.connect(DB_URL, row_factory=dict_row)


# ── Legacy endpoints (kept for backward compatibility) ────────────────────────

def _citation_rows(
    conn,
    target_col: str,
    target_val: int,
    query_terms: list[str],
    statute_filters: list[tuple],
) -> list[dict]:
    params: dict = {"target_val": target_val}

    if query_terms or statute_filters:
        conds: list[str] = []
        for idx, term in enumerate(query_terms):
            k = f"m_kw_{idx}"
            conds.append(f"d2.clean_text ILIKE %({k})s")
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
                f" WHERE drs.decision_id = d2.id AND {inner})"
            )
        match_inner = " AND ".join(conds)
        is_matched_sql = (
            f"EXISTS (SELECT 1 FROM decisions d2"
            f" WHERE d2.id = c.source_id AND {match_inner})"
        )
    else:
        is_matched_sql = "TRUE"

    sql = f"""
        SELECT
            c.id                 AS citation_id,
            c.source_id,
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
            ) AS statutes,
            ({is_matched_sql}) AS is_matched
        FROM citations c
        JOIN decisions src ON c.source_id = src.id
        LEFT JOIN citation_snippet_statutes css ON css.citation_id = c.id
        WHERE {target_col} = %(target_val)s
        GROUP BY c.id, c.source_id, src.unit_norm, src.jyear, src.jcase_norm,
                 src.jno, src.decision_date
        ORDER BY is_matched DESC NULLS LAST, src.decision_date DESC NULLS LAST
    """

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _split_search_response(target, rows, terms, statute_filters):
    return {
        "target": target,
        "matched_sources": [r for r in rows if r["is_matched"]],
        "other_sources":   [r for r in rows if not r["is_matched"]],
        "search_context": {
            "query_terms": terms,
            "laws": [f[0] for f in statute_filters],
            "statutes": [
                {"law": f[0], "article": f[1], "sub_ref": f[2]}
                for f in statute_filters
            ],
        },
    }


@app.get("/api/search")
def search_rankings(
    q: str | None = Query(None),
    case_type: str | None = Query(None),
    law: list[str] | None = Query(None),
    article: list[str] | None = Query(None),
    sub_ref: list[str] | None = Query(None),
    exclude_q: str | None = Query(None),
    exclude_law: list[str] | None = Query(None),
    exclude_article: list[str] | None = Query(None),
    exclude_sub_ref: list[str] | None = Query(None),
    backend: Literal["opensearch", "pg"] = Query("opensearch"),
    source_limit: int | None = Query(None, ge=1),
    limit: int = Query(100, ge=1, le=500),
):
    try:
        terms = tokenize_query(q)
        case_types = parse_case_types(case_type)
        statute_filters = build_statute_filters(
            laws=law or [],
            articles=article or [],
            sub_refs=sub_ref or [],
        )
        exclude_terms = tokenize_query(exclude_q)
        exclude_statute_filters = build_statute_filters(
            laws=exclude_law or [],
            articles=exclude_article or [],
            sub_refs=exclude_sub_ref or [],
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    query_terms = dedupe_query_terms(terms)
    statute_filters = dedupe_statute_filters(statute_filters)
    exclude_terms = dedupe_query_terms(exclude_terms)
    exclude_statute_filters = dedupe_statute_filters(exclude_statute_filters)

    if not query_terms and not statute_filters:
        raise HTTPException(status_code=400, detail="q 與法條（law+article）至少提供一項")

    with get_conn() as conn:
        try:
            if backend == "opensearch":
                source_ids = search_source_ids_opensearch(
                    query_terms=query_terms,
                    case_types=case_types,
                    statute_filters=statute_filters,
                    exclude_terms=exclude_terms,
                    exclude_statute_filters=exclude_statute_filters,
                    source_limit=source_limit,
                )
            else:
                source_ids = search_source_ids_baseline_pg(
                    conn=conn,
                    query_terms=query_terms,
                    case_types=case_types,
                    statute_filters=statute_filters,
                    exclude_terms=exclude_terms,
                    exclude_statute_filters=exclude_statute_filters,
                    source_limit=source_limit,
                )
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"搜尋服務失敗：{e}")

        targets = fetch_rankings_by_source_ids(
            conn=conn,
            source_ids=source_ids,
            query_terms=query_terms,
            statute_filters=statute_filters,
            limit=limit,
        )

    return {
        "backend": backend,
        "query_terms": query_terms,
        "source_count": len(source_ids),
        "targets": targets,
    }


@app.get("/api/decisions/{target_id}/citations")
def citations(
    target_id: int,
    q: str | None = Query(None),
    law: list[str] | None = Query(None),
    article: list[str] | None = Query(None),
    sub_ref: list[str] | None = Query(None),
):
    terms = tokenize_query(q)
    try:
        statute_filters = build_statute_filters(
            laws=law or [], articles=article or [], sub_refs=sub_ref or []
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT root_norm AS court_root_norm, jyear, jcase_norm, jno FROM decisions WHERE id = %s",
                (target_id,)
            )
            target = cur.fetchone()
            if not target:
                raise HTTPException(status_code=404, detail="Not found")

        rows = _citation_rows(conn, "c.target_id", target_id, terms, statute_filters)

    if terms or statute_filters:
        return _split_search_response(target, rows, terms, statute_filters)
    return {"target": target, "sources": rows}
