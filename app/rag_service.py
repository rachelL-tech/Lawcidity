"""
RAG 語意搜尋 + decision 聚合。

流程：
1. Raw factual query → Voyage API embed
2. IVFFlat ANN knn → top 50 chunks（純語意召回）
3. chunk 計分（sim = 1 - cosine distance）→ 聚合到 decision → top N
"""

import os
from collections import defaultdict

import numpy as np
import psycopg

DIMS = 1024
VOYAGE_MODEL = "voyage-law-2"

_voyage_client = None


def _get_voyage_client():
    global _voyage_client
    if _voyage_client is None:
        try:
            import voyageai
        except ImportError:
            raise RuntimeError("缺少 voyageai 套件，請執行 pip install voyageai")
        api_key = os.environ.get("VOYAGE_API_KEY")
        if not api_key:
            raise RuntimeError("VOYAGE_API_KEY 未設定")
        _voyage_client = voyageai.Client(api_key=api_key)
    return _voyage_client


def embed_query(text: str) -> np.ndarray:
    client = _get_voyage_client()
    result = client.embed([text], model=VOYAGE_MODEL)
    vec = np.array(result.embeddings[0], dtype=np.float32)
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def _vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


# ── Chunk 檢索 ────────────────────────────────────────────────────────

CHUNK_SELECT = """
    cc.id AS chunk_id, cc.decision_id, cc.chunk_index,
    cc.citation_id, cc.target_id, cc.target_authority_id,
    cc.case_type, cc.chunk_text,
    d.root_norm, d.display_title, d.doc_type, d.decision_date,
    d.total_citation_count,
    cc.embedding <=> %s::vector AS distance
"""


def _knn(
    conn: psycopg.Connection, vec_str: str,
    *, case_type: str | None, limit: int = 50,
) -> list[dict]:
    where = ["cc.embedding IS NOT NULL"]
    params: list = [vec_str]
    if case_type:
        where.append("cc.case_type = %s")
        params.append(case_type)
    params.extend([vec_str, limit])

    return conn.execute(f"""
        SELECT {CHUNK_SELECT}
        FROM chunks cc
        JOIN decisions d ON d.id = cc.decision_id
        WHERE {" AND ".join(where)}
        ORDER BY cc.embedding <=> %s::vector
        LIMIT %s
    """, params).fetchall()


# ── 聚合 ─────────────────────────────────────────────────────────────

def _aggregate(
    knn_rows: list[dict],
    *, top: int,
) -> list[dict]:
    chunks: dict[int, dict] = {}
    for r in knn_rows:
        r = dict(r)
        key = r["chunk_id"]
        if key not in chunks:
            chunks[key] = r

    for c in chunks.values():
        c["sim"] = 1 - float(c["distance"])
        c["score"] = c["sim"]

    by_decision: dict[int, list[dict]] = defaultdict(list)
    for c in chunks.values():
        by_decision[c["decision_id"]].append(c)

    results = []
    for decision_id, dec_chunks in by_decision.items():
        best = max(dec_chunks, key=lambda x: x["score"])

        target_decision_ids = []
        target_authority_ids = []
        for c in dec_chunks:
            if c.get("target_id"):
                target_decision_ids.append(c["target_id"])
            elif c.get("target_authority_id"):
                target_authority_ids.append(c["target_authority_id"])

        results.append({
            "decision_id": decision_id,
            "root_norm": best["root_norm"],
            "display_title": best["display_title"],
            "doc_type": best["doc_type"],
            "decision_date": str(best["decision_date"]) if best.get("decision_date") else None,
            "case_type": best["case_type"],
            "score": best["score"],
            "sim": best["sim"],
            "chunk_count": len(dec_chunks),
            "best_chunk_text": best["chunk_text"],
            "target_ids": sorted(set(target_decision_ids)) if target_decision_ids else [],
            "target_authority_ids": sorted(set(target_authority_ids)) if target_authority_ids else [],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top]


# ── Main search ──────────────────────────────────────────────────────

def rag_search(
    conn: psycopg.Connection,
    query: str,
    *,
    case_type: str | None = None,
    top: int = 20,
) -> list[dict]:
    vec = embed_query(query)
    vec_str = _vec_to_pg(vec)

    knn_rows = _knn(conn, vec_str, case_type=case_type, limit=50)

    results = _aggregate(knn_rows, top=top)

    # Enrich target display info (decisions + authorities)
    all_decision_ids = set()
    all_authority_ids = set()
    for r in results:
        all_decision_ids.update(r.get("target_ids", []))
        all_authority_ids.update(r.get("target_authority_ids", []))

    decision_info: dict[int, dict] = {}
    if all_decision_ids:
        rows = conn.execute("""
            SELECT id, display_title, root_norm, total_citation_count
            FROM decisions WHERE id = ANY(%s)
        """, (list(all_decision_ids),)).fetchall()
        decision_info = {r["id"]: dict(r) | {"target_type": "decision"} for r in rows}

    auth_info: dict[int, dict] = {}
    if all_authority_ids:
        rows = conn.execute("""
            SELECT id, display AS display_title, root_norm, total_citation_count
            FROM authorities WHERE id = ANY(%s)
        """, (list(all_authority_ids),)).fetchall()
        auth_info = {
            r["id"]: {"id": r["id"], "display_title": r["display_title"],
                      "root_norm": r["root_norm"],
                      "total_citation_count": r["total_citation_count"],
                      "target_type": "authority"}
            for r in rows
        }

    for r in results:
        targets = [
            decision_info[tid] for tid in r.get("target_ids", []) if tid in decision_info
        ] + [
            auth_info[aid] for aid in r.get("target_authority_ids", []) if aid in auth_info
        ]
        r["targets"] = targets

    return results
