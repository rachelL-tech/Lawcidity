"""
RAG 雙路合併搜尋 + decision 聚合。

流程：
1. Query → Voyage API embed
2. Path A: HNSW knn → top 50 chunks
3. Path B: statute filter → brute-force 向量排序
4. 合併 A ∪ B → 去重 → chunk 計分 → 聚合到 decision
"""

import math
import os
from collections import defaultdict

import numpy as np
import psycopg
from psycopg.rows import dict_row

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
    cc.id AS chunk_id, cc.decision_id, cc.chunk_index, cc.chunk_type,
    cc.citation_id, cc.target_id, cc.target_authority_id,
    cc.case_type, cc.chunk_text,
    d.root_norm, d.display_title, d.doc_type, d.decision_date,
    d.total_citation_count,
    cc.embedding <=> %s::vector AS distance
"""


def _path_a_knn(
    conn: psycopg.Connection, vec_str: str,
    *, case_type: str | None, limit: int = 50,
) -> list[dict]:
    """Path A: 純語意 HNSW knn。"""
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


def _path_b_statutes(
    conn: psycopg.Connection, vec_str: str,
    statutes: list[tuple[str, str]],
    *, case_type: str | None,
) -> list[dict]:
    """Path B: 法條命中 → brute-force 向量排序。"""
    if not statutes:
        return []

    values_sql = ",".join(["(%s,%s)"] * len(statutes))
    stat_params: list = []
    for law, article in statutes:
        stat_params.extend([law, article])

    ct_where = "AND cc.case_type = %s" if case_type else ""
    ct_params = [case_type] if case_type else []

    # B1: citation chunks via citation_snippet_statutes
    b1_rows = conn.execute(f"""
        SELECT {CHUNK_SELECT}
        FROM chunks cc
        JOIN decisions d ON d.id = cc.decision_id
        WHERE cc.embedding IS NOT NULL
          AND cc.chunk_type = 'citation_context'
          AND cc.citation_id IN (
              SELECT DISTINCT citation_id
              FROM citation_snippet_statutes
              WHERE (law, article_raw) IN ({values_sql})
          )
          {ct_where}
    """, [vec_str] + stat_params + ct_params).fetchall()

    # B2: supreme chunks via decision_reason_statutes
    b2_rows = conn.execute(f"""
        SELECT {CHUNK_SELECT}
        FROM chunks cc
        JOIN decisions d ON d.id = cc.decision_id
        WHERE cc.embedding IS NOT NULL
          AND cc.chunk_type = 'supreme_reasoning'
          AND cc.decision_id IN (
              SELECT DISTINCT decision_id
              FROM decision_reason_statutes
              WHERE (law, article_raw) IN ({values_sql})
          )
          {ct_where}
    """, [vec_str] + stat_params + ct_params).fetchall()

    return [dict(r) for r in b1_rows] + [dict(r) for r in b2_rows]


# ── 合併 + 聚合 ──────────────────────────────────────────────────────

def _merge_and_aggregate(
    knn_rows: list[dict], statute_rows: list[dict],
    statute_cit_ids: set[int], statute_decision_ids: set[int],
    *, boost: float, authority_boost: float, top: int,
) -> list[dict]:
    """合併雙路結果 → chunk 計分 → 聚合到 decision。"""
    # Dedup chunks by (decision_id, chunk_index)
    chunks: dict[tuple, dict] = {}
    for r in knn_rows:
        r = dict(r)
        key = (r["decision_id"], r["chunk_index"])
        if key not in chunks:
            r["from_knn"] = True
            r["from_statute"] = False
            chunks[key] = r
        else:
            chunks[key]["from_knn"] = True

    for r in statute_rows:
        r = dict(r) if not isinstance(r, dict) else r
        key = (r["decision_id"], r["chunk_index"])
        if key not in chunks:
            r["from_knn"] = False
            r["from_statute"] = True
            chunks[key] = r
        else:
            chunks[key]["from_statute"] = True

    # Score each chunk
    for c in chunks.values():
        sim = 1 - float(c["distance"])
        stat_hit = False
        if c["chunk_type"] == "citation_context" and c.get("citation_id") in statute_cit_ids:
            stat_hit = True
        elif c["chunk_type"] == "supreme_reasoning" and c["decision_id"] in statute_decision_ids:
            stat_hit = True
        elif c.get("from_statute"):
            stat_hit = True

        c["sim"] = sim
        c["statute_hit"] = stat_hit
        c["score"] = sim + (boost if stat_hit else 0)

    # Aggregate to decision
    by_decision: dict[int, list[dict]] = defaultdict(list)
    for c in chunks.values():
        by_decision[c["decision_id"]].append(c)

    results = []
    for decision_id, dec_chunks in by_decision.items():
        best = max(dec_chunks, key=lambda x: x["score"])
        chunk_types = set(c["chunk_type"] for c in dec_chunks)

        if "supreme_reasoning" in chunk_types and "citation_context" in chunk_types:
            result_type = "supreme+citation"
        elif "supreme_reasoning" in chunk_types:
            result_type = "supreme"
        else:
            result_type = "citation"

        targets = []
        for c in dec_chunks:
            if c["chunk_type"] == "citation_context" and c.get("target_id"):
                targets.append(c["target_id"])

        auth_score = 0
        if authority_boost > 0 and best.get("total_citation_count", 0) > 0:
            auth_score = authority_boost * math.log(1 + best["total_citation_count"])

        results.append({
            "decision_id": decision_id,
            "type": result_type,
            "root_norm": best["root_norm"],
            "display_title": best["display_title"],
            "doc_type": best["doc_type"],
            "decision_date": str(best["decision_date"]) if best.get("decision_date") else None,
            "case_type": best["case_type"],
            "score": best["score"] + auth_score,
            "sim": best["sim"],
            "statute_hit": any(c["statute_hit"] for c in dec_chunks),
            "chunk_count": len(dec_chunks),
            "chunk_types": sorted(chunk_types),
            "best_chunk_text": best["chunk_text"],
            "target_ids": sorted(set(targets)) if targets else [],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top]


# ── Main search ──────────────────────────────────────────────────────

def rag_search(
    conn: psycopg.Connection,
    query: str,
    *,
    case_type: str | None = None,
    statutes: list[tuple[str, str]] | None = None,
    boost: float = 0.15,
    authority_boost: float = 0.05,
    top: int = 20,
) -> list[dict]:
    """
    RAG 雙路合併搜尋。

    Args:
        conn: PostgreSQL connection
        query: 案情描述
        case_type: 民事/刑事/行政
        statutes: [(law, article), ...] e.g. [("民法", "184"), ("民法", "195")]
        boost: statute match boost
        authority_boost: authority citation count boost
        top: 回傳筆數

    Returns:
        Decision-level results with score, chunk_text, targets
    """
    statutes = statutes or []

    vec = embed_query(query)
    vec_str = _vec_to_pg(vec)

    # Path A: HNSW knn
    knn_rows = _path_a_knn(conn, vec_str, case_type=case_type, limit=50)

    # Path B: statute brute-force
    statute_rows = _path_b_statutes(conn, vec_str, statutes, case_type=case_type)

    # 預查 statute hit 的 citation_ids 和 decision_ids
    statute_cit_ids: set[int] = set()
    statute_decision_ids: set[int] = set()
    if statutes:
        values_sql = ",".join(["(%s,%s)"] * len(statutes))
        stat_params: list = []
        for law, article in statutes:
            stat_params.extend([law, article])

        cit_rows = conn.execute(f"""
            SELECT DISTINCT citation_id
            FROM citation_snippet_statutes
            WHERE (law, article_raw) IN ({values_sql})
        """, stat_params).fetchall()
        statute_cit_ids = {r["citation_id"] for r in cit_rows}

        dec_rows = conn.execute(f"""
            SELECT DISTINCT decision_id
            FROM decision_reason_statutes
            WHERE (law, article_raw) IN ({values_sql})
        """, stat_params).fetchall()
        statute_decision_ids = {r["decision_id"] for r in dec_rows}

    # Merge + aggregate
    results = _merge_and_aggregate(
        knn_rows, statute_rows, statute_cit_ids, statute_decision_ids,
        boost=boost, authority_boost=authority_boost, top=top,
    )

    # Enrich target display info
    all_target_ids = set()
    for r in results:
        all_target_ids.update(r.get("target_ids", []))

    target_info: dict[int, dict] = {}
    if all_target_ids:
        target_rows = conn.execute("""
            SELECT id, display_title, root_norm, total_citation_count
            FROM decisions WHERE id = ANY(%s)
        """, (list(all_target_ids),)).fetchall()
        target_info = {r["id"]: dict(r) for r in target_rows}

    for r in results:
        r["targets"] = [
            target_info.get(tid, {"id": tid, "display_title": str(tid), "root_norm": "", "total_citation_count": 0})
            for tid in r.get("target_ids", [])
        ]

    return results
