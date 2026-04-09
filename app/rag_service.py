"""
RAG 雙路合併搜尋 + decision 聚合。

流程：
1. Raw factual query → Voyage API embed
2. Path A: IVFFlat ANN knn → top 50 chunks（純語意召回，有排序+LIMIT）
3. Path B: 法條搜尋 chunk.id → sequential scan 計算每個向量距離後，無排序、全數回傳
4. 合併 A ∪ B → chunk_id 去重 → chunk 計分（sim + statute boost）→ 聚合到 decision → top N
"""

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

    # B1: shrink statute matches to citation ids first, then probe chunks by citation_id
    b1_rows = conn.execute(f"""
        WITH statute_citations AS (
            SELECT DISTINCT citation_id
            FROM citation_snippet_statutes
            WHERE (law, article_raw) IN ({values_sql})
        )
        SELECT {CHUNK_SELECT}
        FROM chunks cc
        JOIN statute_citations sc ON sc.citation_id = cc.citation_id
        JOIN decisions d ON d.id = cc.decision_id
        WHERE cc.embedding IS NOT NULL
          AND cc.chunk_type = 'citation_context'
          {ct_where}
    """, stat_params + [vec_str] + ct_params).fetchall()

    # B2: probe the smaller supreme chunk universe, then validate statute hits by decision_id
    b2_rows = conn.execute(f"""
        SELECT {CHUNK_SELECT}
        FROM chunks cc
        JOIN decisions d ON d.id = cc.decision_id
        WHERE cc.embedding IS NOT NULL
          AND cc.chunk_type = 'supreme_reasoning'
          {ct_where}
          AND EXISTS (
              SELECT 1
              FROM decision_reason_statutes drs
              WHERE drs.decision_id = cc.decision_id
                AND (drs.law, drs.article_raw) IN ({values_sql})
          )
    """, [vec_str] + ct_params + stat_params).fetchall()

    return [dict(r) for r in b1_rows] + [dict(r) for r in b2_rows]


# ── 合併 + 聚合 ──────────────────────────────────────────────────────

def _merge_and_aggregate(
    knn_rows: list[dict], statute_rows: list[dict],
    statute_cit_ids: set[int], statute_decision_ids: set[int],
    *, boost: float, top: int,
) -> list[dict]:
    """合併雙路結果 → chunk 計分 → 聚合到 decision。"""
    # Dedup chunks by chunk_id（PK）
    # 同一 chunk 被 Path A / Path B 雙路撈到時合併 flag；不同 citation 的 chunk 各自保留
    chunks: dict[int, dict] = {}
    for r in knn_rows:
        r = dict(r)
        key = r["chunk_id"]
        if key not in chunks:
            r["from_knn"] = True
            r["from_statute"] = False
            chunks[key] = r
        else:
            chunks[key]["from_knn"] = True

    for r in statute_rows:
        r = dict(r) if not isinstance(r, dict) else r
        key = r["chunk_id"]
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

        target_decision_ids = []
        target_authority_ids = []
        for c in dec_chunks:
            if c["chunk_type"] == "citation_context":
                if c.get("target_id"):
                    target_decision_ids.append(c["target_id"])
                elif c.get("target_authority_id"):
                    target_authority_ids.append(c["target_authority_id"])

        results.append({
            "decision_id": decision_id,
            "type": result_type,
            "root_norm": best["root_norm"],
            "display_title": best["display_title"],
            "doc_type": best["doc_type"],
            "decision_date": str(best["decision_date"]) if best.get("decision_date") else None,
            "case_type": best["case_type"],
            "score": best["score"],
            "sim": best["sim"],
            "statute_hit": any(c["statute_hit"] for c in dec_chunks),
            "chunk_count": len(dec_chunks),
            "chunk_types": sorted(chunk_types),
            "best_chunk_text": best["chunk_text"],
            "best_chunk_type": best["chunk_type"],   # "citation_context" | "supreme_reasoning"
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
    issues: list[str] | None = None,
    case_type: str | None = None,
    statutes: list[tuple[str, str]] | None = None,
    boost: float = 0.15,
    top: int = 20,
) -> list[dict]:
    """
    RAG 雙路合併搜尋。

    Args:
        conn: PostgreSQL connection
        query: 案情描述（issues 為空時 fallback）
        issues: 爭點列表（優先用於 embedding 檢索，語意與 chunks 更接近）
        case_type: 民事/刑事/行政
        statutes: [(law, article), ...] e.g. [("民法", "184"), ("民法", "195")]
        boost: statute match boost
        top: 回傳筆數

    Returns:
        Decision-level results with score, chunk_text, targets
    """
    statutes = statutes or []

    # Main representation uses the user's raw factual query.
    # Issues remain available for later auxiliary retrieval or reranking work,
    # but should not replace fact-pattern retrieval at the main query stage.
    search_text = query
    vec = embed_query(search_text)
    vec_str = _vec_to_pg(vec)

    # Path A: HNSW knn
    knn_rows = _path_a_knn(conn, vec_str, case_type=case_type, limit=50)

    # Path B: statute brute-force
    statute_rows = _path_b_statutes(conn, vec_str, statutes, case_type=case_type)

    # 從 Path B 結果直接推導，不另打 SQL
    statute_cit_ids: set[int] = {
        r["citation_id"] for r in statute_rows
        if r.get("chunk_type") == "citation_context" and r.get("citation_id")
    }
    statute_decision_ids: set[int] = {
        r["decision_id"] for r in statute_rows
        if r.get("chunk_type") == "supreme_reasoning"
    }

    # Merge + aggregate
    results = _merge_and_aggregate(
        knn_rows, statute_rows, statute_cit_ids, statute_decision_ids,
        boost=boost, top=top,
    )

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
