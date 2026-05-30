"""
RAG 語意搜尋 + decision 聚合。

流程：
1. Raw factual query → Voyage API embed
2. IVFFlat ANN knn → top 50 chunks（純語意召回）
3. chunk 計分（sim = 1 - cosine distance）→ 聚合到 decision → top N
"""

import hashlib
import os
from collections import defaultdict

import numpy as np
import psycopg

VOYAGE_MODEL = "voyage-law-2"
IVFFLAT_PROBES = 3  # lists=100 下 p=3 recall≈0.90（knee）；prod 1GB 延遲預算內

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


def embed_query(texts: list[str]) -> list[np.ndarray]:
    client = _get_voyage_client()
    try:
        result = client.embed(texts, model=VOYAGE_MODEL)
    except Exception as exc:
        raise RuntimeError("Voyage embedding 呼叫失敗") from exc
    # 若未來要整批重跑 documents，可再和 document 端一起導入 input_type="query" / input_type="document"。
    return [np.array(e, dtype=np.float32) for e in result.embeddings]


def _vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


# ── Chunk 檢索 ────────────────────────────────────────────────────────

CHUNK_SELECT = """
    cc.id AS chunk_id, cc.decision_id,
    cc.target_ids, cc.target_authority_ids,
    cc.chunk_text,
    d.root_norm, d.display_title, d.doc_type,
    cu.level AS court_level,
    cc.embedding <=> %s::vector AS distance
"""


def _knn(
    conn: psycopg.Connection, vec_str: str,
    *, limit: int = 50,
) -> list[dict]:
    try:
        conn.execute(f"SET ivfflat.probes = {int(IVFFLAT_PROBES)}")

        return conn.execute(f"""
            SELECT {CHUNK_SELECT}
            FROM chunks cc
            JOIN decisions d ON d.id = cc.decision_id
            LEFT JOIN court_units cu ON cu.id = d.court_unit_id
            WHERE cc.embedding IS NOT NULL
            ORDER BY cc.embedding <=> %s::vector
            LIMIT %s
        """, (vec_str, vec_str, limit)).fetchall()
    except Exception as exc:
        raise RuntimeError("pgvector KNN 查詢失敗") from exc


# ── 聚合 ─────────────────────────────────────────────────────────────

def _aggregate(
    knn_rows: list[dict],
    *, result_limit: int,
) -> list[dict]:
    for c in knn_rows:
        c["sim"] = 1 - float(c["distance"])

    # chunk-base：以 chunk_text 為單位分組（同文字必同分）
    by_text: dict[str, list[dict]] = defaultdict(list)
    for c in knn_rows:
        text_hash = hashlib.md5(c["chunk_text"].encode("utf-8")).hexdigest()
        by_text[text_hash].append(c)

    results = []
    for chunks in by_text.values():
        # primary：審級最高（court_level 最小）→ decision_id 最小。None 審級排最後。
        primary = min(chunks, key=lambda c: (
            c["court_level"] if c["court_level"] is not None else 99, c["decision_id"]))

        # other_sources：同文字的其他判決（依 decision_id 去重、排除 primary）
        seen = {primary["decision_id"]}
        other_sources = []
        for c in chunks:
            if c["decision_id"] in seen:
                continue
            seen.add(c["decision_id"])
            other_sources.append({
                "decision_id": c["decision_id"],
                "root_norm": c["root_norm"],
                "display_title": c["display_title"],
            })

        results.append({
            "decision_id": primary["decision_id"],
            "root_norm": primary["root_norm"],
            "display_title": primary["display_title"],
            "doc_type": primary["doc_type"],
            "sim": primary["sim"],
            "best_chunk_text": primary["chunk_text"],
            "target_ids": sorted(set(primary.get("target_ids") or [])),
            "target_authority_ids": sorted(set(primary.get("target_authority_ids") or [])),
            "other_sources": other_sources,
        })

    results.sort(key=lambda x: x["sim"], reverse=True)
    return results[:result_limit]


# ── Main search ──────────────────────────────────────────────────────

def rag_search_fanout(
    conn,
    issues: list[str],
    *,
    result_limit_per_issue: int,
):
    vec = embed_query(issues)
    vec_str = [_vec_to_pg(v) for v in vec]

    results = {}
    for i, value in enumerate(vec_str):
        raw_chunks = _knn(conn, value, limit=result_limit_per_issue * 5)

        results_per_issue = _aggregate(raw_chunks, result_limit=result_limit_per_issue)

        results[issues[i]] = results_per_issue

    all_decision_ids: set[int] = set()
    all_authority_ids: set[int] = set()
    for items in results.values():
        for item in items:
            for target_id in (item.get("target_ids") or []):
                all_decision_ids.add(target_id)
            for authority_id in (item.get("target_authority_ids") or []):
                all_authority_ids.add(authority_id)

    decision_info: dict[int, dict] = {}
    auth_info: dict[int, dict] = {}
    try:
        if all_decision_ids:
            rows = conn.execute("""
                SELECT id, display_title, root_norm, total_citation_count
                FROM decisions WHERE id = ANY(%s)
            """, (list(all_decision_ids),)).fetchall()
            decision_info = {r["id"]: r for r in rows}

        if all_authority_ids:
            rows = conn.execute("""
                SELECT id, display AS display_title, root_norm, total_citation_count
            FROM authorities WHERE id = ANY(%s)
            """, (list(all_authority_ids),)).fetchall()
            auth_info = {r["id"]: r for r in rows}
    except Exception as exc:
        raise RuntimeError("RAG target metadata 查詢失敗") from exc

    for items in results.values():
        for item in items:
            targets = [
                decision_info[tid] for tid in item.get("target_ids", []) if tid in decision_info
            ] + [
                auth_info[aid] for aid in item.get("target_authority_ids", []) if aid in auth_info
            ]
            item["targets"] = targets

    return results
