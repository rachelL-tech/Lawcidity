#!/usr/bin/env python3
"""
測試 RAG 雙路合併搜尋 + decision 聚合。

Usage:
  # 純向量搜尋
  python scripts/test_rag_query.py "案情描述" --top 20

  # 加 statute filter（雙路合併）
  python scripts/test_rag_query.py "案情描述" --statutes "民法:184,民法:195"

  # 指定 case_type + statute
  python scripts/test_rag_query.py "案情描述" --case-type 民事 --statutes "民法:184"
"""

import argparse
import math
import os
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

DIMS = 1024
VOYAGE_MODEL = "voyage-law-2"

_client_cache = None


def load_client():
    global _client_cache
    if _client_cache:
        return _client_cache
    try:
        import voyageai
    except ImportError:
        print("ERROR: pip install voyageai")
        sys.exit(1)
    api_key = os.environ.get("VOYAGE_API_KEY")
    if not api_key:
        print("ERROR: VOYAGE_API_KEY not set in .env")
        sys.exit(1)
    _client_cache = voyageai.Client(api_key=api_key)
    return _client_cache


def embed_query(text: str) -> np.ndarray:
    client = load_client()
    result = client.embed([text], model=VOYAGE_MODEL)
    vec = np.array(result.embeddings[0], dtype=np.float32)
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


def parse_statutes(s: str) -> list[tuple[str, str]]:
    """'民法:184,民法:195' → [('民法','184'), ('民法','195')]"""
    result = []
    for item in s.split(","):
        item = item.strip()
        if ":" in item:
            law, article = item.split(":", 1)
            result.append((law.strip(), article.strip()))
    return result


# ── Chunk 檢索 ────────────────────────────────────────────────────────

CHUNK_SELECT = """
    cc.id AS chunk_id, cc.decision_id, cc.chunk_index, cc.chunk_type,
    cc.citation_id, cc.target_id, cc.target_authority_id,
    cc.case_type, cc.chunk_text,
    d.root_norm, d.display_title, d.doc_type, d.decision_date,
    d.total_citation_count,
    cc.embedding <=> %s::vector AS distance
"""


def path_a_knn(conn, vec_str: str, *, case_type: str | None, limit: int,
               chunk_type: str | None = None) -> list[dict]:
    """Path A: 純語意 HNSW knn。"""
    where = ["cc.embedding IS NOT NULL"]
    params: list = [vec_str]
    if case_type:
        where.append("cc.case_type = %s")
        params.append(case_type)
    if chunk_type:
        where.append("cc.chunk_type = %s")
        params.append(chunk_type)
    params.extend([vec_str, limit])

    return conn.execute(f"""
        SELECT {CHUNK_SELECT}
        FROM chunks cc
        JOIN decisions d ON d.id = cc.decision_id
        WHERE {" AND ".join(where)}
        ORDER BY cc.embedding <=> %s::vector
        LIMIT %s
    """, params).fetchall()


def path_b_statutes(conn, vec_str: str, statutes: list[tuple[str, str]],
                    *, case_type: str | None) -> list[dict]:
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

def merge_and_aggregate(knn_rows: list[dict], statute_rows: list[dict],
                        statute_cit_ids: set[int], statute_decision_ids: set[int],
                        *, boost: float, authority_boost: float,
                        top: int) -> list[dict]:
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

        # Collect targets from citation chunks
        targets = []
        for c in dec_chunks:
            if c["chunk_type"] == "citation_context" and c.get("target_id"):
                targets.append(c["target_id"])

        # Authority boost (for citation chunks, use target's citation count)
        auth_score = 0
        if authority_boost > 0 and best.get("total_citation_count", 0) > 0:
            auth_score = authority_boost * math.log(1 + best["total_citation_count"])

        results.append({
            "decision_id": decision_id,
            "type": result_type,
            "root_norm": best["root_norm"],
            "display_title": best["display_title"],
            "doc_type": best["doc_type"],
            "decision_date": best.get("decision_date"),
            "case_type": best["case_type"],
            "score": best["score"] + auth_score,
            "sim": best["sim"],
            "distance": best["distance"],
            "statute_hit": any(c["statute_hit"] for c in dec_chunks),
            "chunk_count": len(dec_chunks),
            "chunk_types": sorted(chunk_types),
            "best_chunk_text": best["chunk_text"],
            "target_ids": sorted(set(targets)) if targets else [],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top]


# ── Main ──────────────────────────────────────────────────────────────

def search(query: str, *, case_type: str | None, statutes: list[tuple[str, str]],
           boost: float, authority_boost: float, top: int,
           supreme_only: bool = False):
    vec = embed_query(query)
    vec_str = vec_to_pg(vec)

    db_url = os.environ.get("DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations")

    chunk_type_filter = "supreme_reasoning" if supreme_only else None

    with psycopg.connect(db_url, row_factory=dict_row) as conn:
        # Path A: HNSW knn
        knn_rows = path_a_knn(conn, vec_str, case_type=case_type, limit=50,
                               chunk_type=chunk_type_filter)

        # Path B: statute brute-force
        statute_rows = path_b_statutes(conn, vec_str, statutes, case_type=case_type)

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

        # Fetch target info for display
        target_info: dict[int, dict] = {}
        all_target_ids = set()
        for r in list(knn_rows) + statute_rows:
            r_dict = dict(r) if not isinstance(r, dict) else r
            if r_dict.get("target_id"):
                all_target_ids.add(r_dict["target_id"])
        if all_target_ids:
            target_rows = conn.execute("""
                SELECT id, display_title, root_norm, total_citation_count
                FROM decisions WHERE id = ANY(%s)
            """, (list(all_target_ids),)).fetchall()
            target_info = {r["id"]: dict(r) for r in target_rows}

    # Merge + aggregate
    results = merge_and_aggregate(
        knn_rows, statute_rows, statute_cit_ids, statute_decision_ids,
        boost=boost, authority_boost=authority_boost, top=top,
    )

    # Enrich target display info
    for r in results:
        r["targets"] = [target_info.get(tid, {"id": tid}) for tid in r.get("target_ids", [])]

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Test RAG dual-path search with decision aggregation")
    parser.add_argument("query", help="案情描述文字")
    parser.add_argument("--case-type", help="民事|刑事|行政")
    parser.add_argument("--statutes", help="法條 filter，格式：民法:184,民法:195")
    parser.add_argument("--boost", type=float, default=0.15,
                        help="statute match boost (default: 0.15)")
    parser.add_argument("--authority-boost", type=float, default=0.05,
                        help="authority citation count boost (default: 0.05)")
    parser.add_argument("--top", type=int, default=20,
                        help="results to show (default: 20)")
    parser.add_argument("--supreme-only", action="store_true",
                        help="只召回 supreme_reasoning chunks（驗證最高法院覆蓋率）")
    args = parser.parse_args()

    statutes = parse_statutes(args.statutes) if args.statutes else []

    print(f"\nQuery: {args.query}")
    if args.case_type:
        print(f"Filter: case_type = {args.case_type}")
    if statutes:
        print(f"Statute filter: {statutes} (boost={args.boost})")
    print(f"Top: {args.top}\n")

    results = search(
        args.query, case_type=args.case_type, statutes=statutes,
        boost=args.boost, authority_boost=args.authority_boost, top=args.top,
        supreme_only=args.supreme_only,
    )

    # Header
    print(f"{'Rank':<5} {'Score':>6} {'Sim':>5} {'Type':<10} {'Stat':>4}  "
          f"{'Decision':<45}  {'Chunks':>4}")
    print("─" * 100)

    for i, r in enumerate(results, 1):
        stat_mark = "Y" if r["statute_hit"] else ""
        label = f"{r['root_norm']} {r['display_title']}"
        if len(label) > 44:
            label = label[:42] + ".."
        print(f"{i:<5} {r['score']:>6.4f} {r['sim']:>5.3f} {r['type']:<10} {stat_mark:>4}  "
              f"{label:<45}  {r['chunk_count']:>4}")

        # Show targets for citation chunks
        for t in r.get("targets", [])[:2]:
            t_label = f"{t.get('root_norm', '')} {t.get('display_title', t.get('id', ''))}"
            t_count = t.get("total_citation_count", 0)
            print(f"      📌 引用: {t_label} (被引 {t_count} 次)")

        # Show chunk preview
        preview = r["best_chunk_text"].replace("\n", " ").replace("\r", "")
        print(f"      {preview}")
        print()


if __name__ == "__main__":
    main()
