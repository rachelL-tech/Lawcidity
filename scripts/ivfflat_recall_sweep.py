#!/usr/bin/env python3
"""
IVFFlat recall harness：sweep lists × probes，量 ANN top-50 vs 精確 top-50 的 overlap。

原則：
  * recall 與 RAM 無關（只看 lists/probes/資料）→ 本地量得準，可代表 prod。
  * latency 與 RAM 有關 → 本地不可代表 prod；延遲預算用 prod 既有數據（probes ≤ 10）。

用快取 embedding（scripts/.diagnose_cache.pkl）當測試 query，繞過 Voyage。
會 DROP 本地 HNSW、輪流建各 lists 的 IVFFlat（建完留最後一個，最終值另行 pin 重建）。

用法：python scripts/ivfflat_recall_sweep.py
"""
import os
import pickle
import sys

import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from app.db import get_conn
from app.rag_service import _vec_to_pg

CACHE = pickle.loads(open("scripts/.diagnose_cache.pkl", "rb").read())
LISTS = [100, 400]
PROBES = [1, 3, 5, 8, 10]
TOPK = 50

# 測試 query：用快取裡所有 embedding 當 query 向量（口語 query + 爭點 + 串接 + 改寫，夠多元）
QUERY_VECS = [(_vec_to_pg(np.array(v, dtype=np.float32)), t[:20]) for t, v in CACHE["embed"].items()]


def topk_ids(conn, vec_str, *, exact):
    if exact:
        conn.execute("SET enable_seqscan = on")
        conn.execute("SET enable_indexscan = off")
        conn.execute("SET enable_bitmapscan = off")
    else:
        # 強制走 ivfflat 索引（量索引 recall，不靠 planner 成本估計）
        conn.execute("SET enable_seqscan = off")
        conn.execute("SET enable_indexscan = on")
        conn.execute("SET enable_bitmapscan = on")
    # 不加 ", id"：次要排序鍵會讓 ivfflat 索引用不上（只能給距離排序）
    rows = conn.execute(
        "SELECT id FROM chunks WHERE embedding IS NOT NULL "
        "ORDER BY embedding <=> %s::vector LIMIT %s",
        (vec_str, TOPK),
    ).fetchall()
    return {r["id"] for r in rows}


def main():
    with get_conn() as conn:
        conn.execute("SET max_parallel_maintenance_workers = 0")
        conn.execute("SET maintenance_work_mem = '512MB'")

        print(f"測試 query 數：{len(QUERY_VECS)}；TOPK={TOPK}")
        print("計算精確 top-50（baseline）...")
        exact = []
        for vec_str, _ in QUERY_VECS:
            exact.append(topk_ids(conn, vec_str, exact=True))

        print("移除 HNSW...")
        conn.execute("DROP INDEX IF EXISTS cc_embedding_hnsw")
        conn.commit()

        print(f"\n{'lists':>6} | " + " | ".join(f"p={p:<2}" for p in PROBES))
        print("-" * 46)
        for lists in LISTS:
            conn.execute("DROP INDEX IF EXISTS cc_embedding_ivfflat")
            conn.execute(
                f"CREATE INDEX cc_embedding_ivfflat ON chunks "
                f"USING ivfflat (embedding vector_cosine_ops) WITH (lists = {lists})"
            )
            conn.execute("ANALYZE chunks")
            conn.commit()

            recalls = []
            for p in PROBES:
                conn.execute(f"SET ivfflat.probes = {p}")
                total = 0
                for (vec_str, _), ex in zip(QUERY_VECS, exact):
                    ann = topk_ids(conn, vec_str, exact=False)
                    total += len(ann & ex)
                recalls.append(total / (len(QUERY_VECS) * TOPK))
            print(f"{lists:>6} | " + " | ".join(f"{r:.3f}" for r in recalls))

        print("\n判讀：在 prod 延遲預算內（probes ≤ 10），挑「達標 recall 的最低 (lists, probes)」。")
        print("註：DB 目前留著最後一個 lists 的 ivfflat；選定後另行 pin 重建。")


if __name__ == "__main__":
    main()
