#!/usr/bin/env python3
"""
Prod recall 驗證（只讀）：量目前 lists=100 / probes=3 的 recall@50。

不動索引、不 DROP、不 CREATE——可安全對 prod 跑。
用本地快取 embedding（scripts/.diagnose_cache.pkl）繞過 Voyage API。

用法：
  DATABASE_URL=<prod_url> python scripts/prod_recall_check.py
"""
import os
import pickle
import sys

import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from app.db import get_conn
from app.rag_service import _vec_to_pg, IVFFLAT_PROBES

CACHE_PATH = "scripts/.diagnose_cache.pkl"
TOPK = 50

cache = pickle.loads(open(CACHE_PATH, "rb").read())
QUERY_VECS = [(_vec_to_pg(np.array(v, dtype=np.float32)), t[:50])
              for t, v in cache["embed"].items()]


def topk_ids(conn, vec_str, *, exact: bool) -> set:
    if exact:
        conn.execute("SET enable_seqscan = on")
        conn.execute("SET enable_indexscan = off")
        conn.execute("SET enable_bitmapscan = off")
    else:
        conn.execute("SET enable_seqscan = off")
        conn.execute("SET enable_indexscan = on")
        conn.execute("SET enable_bitmapscan = on")
    rows = conn.execute(
        "SELECT id FROM chunks WHERE embedding IS NOT NULL "
        "ORDER BY embedding <=> %s::vector LIMIT %s",
        (vec_str, TOPK),
    ).fetchall()
    return {r["id"] for r in rows}


def main():
    print(f"query 數：{len(QUERY_VECS)}，TOPK={TOPK}，probes={IVFFLAT_PROBES}")
    print("注意：精確 top-50 用 seq scan，prod 1GB RAM 下每筆可能較慢。\n")

    with get_conn() as conn:
        conn.execute(f"SET ivfflat.probes = {int(IVFFLAT_PROBES)}")

        print("計算精確 top-50（seq scan baseline）...")
        exact = []
        for vec_str, label in QUERY_VECS:
            exact.append(topk_ids(conn, vec_str, exact=True))
        print("完成。\n")

        print("計算 ANN top-50（ivfflat）...")
        total_overlap = 0
        for (vec_str, label), ex in zip(QUERY_VECS, exact):
            ann = topk_ids(conn, vec_str, exact=False)
            overlap = len(ann & ex)
            total_overlap += overlap
            print(f"  [{label}] recall={overlap}/{TOPK} = {overlap/TOPK:.3f}")

        recall = total_overlap / (len(QUERY_VECS) * TOPK)
        print(f"\nrecall@{TOPK}（平均）= {recall:.3f}")
        print(f"參考值：本地 lists=100 / probes=3 = 0.896")


if __name__ == "__main__":
    main()
