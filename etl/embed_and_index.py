#!/usr/bin/env python3
"""
讀 citation_chunks → voyage-law-2 @ 1024 dims → UPDATE embedding 到 PostgreSQL。

優化：
  (2) batch size 預設 128
  (4) text dedup：相同 chunk_text 只 embed 一次，UPDATE 套用所有相同 row
      - 以 md5(chunk_text) 為 dedup key 與 checkpoint key

注意：DB schema 須為 vector(1024)，執行前請先跑 sql/003_voyage_migration.sql。

Usage:
  python etl/embed_and_index.py                      # 全量（跳過已有 embedding）
  python etl/embed_and_index.py --reset              # 清空所有 embedding 後重跑
  python etl/embed_and_index.py --limit 5000          # 只處理前 N 個 unique text
  python etl/embed_and_index.py --case-type 刑事      # 只處理特定 case_type
  python etl/embed_and_index.py --month 2024-01       # 只處理特定月份的判決
  python etl/embed_and_index.py --embed-batch 128     # embedding batch size（default: 128）
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import numpy as np
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

DIMS = 1024
VOYAGE_MODEL = "voyage-law-2"
CHECKPOINT_FILE = Path("scripts/embed_and_index_checkpoint.json")


# ── DB ─────────────────────────────────────────────────────────────────────

def get_db_conn():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(db_url, row_factory=dict_row)


def fetch_unique_texts(conn, *, case_type=None, month=None, after_hash=None, limit=None):
    """
    以 md5(chunk_text) 去重：相同文字只回傳一次。
    after_hash = md5 hex string — checkpoint 續跑用。
    month = 'YYYY-MM' — 只處理該月份判決的 chunks。
    """
    where = ["cc.embedding IS NULL"]
    params: dict = {}
    join_sql = ""

    if case_type:
        where.append("cc.case_type = %(case_type)s")
        params["case_type"] = case_type

    if month:
        try:
            year, mon = int(month[:4]), int(month[5:7])
        except (ValueError, IndexError):
            raise ValueError(f"--month 格式錯誤，應為 YYYY-MM，收到：{month!r}")
        import datetime
        date_from = datetime.date(year, mon, 1)
        # 下個月第一天
        date_to = datetime.date(year + mon // 12, mon % 12 + 1, 1)
        join_sql = "JOIN decisions d ON d.id = cc.decision_id"
        where.append("d.decision_date >= %(date_from)s AND d.decision_date < %(date_to)s")
        params["date_from"] = date_from
        params["date_to"] = date_to

    if after_hash:
        where.append("md5(cc.chunk_text) > %(after_hash)s")
        params["after_hash"] = after_hash

    where_sql = "WHERE " + " AND ".join(where)
    limit_sql = f"LIMIT {limit}" if limit else ""

    sql = f"""
        SELECT
            MIN(cc.chunk_text)        AS chunk_text,
            md5(MIN(cc.chunk_text))   AS text_hash
        FROM citation_chunks cc
        {join_sql}
        {where_sql}
        GROUP BY md5(cc.chunk_text)
        ORDER BY text_hash
        {limit_sql}
    """
    return conn.execute(sql, params)


# ── Checkpoint ─────────────────────────────────────────────────────────────

def load_checkpoint() -> str | None:
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text())
        return data.get("text_hash")
    return None


def save_checkpoint(text_hash: str):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(json.dumps({"text_hash": text_hash}))


def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


# ── Embedding ──────────────────────────────────────────────────────────────

def load_voyage_client():
    try:
        import voyageai
    except ImportError:
        print("ERROR: pip install voyageai")
        sys.exit(1)

    api_key = os.environ.get("VOYAGE_API_KEY")
    if not api_key:
        print("ERROR: VOYAGE_API_KEY not set in .env")
        sys.exit(1)

    return voyageai.Client(api_key=api_key)


def embed_batch(client, texts: list[str]) -> np.ndarray:
    result = client.embed(texts, model=VOYAGE_MODEL)
    embeds = np.array(result.embeddings, dtype=np.float32)
    norms = np.linalg.norm(embeds, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return embeds / norms


def vec_to_pg(vec: np.ndarray) -> str:
    """numpy vector → pgvector 文字格式 '[x1,x2,...]'"""
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=f"Embed citation_chunks via {VOYAGE_MODEL} and store to PostgreSQL pgvector"
    )
    parser.add_argument("--reset",       action="store_true",
                        help="清空所有 embedding 後重跑（會忽略 checkpoint）")
    parser.add_argument("--limit",       type=int,
                        help="最多處理 N 個 unique text")
    parser.add_argument("--case-type",   type=str,
                        help="只處理特定 case_type")
    parser.add_argument("--month",       type=str,
                        help="只處理特定月份判決的 chunks，格式 YYYY-MM")
    parser.add_argument("--embed-batch", type=int, default=64,
                        help="embedding batch size (default: 64)")
    args = parser.parse_args()

    conn = get_db_conn()

    if args.reset:
        print("Resetting all embeddings (UPDATE embedding = NULL)...")
        conn.execute("UPDATE citation_chunks SET embedding = NULL")
        conn.commit()
        clear_checkpoint()
        print("Done.")

    after_hash = load_checkpoint()
    if after_hash:
        print(f"Resuming from checkpoint: text_hash > {after_hash[:8]}...")

    count_join = ""
    count_where = "WHERE cc.embedding IS NULL"
    count_params: dict = {}
    if args.case_type:
        count_where += " AND cc.case_type = %(case_type)s"
        count_params["case_type"] = args.case_type
    if args.month:
        import datetime
        year, mon = int(args.month[:4]), int(args.month[5:7])
        count_join = "JOIN decisions d ON d.id = cc.decision_id"
        count_where += " AND d.decision_date >= %(date_from)s AND d.decision_date < %(date_to)s"
        count_params["date_from"] = datetime.date(year, mon, 1)
        count_params["date_to"] = datetime.date(year + mon // 12, mon % 12 + 1, 1)
    row = conn.execute(
        f"SELECT COUNT(DISTINCT md5(cc.chunk_text)) AS cnt FROM citation_chunks cc {count_join} {count_where}",
        count_params
    ).fetchone()
    total_pending = row["cnt"] if row else 0
    print(f"Pending unique texts: {total_pending}"
          + (f" (limit: {args.limit})" if args.limit else ""))
    print(f"Model: {VOYAGE_MODEL} @ {DIMS} dims")

    client = load_voyage_client()

    cursor = fetch_unique_texts(
        conn, case_type=args.case_type, month=args.month,
        after_hash=after_hash, limit=args.limit
    )

    t0 = time.time()
    total_embedded = 0
    errors = 0
    embed_buf: list[dict] = []

    def flush(buf: list[dict]):
        nonlocal total_embedded, errors
        if not buf:
            return

        texts = [r["chunk_text"] for r in buf]
        try:
            vectors = embed_batch(client, texts)
        except Exception as e:
            errors += len(buf)
            print(f"  ERROR: embed failed — {e}", file=sys.stderr)
            return

        params = [
            (vec_to_pg(vec), row["chunk_text"])
            for row, vec in zip(buf, vectors)
        ]
        try:
            with conn.cursor() as cur:
                cur.executemany(
                    "UPDATE citation_chunks SET embedding = %s::vector "
                    "WHERE chunk_text = %s AND embedding IS NULL",
                    params,
                )
            conn.commit()
            total_embedded += len(buf)
        except Exception as e:
            conn.rollback()
            errors += len(buf)
            print(f"  ERROR: update failed — {e}", file=sys.stderr)
            return

        save_checkpoint(buf[-1]["text_hash"])

    for row in cursor:
        embed_buf.append(dict(row))

        if len(embed_buf) >= args.embed_batch:
            flush(embed_buf)
            embed_buf.clear()

            elapsed = time.time() - t0
            rate = total_embedded / elapsed if elapsed > 0 else 0
            pct = total_embedded / total_pending * 100 if total_pending else 0
            eta = (total_pending - total_embedded) / rate if rate > 0 else 0
            print(f"  embedded={total_embedded}/{total_pending} ({pct:.1f}%), "
                  f"errors={errors}, {rate:.1f} texts/s, ETA {eta:.0f}s")

    flush(embed_buf)
    embed_buf.clear()

    conn.close()
    elapsed = time.time() - t0

    print(f"\n--- 完成 ---")
    print(f"Unique texts embedded: {total_embedded}, Errors: {errors}")
    if elapsed > 0:
        print(f"Time: {elapsed:.1f}s ({total_embedded/elapsed:.1f} texts/s)")
    if not errors:
        clear_checkpoint()
        print("\n下一步：執行 HNSW index（見 sql/003_voyage_migration.sql）")


if __name__ == "__main__":
    main()
