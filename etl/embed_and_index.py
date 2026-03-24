#!/usr/bin/env python3
"""
讀 citation_chunks → Qwen3-Embedding-0.6B @ 512 dims → UPDATE embedding 到 PostgreSQL。

優化：
  (2) batch size 預設 128（原 32）
  (4) text dedup：相同 chunk_text 只 embed 一次，UPDATE 套用所有相同 row
      - 以 md5(chunk_text) 為 dedup key 與 checkpoint key

Usage:
  python etl/embed_and_index.py                      # 全量（跳過已有 embedding）
  python etl/embed_and_index.py --reset              # 清空所有 embedding 後重跑
  python etl/embed_and_index.py --limit 5000          # 只處理前 N 個 unique text
  python etl/embed_and_index.py --case-type 刑事      # 只處理特定 case_type
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

DIMS = 512
CHECKPOINT_FILE = Path("scripts/embed_and_index_checkpoint.json")
MLX_MODEL = "mlx-community/Qwen3-Embedding-0.6B-8bit"


# ── DB ─────────────────────────────────────────────────────────────────────

def get_db_conn():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(db_url, row_factory=dict_row)


def fetch_unique_texts(conn, *, case_type=None, after_hash=None, limit=None):
    """
    以 md5(chunk_text) 去重：相同文字只回傳一次。
    after_hash = md5 hex string — checkpoint 續跑用。
    """
    where = ["embedding IS NULL"]
    params: dict = {}

    if case_type:
        where.append("case_type = %(case_type)s")
        params["case_type"] = case_type

    if after_hash:
        where.append("md5(chunk_text) > %(after_hash)s")
        params["after_hash"] = after_hash

    where_sql = "WHERE " + " AND ".join(where)
    limit_sql = f"LIMIT {limit}" if limit else ""

    sql = f"""
        SELECT
            MIN(chunk_text)        AS chunk_text,
            md5(MIN(chunk_text))   AS text_hash
        FROM citation_chunks
        {where_sql}
        GROUP BY md5(chunk_text)
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

def load_model():
    try:
        from mlx_embeddings.utils import load as mlx_load
    except ImportError:
        print("ERROR: pip install mlx-embeddings")
        sys.exit(1)
    print(f"Loading {MLX_MODEL} (truncate_dim={DIMS})...")
    model, tokenizer = mlx_load(MLX_MODEL)
    return model, tokenizer


def embed_batch(model_pair, texts: list[str]) -> np.ndarray:
    import mlx.core as mx
    model, tokenizer = model_pair
    encoded = [tokenizer.encode(t, max_length=512, truncation=True) for t in texts]
    max_len = max(len(e) for e in encoded)
    pad_id = tokenizer.pad_token_id or 0
    padded = [e + [pad_id] * (max_len - len(e)) for e in encoded]
    mask = [[1] * len(e) + [0] * (max_len - len(e)) for e in encoded]
    out = model(mx.array(padded), attention_mask=mx.array(mask))
    embeds = np.array(out.text_embeds)[:, :DIMS]
    norms = np.linalg.norm(embeds, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return embeds / norms


def vec_to_pg(vec: np.ndarray) -> str:
    """numpy vector → pgvector 文字格式 '[x1,x2,...]'"""
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Embed citation_chunks and store to PostgreSQL pgvector"
    )
    parser.add_argument("--reset",       action="store_true",
                        help="清空所有 embedding 後重跑（會忽略 checkpoint）")
    parser.add_argument("--limit",       type=int,
                        help="最多處理 N 個 unique text")
    parser.add_argument("--case-type",   type=str,
                        help="只處理特定 case_type")
    parser.add_argument("--embed-batch", type=int, default=128,
                        help="embedding batch size (default: 128)")
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

    count_where = "WHERE embedding IS NULL"
    count_params: dict = {}
    if args.case_type:
        count_where += " AND case_type = %(case_type)s"
        count_params["case_type"] = args.case_type
    row = conn.execute(
        f"SELECT COUNT(DISTINCT md5(chunk_text)) AS cnt FROM citation_chunks {count_where}",
        count_params
    ).fetchone()
    total_pending = row["cnt"] if row else 0
    print(f"Pending unique texts: {total_pending}"
          + (f" (limit: {args.limit})" if args.limit else ""))

    model = load_model()

    cursor = fetch_unique_texts(
        conn, case_type=args.case_type, after_hash=after_hash, limit=args.limit
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
            vectors = embed_batch(model, texts)
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
        print("\n下一步：執行 HNSW index（見 sql/002_pgvector_migration.sql 步驟 4）")


if __name__ == "__main__":
    main()
