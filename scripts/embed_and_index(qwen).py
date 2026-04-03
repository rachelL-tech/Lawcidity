#!/usr/bin/env python3
"""
舊版保留腳本：讀 citation_chunks → Qwen3-Embedding-0.6B @ 512 dims
→ bulk index 到 OpenSearch。

用途：
  這支腳本用來保留先前的「Qwen embedding + OpenSearch knn index」流程，
  方便對照或在仍需 OpenSearch 的環境中重建舊版索引。

關係：
  目前正式使用的 PostgreSQL pgvector 流程在 scripts/embed_and_index.py。
  這支檔案不會更新 PostgreSQL embedding 欄位，而是直接寫入 OpenSearch
  index citation_chunks_v1。

每個 unique chunk (decision_id, chunk_index) 對應一筆 OS document。
_id = "{decision_id}_{chunk_index}"

Usage:
  python 'scripts/embed_and_index(qwen).py'           # 全量
  python 'scripts/embed_and_index(qwen).py' --resume  # 從上次中斷點繼續
  python 'scripts/embed_and_index(qwen).py' --limit 5000
  python 'scripts/embed_and_index(qwen).py' --case-type 刑事
  python 'scripts/embed_and_index(qwen).py' --recreate-index
  python 'scripts/embed_and_index(qwen).py' --embed-batch 32
  python 'scripts/embed_and_index(qwen).py' --index-batch 200

需要 SSH tunnel（若 OpenSearch 在遠端）：
  ssh -L 9200:localhost:9200 ubuntu@<OS-EC2-IP>
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

INDEX_NAME = "citation_chunks_v1"
DIMS = 512
CHECKPOINT_FILE = Path("scripts/embed_and_index_qwen_checkpoint.json")
MLX_MODEL = "mlx-community/Qwen3-Embedding-0.6B-8bit"


INDEX_MAPPING = {
    "settings": {
        "index": {
            "knn": True,
            "knn.algo_param.ef_search": 100,
        }
    },
    "mappings": {
        "properties": {
            "decision_id": {"type": "long"},
            "case_type": {"type": "keyword"},
            "target_ids": {"type": "long"},
            "target_authority_ids": {"type": "long"},
            "chunk_index": {"type": "integer"},
            "start_offset": {"type": "integer"},
            "end_offset": {"type": "integer"},
            "embedding": {
                "type": "knn_vector",
                "dimension": DIMS,
                "method": {
                    "name": "hnsw",
                    "space_type": "cosinesimil",
                    "engine": "lucene",
                    "parameters": {"ef_construction": 128, "m": 16},
                },
            },
        }
    },
}


def build_os_client():
    try:
        from opensearchpy import OpenSearch
    except ImportError:
        print("ERROR: pip install opensearch-py")
        sys.exit(1)

    url = os.environ.get("OPENSEARCH_URL", "http://localhost:9200").strip()
    parsed = urlparse(url)
    scheme = parsed.scheme or "http"
    host = parsed.hostname or "localhost"
    port = parsed.port or 9200
    use_ssl = scheme == "https"

    username = os.environ.get("OPENSEARCH_USERNAME", "admin").strip()
    password = os.environ.get("OPENSEARCH_PASSWORD", "").strip()

    kwargs = {
        "hosts": [{"host": host, "port": port}],
        "http_auth": (username, password) if password else None,
        "use_ssl": use_ssl,
        "verify_certs": False,
    }
    if use_ssl:
        kwargs["ssl_assert_hostname"] = False

    return OpenSearch(**kwargs)


def ensure_index(os_client, recreate: bool):
    if recreate and os_client.indices.exists(index=INDEX_NAME):
        os_client.indices.delete(index=INDEX_NAME)
        print(f"Deleted existing index: {INDEX_NAME}")

    if not os_client.indices.exists(index=INDEX_NAME):
        os_client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
        print(f"Created index: {INDEX_NAME} (dim={DIMS})")
    else:
        count = os_client.count(index=INDEX_NAME)["count"]
        print(f"Index {INDEX_NAME} exists ({count} docs)")


def get_db_conn():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(db_url, row_factory=dict_row)


def fetch_chunks(conn, *, case_type: str | None, after: tuple | None, limit: int | None):
    """
    回傳 unique chunks iterator。
    after = (decision_id, chunk_index) — resume 用，跳過已處理的。
    """
    where = []
    params: dict = {}

    if case_type:
        where.append("case_type = %(case_type)s")
        params["case_type"] = case_type

    if after:
        where.append("(decision_id, chunk_index) > (%(after_did)s, %(after_ci)s)")
        params["after_did"] = after[0]
        params["after_ci"] = after[1]

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    limit_sql = f"LIMIT {limit}" if limit else ""

    sql = f"""
        SELECT
            decision_id,
            chunk_index,
            MIN(start_offset) AS start_offset,
            MIN(end_offset) AS end_offset,
            MIN(chunk_text) AS chunk_text,
            MIN(case_type) AS case_type,
            array_agg(DISTINCT target_id)
                FILTER (WHERE target_id IS NOT NULL) AS target_ids,
            array_agg(DISTINCT target_authority_id)
                FILTER (WHERE target_authority_id IS NOT NULL) AS target_authority_ids
        FROM citation_chunks
        {where_sql}
        GROUP BY decision_id, chunk_index
        ORDER BY decision_id, chunk_index
        {limit_sql}
    """
    return conn.execute(sql, params)


def load_checkpoint() -> tuple | None:
    if CHECKPOINT_FILE.exists():
        data = json.loads(CHECKPOINT_FILE.read_text())
        return (data["decision_id"], data["chunk_index"])
    return None


def save_checkpoint(decision_id: int, chunk_index: int):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_FILE.write_text(
        json.dumps({"decision_id": decision_id, "chunk_index": chunk_index})
    )


def clear_checkpoint():
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()


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


def main():
    parser = argparse.ArgumentParser(
        description="Embed citation_chunks with Qwen and bulk index to OpenSearch"
    )
    parser.add_argument("--resume", action="store_true", help="從 checkpoint 繼續")
    parser.add_argument("--recreate-index", action="store_true", help="刪掉舊 index 重建")
    parser.add_argument("--limit", type=int, help="最多處理 N 個 unique chunk")
    parser.add_argument("--case-type", type=str, help="只處理特定 case_type")
    parser.add_argument(
        "--embed-batch",
        type=int,
        default=32,
        help="embedding batch size (default: 32)",
    )
    parser.add_argument(
        "--index-batch",
        type=int,
        default=200,
        help="OS bulk batch size (default: 200)",
    )
    args = parser.parse_args()

    os_client = build_os_client()
    try:
        info = os_client.info()
        endpoint = os.environ.get("OPENSEARCH_URL", "localhost:9200")
        print(f"OpenSearch: {info['version']['number']} @ {endpoint}")
    except Exception as e:
        print(f"ERROR: 無法連到 OpenSearch — {e}")
        print("提示：若 OS 在遠端，先開 SSH tunnel：ssh -L 9200:localhost:9200 ubuntu@<EC2-IP>")
        sys.exit(1)

    ensure_index(os_client, recreate=args.recreate_index)

    after = None
    if args.resume:
        after = load_checkpoint()
        if after:
            print(f"Resume from checkpoint: decision_id={after[0]}, chunk_index={after[1]}")
        else:
            print("No checkpoint found, starting from beginning")
    elif not args.recreate_index:
        clear_checkpoint()

    conn = get_db_conn()
    model = load_model()
    cursor = fetch_chunks(conn, case_type=args.case_type, after=after, limit=args.limit)

    try:
        from opensearchpy import helpers as os_helpers
    except ImportError:
        print("ERROR: pip install opensearch-py")
        sys.exit(1)

    t0 = time.time()
    total_indexed = 0
    errors = 0
    embed_buf: list[dict] = []

    def flush(buf: list[dict]):
        nonlocal total_indexed, errors
        if not buf:
            return

        texts = [row["chunk_text"] for row in buf]
        vectors = embed_batch(model, texts)

        actions = []
        for row, vec in zip(buf, vectors):
            doc = {
                "decision_id": row["decision_id"],
                "case_type": row["case_type"],
                "target_ids": row["target_ids"] or [],
                "target_authority_ids": row["target_authority_ids"] or [],
                "chunk_index": row["chunk_index"],
                "start_offset": row["start_offset"],
                "end_offset": row["end_offset"],
                "embedding": vec.tolist(),
            }
            actions.append(
                {
                    "_index": INDEX_NAME,
                    "_id": f"{row['decision_id']}_{row['chunk_index']}",
                    "_source": doc,
                }
            )

        try:
            for start in range(0, len(actions), args.index_batch):
                batch = actions[start : start + args.index_batch]
                ok, errs = os_helpers.bulk(os_client, batch, raise_on_error=False)
                total_indexed += ok
                if errs:
                    errors += len(errs)
                    print(f"  WARN: {len(errs)} bulk errors in batch", file=sys.stderr)
        except Exception as e:
            errors += len(buf)
            print(f"  ERROR: bulk failed — {e}", file=sys.stderr)
            return

        last = buf[-1]
        save_checkpoint(last["decision_id"], last["chunk_index"])

    for row in cursor:
        embed_buf.append(dict(row))

        if len(embed_buf) >= args.embed_batch:
            flush(embed_buf)
            embed_buf.clear()

            elapsed = time.time() - t0
            rate = total_indexed / elapsed if elapsed > 0 else 0
            print(f"  indexed={total_indexed}, errors={errors}, {rate:.0f} chunks/s")

    flush(embed_buf)
    embed_buf.clear()

    conn.close()
    elapsed = time.time() - t0

    print("\n--- 完成 ---")
    print(f"Indexed: {total_indexed}, Errors: {errors}")
    if elapsed > 0:
        print(f"Time: {elapsed:.1f}s ({total_indexed / elapsed:.0f} chunks/s)")
    if not errors:
        clear_checkpoint()


if __name__ == "__main__":
    main()
