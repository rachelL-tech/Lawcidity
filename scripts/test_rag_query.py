#!/usr/bin/env python3
"""
快速測試 RAG 向量搜尋召回率。

Usage:
  python scripts/test_rag_query.py "案情描述" [--case-type 民事|刑事|行政] [--top 20]
"""

import argparse
import os
import sys
from pathlib import Path

import numpy as np
import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

DIMS = 512
MLX_MODEL = "mlx-community/Qwen3-Embedding-0.6B-8bit"

_model_cache = None


def load_model():
    global _model_cache
    if _model_cache:
        return _model_cache
    try:
        from mlx_embeddings.utils import load as mlx_load
    except ImportError:
        print("ERROR: pip install mlx-embeddings")
        sys.exit(1)
    print(f"Loading {MLX_MODEL}...")
    _model_cache = mlx_load(MLX_MODEL)
    return _model_cache


def embed_query(text: str) -> np.ndarray:
    import mlx.core as mx
    model, tokenizer = load_model()
    enc = tokenizer.encode(text, max_length=512, truncation=True)
    out = model(mx.array([enc]), attention_mask=mx.array([[1] * len(enc)]))
    vec = np.array(out.text_embeds)[0, :DIMS]
    norm = np.linalg.norm(vec)
    return vec / norm if norm > 0 else vec


def vec_to_pg(vec: np.ndarray) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


def search(query: str, case_type: str | None, top: int):
    vec = embed_query(query)
    vec_str = vec_to_pg(vec)

    db_url = os.environ.get("DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations")
    with psycopg.connect(db_url, row_factory=dict_row) as conn:
        where_parts = ["cc.embedding IS NOT NULL"]
        if case_type:
            where_parts.append("cc.case_type = %s")

        where_sql = " AND ".join(where_parts)

        # params 順序須與 SQL 中 %s 出現順序一致：
        # 1. SELECT distance: vec_str
        # 2. WHERE case_type (若有)
        # 3. ORDER BY: vec_str
        # 4. inner LIMIT: top*3
        # 5. outer LIMIT: top
        params: list = [vec_str]
        if case_type:
            params.append(case_type)
        params.extend([vec_str, top])

        rows = conn.execute(f"""
            SELECT
                cc.decision_id,
                cc.chunk_index,
                cc.case_type,
                cc.chunk_text,
                d.root_norm,
                d.display_title,
                d.doc_type,
                cc.embedding <=> %s::vector AS distance
            FROM citation_chunks cc
            JOIN decisions d ON d.id = cc.decision_id
            WHERE {where_sql}
            ORDER BY cc.embedding <=> %s::vector
            LIMIT %s
        """, params).fetchall()

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="案情描述文字")
    parser.add_argument("--case-type", help="民事|刑事|行政")
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    print(f"\nQuery: {args.query}")
    if args.case_type:
        print(f"Filter: case_type = {args.case_type}")
    print(f"Top: {args.top}\n")

    results = search(args.query, args.case_type, args.top)

    print(f"{'Rank':<5} {'Dist':>6}  {'ref_key':<40}  {'type':<3}  chunk_idx")
    print("─" * 80)
    for i, r in enumerate(results, 1):
        ref = r["root_norm"] or r["display_title"] or str(r["decision_id"])
        print(f"{i:<5} {r['distance']:>6.4f}  {ref:<40}  "
              f"{(r['case_type'] or ''):<3}  [{r['chunk_index']}]")
        print(f"      {r['chunk_text'].replace(chr(10), ' ')}")
        print()


if __name__ == "__main__":
    main()
