#!/usr/bin/env python3
"""
Export / restore citation_chunks embeddings as JSONL.

Usage:
  # Export existing embeddings to file
  python scripts/export_embeddings.py export --out scripts/embeddings_backup.jsonl

  # Restore from file (after schema migration)
  python scripts/export_embeddings.py restore --in scripts/embeddings_backup.jsonl
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)


def get_db_conn():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(db_url, row_factory=dict_row)


def cmd_export(out_path: Path):
    conn = get_db_conn()

    total = conn.execute(
        "SELECT COUNT(*) AS cnt FROM citation_chunks WHERE embedding IS NOT NULL"
    ).fetchone()["cnt"]
    print(f"Rows with embedding: {total}")

    if total == 0:
        print("Nothing to export.")
        return

    cursor = conn.execute("""
        SELECT id, decision_id, chunk_index, chunk_type, embedding::text AS emb
        FROM citation_chunks
        WHERE embedding IS NOT NULL
        ORDER BY id
    """)

    written = 0
    t0 = time.time()
    with open(out_path, "w") as f:
        for row in cursor:
            f.write(json.dumps({
                "id":           row["id"],
                "decision_id":  row["decision_id"],
                "chunk_index":  row["chunk_index"],
                "chunk_type":   row["chunk_type"],
                "emb":          row["emb"],
            }) + "\n")
            written += 1
            if written % 10000 == 0:
                elapsed = time.time() - t0
                print(f"  {written}/{total} ({elapsed:.0f}s)")

    conn.close()
    size_mb = out_path.stat().st_size / 1024 / 1024
    print(f"\nExported {written} rows → {out_path} ({size_mb:.1f} MB)")


def cmd_restore(in_path: Path, use_natural_key: bool = False):
    """
    use_natural_key=False（預設）：用 id 對齊，適合同一台 DB。
    use_natural_key=True：用 (decision_id, chunk_index, chunk_type) 對齊，
                          適合跨 DB 或 id 不同的情況。
    """
    if not in_path.exists():
        print(f"ERROR: {in_path} not found")
        sys.exit(1)

    conn = get_db_conn()

    total = sum(1 for _ in open(in_path))
    key_mode = "natural key (decision_id, chunk_index, chunk_type)" if use_natural_key else "id"
    print(f"Rows to restore: {total}  |  match by: {key_mode}")

    if use_natural_key:
        update_sql = """
            UPDATE citation_chunks SET embedding = %s::vector
            WHERE decision_id = %s AND chunk_index = %s AND chunk_type = %s
        """
        def make_params(rec):
            return (rec["emb"], rec["decision_id"], rec["chunk_index"], rec["chunk_type"])
    else:
        update_sql = "UPDATE citation_chunks SET embedding = %s::vector WHERE id = %s"
        def make_params(rec):
            return (rec["emb"], rec["id"])

    restored = 0
    t0 = time.time()

    with open(in_path) as f:
        batch = []
        for line in f:
            rec = json.loads(line)
            batch.append(make_params(rec))

            if len(batch) >= 500:
                with conn.cursor() as cur:
                    cur.executemany(update_sql, batch)
                conn.commit()
                restored += len(batch)
                batch.clear()
                elapsed = time.time() - t0
                print(f"  {restored}/{total} ({elapsed:.0f}s)")

    if batch:
        with conn.cursor() as cur:
            cur.executemany(update_sql, batch)
        conn.commit()
        restored += len(batch)

    conn.close()
    print(f"\nRestored {restored} rows")


def main():
    parser = argparse.ArgumentParser(description="Export/restore citation_chunks embeddings")
    sub = parser.add_subparsers(dest="cmd")

    p_export = sub.add_parser("export", help="Export embeddings to JSONL")
    p_export.add_argument("--out", required=True, type=Path, help="Output JSONL path")

    p_restore = sub.add_parser("restore", help="Restore embeddings from JSONL")
    p_restore.add_argument("--in", dest="in_path", required=True, type=Path, help="Input JSONL path")
    p_restore.add_argument("--natural-key", action="store_true",
                           help="用 (decision_id, chunk_index, chunk_type) 對齊，適合跨 DB")

    args = parser.parse_args()

    if args.cmd == "export":
        cmd_export(args.out)
    elif args.cmd == "restore":
        cmd_restore(args.in_path, use_natural_key=args.natural_key)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
