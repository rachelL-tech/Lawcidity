#!/usr/bin/env python3
"""
Embedding model comparison for legal text semantic search.

Usage:
  # Step 1: Select targets and generate test data
  python scripts/eval_embeddings.py --select

  # Step 2: Edit scripts/eval_targets.json — fill in "query" for each target

  # Step 3: Run evaluation
  python scripts/eval_embeddings.py --run
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

load_dotenv()

DATA_PATH = Path("scripts/eval_targets.json")


# ── DB ────────────────────────────────────────────────────────────────

def get_db_conn():
    return psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row)


def select_targets(conn):
    """Pick 6 targets (2 civil, 2 criminal, 1 admin, 1 IP-related) and snippets."""
    targets = []
    used_ids: set[int] = set()

    # 民事 x2, 刑事 x2, 行政 x1
    for category, case_type, n in [
        ("民事", "民事", 2),
        ("刑事", "刑事", 2),
        ("行政", "行政", 1),
    ]:
        exclude = list(used_ids) if used_ids else []
        rows = conn.execute("""
            SELECT t.target_id, t.cite_count, d.ref_key, d.case_type, d.doc_type
            FROM (
                SELECT target_id, count(*) AS cite_count
                FROM citations
                WHERE target_id IS NOT NULL
                  AND snippet IS NOT NULL AND length(snippet) > 50
                GROUP BY target_id
                HAVING count(*) BETWEEN 3 AND 8
            ) t
            JOIN decisions d ON d.id = t.target_id
            WHERE d.case_type = %s
              AND d.id != ALL(%s)
            ORDER BY random()
            LIMIT %s
        """, (case_type, exclude, n)).fetchall()

        for row in rows:
            used_ids.add(row["target_id"])
            targets.append({
                "target_id": row["target_id"],
                "ref_key": row["ref_key"],
                "case_type": row["case_type"],
                "doc_type": row["doc_type"],
                "category": category,
                "cite_count": row["cite_count"],
                "query": "",
            })

    # 智財相關: target cited by 智財法院, 3-10 total citations
    exclude = list(used_ids) if used_ids else []
    ipc_row = conn.execute("""
        SELECT d_target.id AS target_id, d_target.ref_key,
               d_target.case_type, d_target.doc_type,
               count(*) AS cite_count
        FROM citations c
        JOIN decisions d_src  ON d_src.id  = c.source_id
        JOIN decisions d_target ON d_target.id = c.target_id
        WHERE c.snippet IS NOT NULL AND length(c.snippet) > 50
          AND d_target.id != ALL(%s)
        GROUP BY d_target.id, d_target.ref_key,
                 d_target.case_type, d_target.doc_type
        HAVING count(*) BETWEEN 3 AND 10
           AND count(*) FILTER (
               WHERE d_src.unit_norm = '智慧財產及商業法院'
           ) >= 1
        ORDER BY random()
        LIMIT 1
    """, (exclude,)).fetchone()

    if ipc_row:
        used_ids.add(ipc_row["target_id"])
        targets.append({
            "target_id": ipc_row["target_id"],
            "ref_key": ipc_row["ref_key"],
            "case_type": ipc_row["case_type"],
            "doc_type": ipc_row["doc_type"],
            "category": "智財相關",
            "cite_count": ipc_row["cite_count"],
            "query": "",
        })
    else:
        print("WARNING: No suitable 智財-related target found.")

    # Fetch related snippets for each target
    for target in targets:
        snippets = conn.execute("""
            SELECT id AS citation_id, source_id, snippet, raw_match
            FROM citations
            WHERE target_id = %s
              AND snippet IS NOT NULL AND length(snippet) > 50
            ORDER BY id
        """, (target["target_id"],)).fetchall()
        target["related_snippets"] = [dict(s) for s in snippets]

    # Fetch 20 random unrelated snippets
    exclude = list(used_ids)
    unrelated = conn.execute("""
        SELECT id AS citation_id, source_id, target_id, snippet, raw_match
        FROM citations
        WHERE target_id != ALL(%s)
          AND snippet IS NOT NULL AND length(snippet) > 50
        ORDER BY random()
        LIMIT 20
    """, (exclude,)).fetchall()

    return targets, [dict(s) for s in unrelated]


# ── Embedding ─────────────────────────────────────────────────────────

def embed_st_model(model_name: str, texts: list[str],
                   device: str | None = None, batch_size: int = 32,
                   truncate_dim: int | None = None) -> np.ndarray:
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("ERROR: pip install sentence-transformers")
        sys.exit(1)

    dim_note = f", truncate_dim={truncate_dim}" if truncate_dim else ""
    print(f"  Loading {model_name} (device={device or 'auto'}{dim_note})...")
    model = SentenceTransformer(model_name, device=device, truncate_dim=truncate_dim)
    print(f"  Encoding {len(texts)} texts (batch_size={batch_size})...")
    return model.encode(texts, batch_size=batch_size,
                        show_progress_bar=True, normalize_embeddings=True)


def embed_gemini(texts: list[str]) -> np.ndarray:
    from google import genai

    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("ERROR: GEMINI_API_KEY not set in .env")
        sys.exit(1)

    client = genai.Client(api_key=api_key)
    embeddings = []
    batch_size = 20
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        result = client.models.embed_content(
            model="text-embedding-004",
            contents=batch,
        )
        for emb in result.embeddings:
            embeddings.append(emb.values)
        print(f"  Gemini: {min(i + batch_size, len(texts))}/{len(texts)}")
        if i + batch_size < len(texts):
            time.sleep(1)

    return np.array(embeddings)


def embed_voyage(texts: list[str], model_name: str) -> np.ndarray:
    import voyageai

    api_key = os.environ.get("VOYAGE_API_KEY")
    if not api_key:
        print("ERROR: VOYAGE_API_KEY not set in .env")
        sys.exit(1)

    vo = voyageai.Client(api_key=api_key)
    embeddings = []
    batch_size = 8
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        result = vo.embed(batch, model=model_name)
        embeddings.extend(result.embeddings)
        print(f"  {model_name}: {min(i + batch_size, len(texts))}/{len(texts)}")
        if i + batch_size < len(texts):
            time.sleep(1)

    return np.array(embeddings)


# ── Evaluation ────────────────────────────────────────────────────────

def cosine_sim(a: np.ndarray, b: np.ndarray) -> np.ndarray:
    """Cosine similarity between vector a (1D) and matrix b (2D)."""
    a = a.reshape(1, -1)
    norm_a = np.linalg.norm(a, axis=1, keepdims=True)
    norm_b = np.linalg.norm(b, axis=1, keepdims=True)
    return ((a @ b.T) / (norm_a * norm_b.T)).flatten()


def run_evaluation():
    if not DATA_PATH.exists():
        print(f"ERROR: {DATA_PATH} not found. Run --select first.")
        sys.exit(1)

    with open(DATA_PATH) as f:
        data = json.load(f)

    targets = data["targets"]
    unrelated_snippets = data["unrelated_snippets"]

    # Validate queries
    missing = [t["ref_key"] for t in targets if not t.get("query")]
    if missing:
        print(f"ERROR: Missing query for: {', '.join(missing)}")
        print(f"Edit {DATA_PATH} and fill in 'query' for each target.")
        sys.exit(1)

    # Build text list: queries first, then all snippets
    queries = [t["query"] for t in targets]

    snippet_pool = []  # (snippet_text, target_idx_or_none, citation_id)
    for i, target in enumerate(targets):
        for s in target["related_snippets"]:
            snippet_pool.append((s["snippet"], i, s["citation_id"]))
    for s in unrelated_snippets:
        snippet_pool.append((s["snippet"], None, s["citation_id"]))

    all_texts = queries + [s[0] for s in snippet_pool]

    print(f"\nTexts to embed: {len(queries)} queries + {len(snippet_pool)} snippets"
          f" = {len(all_texts)} total")

    cache_dir = DATA_PATH.parent / "eval_cache"
    cache_dir.mkdir(exist_ok=True)

    def cached_embed(name: str, fn):
        cache_file = cache_dir / f"{name}.npy"
        if cache_file.exists():
            print(f"  Loading from cache: {cache_file}")
            return np.load(cache_file)
        emb = fn()
        np.save(cache_file, emb)
        print(f"  Saved to cache: {cache_file}")
        return emb

    def truncate_and_normalize(emb: np.ndarray, dim: int) -> np.ndarray:
        t = emb[:, :dim].astype(np.float32)
        norms = np.linalg.norm(t, axis=1, keepdims=True)
        norms[norms == 0] = 1
        return t / norms

    def _load_qwen_512():
        # Try truncation from cached 1024-dim base first
        base_cache = cache_dir / "qwen3-06b.npy"
        if base_cache.exists():
            emb_1024 = np.load(base_cache)
            return truncate_and_normalize(emb_1024, 512)
        # Otherwise use sentence-transformers
        emb_1024 = embed_st_model("Qwen/Qwen3-Embedding-0.6B", all_texts)
        np.save(cache_dir / "qwen3-06b.npy", emb_1024)
        return truncate_and_normalize(emb_1024, 512)

    print("\n[1/3] Qwen3-0.6B @ 512")
    emb_q512 = cached_embed("qwen3-06b-512", _load_qwen_512)

    print("\n[2/3] voyage-law-2")
    emb_vlaw = cached_embed("voyage-law-2",
                            lambda: embed_voyage(all_texts, "voyage-law-2"))

    print("\n[3/3] voyage-4-large")
    emb_v4l = cached_embed("voyage-4-large",
                           lambda: embed_voyage(all_texts, "voyage-4-large"))

    n_q = len(queries)
    all_embs = {
        "q-512":   emb_q512,
        "v-law-2": emb_vlaw,
        "v-4-lg":  emb_v4l,
    }
    labels = ["q-512", "v-law-2", "v-4-lg"]
    gaps    = {k: [] for k in labels}
    recalls = {k: [] for k in labels}

    print("\n" + "=" * 80)
    print("RESULTS")
    print("=" * 80)

    for qi, target in enumerate(targets):
        n_related = len(target["related_snippets"])

        # Compute similarities for all models
        sims = {
            k: cosine_sim(all_embs[k][qi], all_embs[k][n_q:])
            for k in labels
        }

        # Build scored list
        scored = []
        for j, (_, target_idx, cit_id) in enumerate(snippet_pool):
            is_rel = (target_idx == qi)
            row = {"j": j, "cit_id": cit_id, "related": is_rel}
            for k in labels:
                row[k] = float(sims[k][j])
            scored.append(row)

        ranked = {k: sorted(scored, key=lambda x, k=k: x[k], reverse=True) for k in labels}

        # Metrics per model
        for k in labels:
            rel_scores   = [r[k] for r in scored if r["related"]]
            unrel_scores = [r[k] for r in scored if not r["related"]]
            gap = np.mean(rel_scores) - np.mean(unrel_scores)
            gaps[k].append(gap)
            recall5 = sum(1 for r in ranked[k][:5] if r["related"])
            recalls[k].append(recall5 / n_related)

        # Print header
        print(f"\n{'─' * 80}")
        print(f"[{qi + 1}] {target['category']} — {target['ref_key']}")
        print(f"    Query: {target['query'][:75]}...")
        print(f"    Related: {n_related}  |  Pool: {len(scored)}")

        # Top 10 table (sorted by first model as reference)
        header_cols = " ".join(f"{k:>8}" for k in labels)
        print(f"\n    {'Rank':<5} {header_cols}  Label")
        print(f"    {'─' * (5 + 9 * len(labels) + 7)}")
        ref_ranked = ranked[labels[0]]
        for rank, r in enumerate(ref_ranked[:10]):
            label = "✓ rel" if r["related"] else "  ---"
            vals = " ".join(f"{r[k]:>8.4f}" for k in labels)
            print(f"    {rank+1:<5} {vals}  {label}")
        print(f"    (sorted by {labels[0]} score)")

        # Summary metrics
        print(f"\n    {'Metric':<16} {header_cols}")
        print(f"    {'─' * (16 + 9 * len(labels))}")
        for metric_name, get_val in [
            ("avg related",   lambda k: np.mean([r[k] for r in scored if r["related"]])),
            ("avg unrelated", lambda k: np.mean([r[k] for r in scored if not r["related"]])),
            ("gap",           lambda k: gaps[k][-1]),
            ("Recall@5",      lambda k: f"{sum(1 for r in ranked[k][:5] if r['related'])}/{n_related}"),
        ]:
            vals = [get_val(k) for k in labels]
            fmt = lambda v: f"{v:>8.4f}" if isinstance(v, float) else f"{v:>8}"
            print(f"    {metric_name:<16} " + " ".join(fmt(v) for v in vals))

    # Overall summary
    header_cols = " ".join(f"{k:>8}" for k in labels)
    print(f"\n{'=' * 90}")
    print("OVERALL SUMMARY")
    print(f"{'=' * 90}")
    print(f"  {'Metric':<18} {header_cols}")
    print(f"  {'─' * (18 + 9 * len(labels))}")
    for metric_name, fn in [
        ("avg gap",      lambda k: np.mean(gaps[k])),
        ("min gap",      lambda k: np.min(gaps[k])),
        ("avg Recall@5", lambda k: np.mean(recalls[k])),
    ]:
        vals = [fn(k) for k in labels]
        print(f"  {metric_name:<18} " + " ".join(f"{v:>8.4f}" for v in vals))

    best = max(labels, key=lambda k: np.mean(gaps[k]))
    names = {
        "q-512":  "Qwen3-Embedding-0.6B @ 512",
        "v-law-2": "voyage-law-2",
        "v-4-lg":  "voyage-4-large",
    }
    print(f"\n  → Winner by avg gap: {names[best]}")


# ── CLI ───────────────────────────────────────────────────────────────

def cmd_select():
    conn = get_db_conn()
    targets, unrelated = select_targets(conn)
    conn.close()

    data = {"targets": targets, "unrelated_snippets": unrelated}
    DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_PATH, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved to {DATA_PATH}")
    print(f"\n{'=' * 60}")
    print("Selected targets — fill in 'query' for each:")
    print(f"{'=' * 60}")

    for i, t in enumerate(targets):
        print(f"\n[{i + 1}] {t['category']} — {t['ref_key']} "
              f"({t['cite_count']} citations)")
        for j, s in enumerate(t["related_snippets"][:3]):
            preview = s["snippet"][:150].replace("\n", " ")
            print(f"    snippet {j + 1}: {preview}...")

    print(f"\n{'=' * 60}")
    print(f"→ Edit {DATA_PATH}")
    print(f"  Fill in 'query' (案情描述) for each target")
    print(f"→ Then run:  python scripts/eval_embeddings.py --run")


def main():
    parser = argparse.ArgumentParser(
        description="Compare embedding models for legal text semantic search"
    )
    parser.add_argument("--select", action="store_true",
                        help="Select targets from DB and save to JSON")
    parser.add_argument("--run", action="store_true",
                        help="Run evaluation (after filling in queries)")
    args = parser.parse_args()

    if args.select:
        cmd_select()
    elif args.run:
        run_evaluation()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
