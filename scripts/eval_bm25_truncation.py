"""
比對 BM25 排序 + max_hits 截斷對 top-target 覆蓋率的影響。

四組設定：
  A_old = 現版未修正（_score + _doc, cap=40000, 無 msm, rank 按 avg top5 BM25）
  E_new = 新修正版（_score + _doc, cap=40000, strict msm=1→fallback msm=None, rank 按 sum top5 BM25）
  F_old = 無 BM25 sort 的舊版（_doc, cap=40000, strict msm=1→fallback msm=None, rank 按 matched_count）
  B = 放寬 cap 上界（_score + _doc, cap=500000, 無 msm, rank 按 avg top5 BM25）— 只做 reference

用法：
  python scripts/eval_bm25_truncation.py --query 損害賠償 --top 200
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from collections import defaultdict
from typing import Any

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.opensearch_service import (  # noqa: E402
    _get_opensearch_client,
    build_source_target_rerank_query,
    chunk_source_ids,
    search_source_ids_opensearch,
)


def iter_pair_hits(
    *,
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    sort: list[Any],
    max_hits: int,
    page_size: int = 5000,
    source_chunk_size: int = 5000,
    minimum_should_match: int | None = None,
):
    client = _get_opensearch_client()
    index_name = os.environ.get("OPENSEARCH_SOURCE_TARGET_INDEX", "source_target_windows_v2")

    yielded = 0
    for source_id_chunk in chunk_source_ids(source_ids, source_chunk_size):
        if yielded >= max_hits:
            break
        search_after: list[Any] | None = None
        while yielded < max_hits:
            body = build_source_target_rerank_query(
                query_terms=query_terms,
                source_ids=source_id_chunk,
                statute_filters=statute_filters,
                size=min(page_size, max_hits - yielded),
            )
            body["sort"] = sort
            if minimum_should_match is not None:
                body["query"]["bool"]["minimum_should_match"] = minimum_should_match
            if search_after is not None:
                body["search_after"] = search_after

            response = client.search(index=index_name, body=body)
            hits = ((response.get("hits") or {}).get("hits") or [])
            if not hits:
                break

            last_sort = None
            for hit in hits:
                src = hit.get("_source") or {}
                try:
                    source_id = int(src.get("source_id"))
                except Exception:
                    continue
                target_id = src.get("target_id")
                target_authority_id = src.get("target_authority_id")
                yield {
                    "source_id": source_id,
                    "target_id": int(target_id) if target_id is not None else None,
                    "target_authority_id": (
                        int(target_authority_id) if target_authority_id is not None else None
                    ),
                    "score": float(hit.get("_score") or 0.0),
                }
                yielded += 1
                last_sort = hit.get("sort")
                if yielded >= max_hits:
                    break

            if yielded >= max_hits or len(hits) < body["size"] or not last_sort:
                break
            search_after = last_sort


def aggregate_by_target(hits) -> dict[str, dict[str, Any]]:
    agg: dict[str, dict[str, Any]] = {}
    for h in hits:
        if h["target_id"] is not None:
            key = f"decision:{h['target_id']}"
        elif h["target_authority_id"] is not None:
            key = f"authority:{h['target_authority_id']}"
        else:
            continue
        row = agg.setdefault(key, {"scores": {}, "sources": set()})
        row["sources"].add(h["source_id"])
        prev = row["scores"].get(h["source_id"], 0.0)
        if h["score"] > prev:
            row["scores"][h["source_id"]] = h["score"]
    return agg


def rank_targets(
    agg: dict[str, dict[str, Any]],
    *,
    rank_by: str = "bm25_avg",
) -> list[tuple[str, dict[str, Any]]]:
    out = []
    for key, row in agg.items():
        top5 = sorted(row["scores"].values(), reverse=True)[:5]
        if rank_by == "bm25_sum":
            score = sum(top5)
        else:
            score = sum(top5) / len(top5) if top5 else 0.0
        out.append(
            (
                key,
                {
                    "score": score,
                    "matched_count": len(row["sources"]),
                    "preview_len": min(5, len(row["sources"])),
                },
            )
        )
    if rank_by == "matched":
        out.sort(key=lambda item: (-item[1]["matched_count"], -item[1]["score"]))
    else:
        out.sort(key=lambda item: (-item[1]["score"], -item[1]["matched_count"]))
    return out


def dump_top_details(
    label: str,
    ranked: list[tuple[str, dict[str, Any]]],
    agg: dict[str, dict[str, Any]],
    n: int = 10,
):
    print(f"[{label}] top{n} 細節（rank | target | matched | score | BM25 top5）：")
    for idx, (key, row) in enumerate(ranked[:n], 1):
        top5_scores = sorted(agg[key]["scores"].values(), reverse=True)[:5]
        top5_str = ", ".join(f"{s:.2f}" for s in top5_scores)
        print(f"  #{idx:<3} {key:24s}  matched={row['matched_count']:<5d}  score={row['score']:6.2f}  top5=[{top5_str}]")
    print()


def summarise(label: str, ranked: list[tuple[str, dict[str, Any]]], top: int):
    head = ranked[:top]
    counts = [row["matched_count"] for _, row in head]
    previews = [row["preview_len"] for _, row in head]
    print(f"[{label}] top{top} targets:")
    print(f"  total targets found = {len(ranked)}")
    if not counts:
        print("  (empty)\n")
        return
    print(f"  sum(matched)        = {sum(counts)}")
    print(f"  mean(matched)       = {statistics.mean(counts):.2f}")
    print(f"  median(matched)     = {statistics.median(counts)}")
    print(f"  #(matched==1)       = {sum(1 for c in counts if c == 1)}")
    print(f"  #(preview==1)       = {sum(1 for p in previews if p == 1)}")
    print(f"  #(preview==5)       = {sum(1 for p in previews if p == 5)}")
    print()


def compare(a_ranked, b_ranked, top: int, label_a: str, label_b: str):
    a_map = dict(a_ranked[:top])
    b_map = dict(b_ranked)
    shared = [k for k in a_map if k in b_map]
    diffs = []
    for key in shared:
        delta = b_map[key]["matched_count"] - a_map[key]["matched_count"]
        if delta != 0:
            diffs.append((key, a_map[key]["matched_count"], b_map[key]["matched_count"], delta))
    diffs.sort(key=lambda row: -row[3])
    print(f"[compare] {label_a} top{top} vs {label_b}（同 target 的 matched 差異）:")
    print(f"  shared targets     = {len(shared)}")
    print(f"  targets with delta = {len(diffs)}")
    if diffs:
        total_delta = sum(d[3] for d in diffs)
        print(f"  total delta(b-a)   = {total_delta}")
        print(f"  max delta          = {diffs[0][3]} ({diffs[0][0]}: {diffs[0][1]} → {diffs[0][2]})")
        print("  top 10 most-truncated targets:")
        for key, a_c, b_c, delta in diffs[:10]:
            print(f"    {key:30s}  A={a_c:4d}  {label_b}={b_c:4d}  Δ=+{delta}")
    print()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, default="損害賠償")
    parser.add_argument("--top", type=int, default=200)
    parser.add_argument("--source-limit", type=int, default=None)
    parser.add_argument("--cap-a", type=int, default=40000, help="現版 max_hits")
    parser.add_argument("--cap-b", type=int, default=500000, help="放寬 max_hits")
    args = parser.parse_args()

    query_terms = [args.query]

    print(f"== query = {args.query!r} ==\n")

    print("[stage 1] 召回 source_ids ...")
    source_ids = search_source_ids_opensearch(
        query_terms=query_terms,
        case_types=[],
        statute_filters=[],
        exclude_terms=[],
        exclude_statute_filters=[],
        source_limit=args.source_limit,
    )
    print(f"  |source_ids| = {len(source_ids)}\n")

    # (label, sort, cap, rank_by, msm, use_fallback)
    runs = [
        ("A_old 現版未修正",            ["_score", "_doc"], args.cap_a, "bm25_avg", None, False),
        ("B 上界reference",             ["_score", "_doc"], args.cap_b, "bm25_avg", None, False),
        ("E_new 新修正(sum top5)",       ["_score", "_doc"], args.cap_a, "bm25_sum", 1,    True),
        ("F_old 舊版(_doc, matched)",    ["_doc"],           args.cap_a, "matched",  1,    True),
    ]

    ranked_by_label: dict[str, list] = {}
    agg_by_label: dict[str, dict] = {}
    for label, sort, cap, rank_by, msm, use_fallback in runs:
        print(f"[{label}] iterating pair hits (cap={cap}, msm={msm}, fallback={use_fallback})...")
        hits = list(
            iter_pair_hits(
                query_terms=query_terms,
                source_ids=source_ids,
                statute_filters=[],
                sort=sort,
                max_hits=cap,
                minimum_should_match=msm,
            )
        )
        if not hits and use_fallback:
            print(f"  strict path 空，走 fallback (msm=None)...")
            hits = list(
                iter_pair_hits(
                    query_terms=query_terms,
                    source_ids=source_ids,
                    statute_filters=[],
                    sort=sort,
                    max_hits=cap,
                    minimum_should_match=None,
                )
            )
        print(f"  hits yielded = {len(hits)}")
        agg = aggregate_by_target(hits)
        ranked = rank_targets(agg, rank_by=rank_by)
        ranked_by_label[label] = ranked
        agg_by_label[label] = agg
        summarise(label, ranked, args.top)
        dump_top_details(label, ranked, agg, n=10)

    labels = list(ranked_by_label.keys())
    for i in range(len(labels)):
        for j in range(i + 1, len(labels)):
            li, lj = labels[i], labels[j]
            set_i = {k for k, _ in ranked_by_label[li][:args.top]}
            set_j = {k for k, _ in ranked_by_label[lj][:args.top]}
            print(f"  {li:30s} ∩ {lj:30s} = {len(set_i & set_j):>3} / {args.top}")


if __name__ == "__main__":
    main()
