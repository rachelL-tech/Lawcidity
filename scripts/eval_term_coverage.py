"""
比較 multi-term query 下，term-coverage 對 ranking 的影響 + 查詢速度。

三個方法：
  flat_msm1   : msm=1，純 matched_count 排序（無 term-coverage）
  hit_flag    : msm=1，named queries 抓 per-pair coverage，sum(coverage) 排序（舊版）
  step_down   : msm=len 起遞減，target pool >= threshold 停

用法：
  python scripts/eval_term_coverage.py --query "損害賠償 過失" --top 200
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from typing import Any

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.opensearch_service import (  # noqa: E402
    _get_opensearch_client,
    chunk_source_ids,
    search_source_ids_opensearch,
)


INDEX_NAME = os.environ.get("OPENSEARCH_SOURCE_TARGET_INDEX", "source_target_windows_v2")
PAGE_SIZE = 5000
SOURCE_CHUNK_SIZE = 5000
HIT_LIMIT = 40000


def build_body(
    *,
    query_terms: list[str],
    source_ids: list[int],
    msm: int | None,
    with_named: bool,
    size: int,
) -> dict[str, Any]:
    should: list[dict[str, Any]] = []
    for idx, term in enumerate(query_terms):
        mp: dict[str, Any] = {"query": term}
        if with_named:
            mp["_name"] = f"t{idx}"
        should.append({"match_phrase": {"window_text_snippet": mp}})

    bool_query: dict[str, Any] = {
        "filter": [{"terms": {"source_id": source_ids}}],
    }
    if should:
        bool_query["should"] = should
        if msm is not None:
            bool_query["minimum_should_match"] = msm

    return {
        "size": size,
        "_source": ["source_id", "target_id", "target_authority_id"],
        "query": {"bool": bool_query},
        "sort": ["_doc"],
    }


def iter_pairs(
    *,
    query_terms: list[str],
    source_ids: list[int],
    msm: int | None,
    with_named: bool,
    hit_limit: int = HIT_LIMIT,
):
    client = _get_opensearch_client()
    yielded = 0
    for chunk in chunk_source_ids(source_ids, SOURCE_CHUNK_SIZE):
        if yielded >= hit_limit:
            break
        search_after: list[Any] | None = None
        while yielded < hit_limit:
            size = min(PAGE_SIZE, hit_limit - yielded)
            body = build_body(
                query_terms=query_terms,
                source_ids=chunk,
                msm=msm,
                with_named=with_named,
                size=size,
            )
            if search_after is not None:
                body["search_after"] = search_after

            resp = client.search(index=INDEX_NAME, body=body)
            hits = ((resp.get("hits") or {}).get("hits") or [])
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
                target_auth = src.get("target_authority_id")
                if target_id is None and target_auth is None:
                    continue
                target_key = (
                    f"decision:{int(target_id)}" if target_id is not None
                    else f"authority:{int(target_auth)}"
                )
                coverage = 0
                if with_named:
                    mq = hit.get("matched_queries") or []
                    coverage = len({n for n in mq if isinstance(n, str) and n.startswith("t")})
                yield {
                    "source_id": source_id,
                    "target_key": target_key,
                    "coverage": coverage,
                }
                yielded += 1
                last_sort = hit.get("sort")
                if yielded >= hit_limit:
                    break

            if yielded >= hit_limit or len(hits) < size or not last_sort:
                break
            search_after = last_sort


def agg_flat(pairs) -> dict[str, dict[str, Any]]:
    agg: dict[str, dict[str, Any]] = {}
    for p in pairs:
        row = agg.setdefault(p["target_key"], {"sources": set()})
        row["sources"].add(p["source_id"])
    return agg


def agg_hit_flag(pairs) -> dict[str, dict[str, Any]]:
    agg: dict[str, dict[str, Any]] = {}
    for p in pairs:
        row = agg.setdefault(p["target_key"], {"sources": {}, "best_coverage": 0})
        sid = p["source_id"]
        prev = row["sources"].get(sid, 0)
        if p["coverage"] > prev:
            row["sources"][sid] = p["coverage"]
        if p["coverage"] > row["best_coverage"]:
            row["best_coverage"] = p["coverage"]
    return agg


def rank_flat(agg):
    rows = [
        (k, {"matched_count": len(v["sources"])})
        for k, v in agg.items()
    ]
    rows.sort(key=lambda r: -r[1]["matched_count"])
    return rows


def rank_hit_flag(agg):
    rows = []
    for k, v in agg.items():
        matched_count = len(v["sources"])
        coverage_sum = sum(v["sources"].values())
        rows.append((k, {
            "matched_count": matched_count,
            "coverage_sum": coverage_sum,
            "best_coverage": v["best_coverage"],
        }))
    rows.sort(key=lambda r: (-r[1]["coverage_sum"], -r[1]["matched_count"]))
    return rows


def run_step_down(
    *,
    query_terms: list[str],
    source_ids: list[int],
    threshold: int,
):
    n = len(query_terms)
    pool: dict[str, dict[str, Any]] = {}
    start = time.perf_counter()
    total_pairs = 0
    steps_used = 0
    for msm in range(n, 0, -1):
        steps_used += 1
        pairs_list = list(iter_pairs(
            query_terms=query_terms,
            source_ids=source_ids,
            msm=msm,
            with_named=False,
        ))
        total_pairs += len(pairs_list)
        level_agg = agg_flat(iter(pairs_list))
        for k, v in level_agg.items():
            if k not in pool:
                pool[k] = {
                    "matched_count": len(v["sources"]),
                    "reached_at_msm": msm,
                }
        if len(pool) >= threshold:
            break
    elapsed = time.perf_counter() - start
    rows = list(pool.items())
    rows.sort(key=lambda r: (-r[1]["reached_at_msm"], -r[1]["matched_count"]))
    return rows, elapsed, total_pairs, steps_used


def dump_top(label, rows, n=10):
    print(f"[{label}] top{n}:")
    for i, (k, v) in enumerate(rows[:n], 1):
        extras = " ".join(f"{kk}={vv}" for kk, vv in v.items())
        print(f"  #{i:<3} {k:24s} {extras}")
    print()


def overlap(a, b, n):
    sa = {k for k, _ in a[:n]}
    sb = {k for k, _ in b[:n]}
    return len(sa & sb)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True, help="space-separated terms")
    parser.add_argument("--top", type=int, default=200)
    parser.add_argument("--threshold", type=int, default=200)
    args = parser.parse_args()

    query_terms = args.query.split()
    print(f"== query = {query_terms} ({len(query_terms)} terms) ==\n")

    t0 = time.perf_counter()
    source_ids = search_source_ids_opensearch(
        query_terms=query_terms,
        case_types=[],
        statute_filters=[],
        exclude_terms=[],
        exclude_statute_filters=[],
    )
    t_recall = time.perf_counter() - t0
    print(f"[stage1] |source_ids| = {len(source_ids)}  ({t_recall:.2f}s)\n")

    # 1. flat_msm1
    t0 = time.perf_counter()
    pairs1 = list(iter_pairs(query_terms=query_terms, source_ids=source_ids, msm=1, with_named=False))
    flat_rows = rank_flat(agg_flat(iter(pairs1)))
    t_flat = time.perf_counter() - t0
    print(f"[flat_msm1]  pairs={len(pairs1)}  targets={len(flat_rows)}  time={t_flat:.2f}s")

    # 2. hit_flag
    t0 = time.perf_counter()
    pairs2 = list(iter_pairs(query_terms=query_terms, source_ids=source_ids, msm=1, with_named=True))
    hf_rows = rank_hit_flag(agg_hit_flag(iter(pairs2)))
    t_hf = time.perf_counter() - t0
    print(f"[hit_flag]   pairs={len(pairs2)}  targets={len(hf_rows)}  time={t_hf:.2f}s")

    # 3. step_down
    sd_rows, t_sd, sd_pairs, sd_steps = run_step_down(
        query_terms=query_terms,
        source_ids=source_ids,
        threshold=args.threshold,
    )
    print(f"[step_down]  pairs={sd_pairs}  targets={len(sd_rows)}  time={t_sd:.2f}s  steps={sd_steps}")
    print()

    dump_top("flat_msm1", flat_rows)
    dump_top("hit_flag", hf_rows)
    dump_top("step_down", sd_rows)

    print(f"== overlap (top {args.top}) ==")
    print(f"  flat ∩ hit_flag      = {overlap(flat_rows, hf_rows, args.top):>3} / {args.top}")
    print(f"  flat ∩ step_down     = {overlap(flat_rows, sd_rows, args.top):>3} / {args.top}")
    print(f"  hit_flag ∩ step_down = {overlap(hf_rows, sd_rows, args.top):>3} / {args.top}")


if __name__ == "__main__":
    main()
