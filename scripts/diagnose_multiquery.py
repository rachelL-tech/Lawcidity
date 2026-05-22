#!/usr/bin/env python3
"""
診斷：multi-query（各爭點分開召回）相對單一 query 召回，到底多救回多少？

回答三件事：
  A. recall —— 單一 query top-30 漏掉、但某個 issue 召回得到的 chunk 有多少？
  B. L1 合併風險 —— 同一 decision 被不同 issue 指向不同 chunk_index 的案例有多少？
  C. 補強（關鍵）—— 每個 issue 的 top chunk，在單一 query 裡到底排第幾？
     落在 31~50 名 → 只是 K 太小，調大就好；落在 100+ 或查無 → 真稀釋，multi-query 有據。

純讀，不動 production。embedding / Gemini 抽取結果存進磁碟快取，rerun 不重打 API。
測試案例寫死在 QUERIES 常數。

用法：
  python scripts/diagnose_multiquery.py    # 跑診斷，輸出 scripts/diagnose_result.jsonl
"""

import json
import os
import pickle
import sys
from pathlib import Path

import numpy as np
from dotenv import load_dotenv

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
load_dotenv()

from app.db import get_conn
from app.gemini_service import _get_client, extract_issues_and_statutes
from app.rag_service import _knn, _vec_to_pg, embed_query

REWRITE_PROMPT = """你是台灣法律檢索助手。把下面使用者的口語提問，改寫成「一句」精簡、正式的法律檢索 query，\
涵蓋提問中的所有法律爭點。只輸出改寫後的句子，不要解釋、不要分項。

使用者提問：{q}"""

RESULT_PATH = Path("scripts/diagnose_result.jsonl")
CACHE_PATH = Path("scripts/.diagnose_cache.pkl")

# 多爭點測試案例（口語提問，模擬真實使用者輸入）
QUERIES = [
    "我的車被撞了，如果對方主張是舊車，要折舊，這個主張成立嗎？錢會被扣多少？",
    "如果我被同事霸凌，不去上班，老闆可以扣我薪資嗎？",
    "警察在路上臨檢，堅持要看我的身分證，我可以拒絕嗎？我該怎麼請求國賠？",
]

SINGLE_K = 500   # 單 query 召回放大，才能查 issue chunk 在單 query 排第幾
ISSUE_K = 30     # 每個 issue 召回深度
SINGLE_TOPN = 30 # 「單 query top-N」的界線（指標 A 用）
ISSUE_TOPN = 10  # 每個 issue 取前幾名進比較


# ── 磁碟快取：text -> embedding / query -> issues ────────────────────

_CACHE = {"embed": {}, "issues": {}, "rewrite": {}}


def _load_cache():
    global _CACHE
    if CACHE_PATH.exists():
        _CACHE = pickle.loads(CACHE_PATH.read_bytes())
    for k in ("embed", "issues", "rewrite"):  # 舊快取可能缺新欄位
        _CACHE.setdefault(k, {})


def _save_cache():
    CACHE_PATH.write_bytes(pickle.dumps(_CACHE))


def cached_embed(text: str) -> np.ndarray:
    if text not in _CACHE["embed"]:
        _CACHE["embed"][text] = embed_query(text).tolist()
        _save_cache()
    return np.array(_CACHE["embed"][text], dtype=np.float32)


def cached_issues(query: str) -> list[str]:
    if query not in _CACHE["issues"]:
        _CACHE["issues"][query] = extract_issues_and_statutes(query).get("issues", [])
        _save_cache()
    return _CACHE["issues"][query]


def cached_rewrite(query: str) -> str:
    if query not in _CACHE["rewrite"]:
        resp = _get_client().models.generate_content(
            model="gemini-3.1-flash-lite",
            contents=REWRITE_PROMPT.format(q=query),
            config={"temperature": 0.1},
        )
        _CACHE["rewrite"][query] = resp.text.strip()
        _save_cache()
    return _CACHE["rewrite"][query]


# ── 召回 ─────────────────────────────────────────────────────────────

def _retrieve_ranked(conn, text: str, k: int) -> list[tuple[tuple[int, int], float, str]]:
    """召回並收斂到 (decision_id, chunk_index)，回傳依 sim 由高到低排序的 (key, sim, chunk_text)。"""
    vec_str = _vec_to_pg(cached_embed(text))
    best: dict[tuple[int, int], tuple[float, str]] = {}
    for row in _knn(conn, vec_str, limit=k):
        key = (row["decision_id"], row["chunk_index"])
        sim = 1 - float(row["distance"])
        if key not in best or sim > best[key][0]:
            best[key] = (sim, row["chunk_text"])
    ordered = sorted(best.items(), key=lambda kv: kv[1][0], reverse=True)
    return [(key, sim, text) for key, (sim, text) in ordered]


# ── 指標 ─────────────────────────────────────────────────────────────

def _metric_a(single_top_set: set, per_issue: dict[str, list]) -> int:
    """聯集 \\ 單query top-N：單 query 漏掉、issue 才召回到的 chunk 數。"""
    misses = set()
    for ordered in per_issue.values():
        for key, _, _ in ordered:
            if key not in single_top_set:
                misses.add(key)
    return len(misses)


def _metric_b(per_issue: dict[str, list]) -> int:
    """同一 decision 被不同 issue 指向不同 chunk_index → L1 取 best 會丟 facet。"""
    issue_best: dict[str, dict[int, int]] = {}  # issue -> {decision_id: best chunk_index}
    for issue, ordered in per_issue.items():
        per_dec: dict[int, tuple[int, float]] = {}
        for (did, cidx), sim, _ in ordered:
            if did not in per_dec or sim > per_dec[did][1]:
                per_dec[did] = (cidx, sim)
        issue_best[issue] = {did: v[0] for did, v in per_dec.items()}

    at_risk = 0
    decisions = {did for pd in issue_best.values() for did in pd}
    for did in decisions:
        picks = {pd[did] for pd in issue_best.values() if did in pd}
        if len(picks) >= 2:
            at_risk += 1
    return at_risk


SIM_RELEVANT = 0.5  # 低於此 sim 的 issue chunk 視為雜訊，不算真漏召


def _entry(rank, key, sim, text, single_rank=None, combined_rank=None,
           rewrite_rank=None, ranks=False):
    e = {"rank": rank, "decision_id": key[0], "chunk_index": key[1],
         "sim": round(sim, 4), "chunk_text": text}
    if ranks:
        # None 代表該 query top-SINGLE_K 內查無
        e["single_rank"] = single_rank
        e["combined_rank"] = combined_rank
        e["rewrite_rank"] = rewrite_rank
    return e


# ── 主流程 ───────────────────────────────────────────────────────────

def run():
    _load_cache()
    records = []
    with get_conn() as conn:
        for i, query in enumerate(QUERIES, 1):
            issues = cached_issues(query)
            print(f"\n{'='*70}\n[Case {i}] {query[:50]}...  抽到 {len(issues)} 爭點")
            if not issues:
                print("  （無爭點，跳過）")
                continue

            single = _retrieve_ranked(conn, query, SINGLE_K)
            single_rank = {key: idx + 1 for idx, (key, _, _) in enumerate(single)}
            single_top_set = {key for key, _, _ in single[:SINGLE_TOPN]}
            per_issue = {iss: _retrieve_ranked(conn, iss, ISSUE_K) for iss in issues}

            # 兩種「單一 query」對照：
            #   串接 = 爭點句子接起來（測多 facet 平均點能否涵蓋）
            #   改寫 = Gemini 把口語 query 重寫成一句正式 query（測措辭效應）
            combined_text = "；".join(issues)
            combined = _retrieve_ranked(conn, combined_text, SINGLE_K)
            combined_rank = {key: idx + 1 for idx, (key, _, _) in enumerate(combined)}

            rewrite_text = cached_rewrite(query)
            rewritten = _retrieve_ranked(conn, rewrite_text, SINGLE_K)
            rewrite_rank = {key: idx + 1 for idx, (key, _, _) in enumerate(rewritten)}
            print(f"  改寫 query：{rewrite_text}")

            single_top10 = [
                _entry(r + 1, key, sim, text)
                for r, (key, sim, text) in enumerate(single[:10])
            ]

            # 決勝：原 query 查無、但「高 sim（真相關）」的 issue chunk，串接 / 改寫救不救得回
            rec_comb = miss_comb = rec_rw = miss_rw = 0
            issues_detail = []
            for iss, ordered in per_issue.items():
                top_chunks = [
                    _entry(r + 1, key, sim, text, single_rank.get(key),
                           combined_rank.get(key), rewrite_rank.get(key), ranks=True)
                    for r, (key, sim, text) in enumerate(ordered[:ISSUE_TOPN])
                ]
                issues_detail.append({"issue": iss, "top_chunks": top_chunks})
                print(f"\n  ── issue: {iss} ──")
                for e in top_chunks:
                    sr = "查無" if e["single_rank"] is None else f"#{e['single_rank']}"
                    cr = "查無" if e["combined_rank"] is None else f"#{e['combined_rank']}"
                    rr = "查無" if e["rewrite_rank"] is None else f"#{e['rewrite_rank']}"
                    print(f"  #{e['rank']} decision={e['decision_id']} chunk={e['chunk_index']} "
                          f"sim={e['sim']} (單query {sr} / 串接 {cr} / 改寫 {rr})")
                    if e["sim"] >= SIM_RELEVANT and e["single_rank"] is None:
                        rec_comb += e["combined_rank"] is not None
                        miss_comb += e["combined_rank"] is None
                        rec_rw += e["rewrite_rank"] is not None
                        miss_rw += e["rewrite_rank"] is None

            print(f"\n  決勝（高sim且單query查無的 chunk）："
                  f"串接救回 {rec_comb}/仍查無 {miss_comb}；改寫救回 {rec_rw}/仍查無 {miss_rw}")
            records.append({
                "case": i,
                "query": query,
                "issues": issues,
                "combined_query": combined_text,
                "rewrite_query": rewrite_text,
                "single_top10": single_top10,
                "issues_detail": issues_detail,
                "metric_a_misses": _metric_a(single_top_set, per_issue),
                "metric_b_at_risk": _metric_b(per_issue),
                "combined_recovered": rec_comb, "combined_still_missing": miss_comb,
                "rewrite_recovered": rec_rw, "rewrite_still_missing": miss_rw,
            })

    RESULT_PATH.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n",
        encoding="utf-8",
    )
    c_rec = sum(r.get("combined_recovered", 0) for r in records)
    c_miss = sum(r.get("combined_still_missing", 0) for r in records)
    r_rec = sum(r.get("rewrite_recovered", 0) for r in records)
    r_miss = sum(r.get("rewrite_still_missing", 0) for r in records)
    print(f"\n{'='*70}\n已寫出 {RESULT_PATH}（{len(records)} case）")
    print(f"決勝總計（高sim且單query查無的 chunk）：")
    print(f"  串接 query 救回 {c_rec}、仍查無 {c_miss}")
    print(f"  改寫 query 救回 {r_rec}、仍查無 {r_miss}")
    print("某個單一 query 救回多 → 可取代 fan-out；兩者都仍查無多 → facet 群集太遠，fan-out 有據。")


if __name__ == "__main__":
    run()
