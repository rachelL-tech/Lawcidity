#!/usr/bin/env python3
"""
驗證 snippet-adjacent chunking 的切塊品質，匯出 JSONL 供肉眼檢查。

Usage:
  python scripts/validate_chunks.py                      # 每個 case_type 抽 5 篇 → JSONL
  python scripts/validate_chunks.py --per-type 3          # 每個 case_type 抽 3 篇
  python scripts/validate_chunks.py --decision-id 12345   # 指定單一 decision
  python scripts/validate_chunks.py --decision-id 631711  # 前次 bug 案例
"""

import argparse
import json
import os
import re
import sys
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

# ── Chunking 參數 ──────────────────────────────────────────────────────────

MAX_CHUNK_LEN = 2000

# 小節符 pattern（台灣法律文書常見）
# minor markers 要求行首（\n 或文首），避免句中誤判
SECTION_RE = re.compile(
    r'(?:^|\n)[㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩]'
    r'|(?:^|\n)[⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛]'
    r'|(?:^|\n)[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]'
    r'|(?:^|\n)[⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽]'
    r'|\n[一二三四五六七八九十]+、'
)

# Footer pattern（判決書尾端：中　華　民　國）
FOOTER_RE = re.compile(r'中\s*華\s*民\s*國')


# ── Chunking 邏輯 ─────────────────────────────────────────────────────────

def find_section_markers(text: str) -> list[int]:
    """找出 text 中所有小節符的 offset（指向 marker 字元本身，不含前導 \\n）。"""
    result = []
    for m in SECTION_RE.finditer(text):
        pos = m.start()
        # 若匹配到前導 \n，offset 指向 \n 之後的 marker 字元
        if text[pos] == '\n':
            pos += 1
        result.append(pos)
    return result


def find_footer_start(text: str) -> int | None:
    """找出判決書尾端 '中　華　民　國' 的位置（限最後 500 字）。"""
    search_from = max(0, len(text) - 500)
    m = FOOTER_RE.search(text, search_from)
    return m.start() if m else None


def _locate_snippet(text: str, snippet: str, match_start: int) -> tuple[int, int]:
    """在 clean_text 中定位 snippet 的實際 [start, end) 位置。"""
    if not snippet or len(snippet) < 10:
        return match_start, match_start
    # snippet 應在 match_start 附近，往前搜尋
    search_from = max(0, match_start - len(snippet) - 50)
    search_to = min(len(text), match_start + 50)
    prefix = snippet[:min(80, len(snippet))]
    pos = text.find(prefix, search_from, search_to)
    if pos >= 0:
        return pos, pos + len(snippet)
    # fallback: 找不到就用 match 位置
    return match_start, match_start


def find_chunk_bounds(text: str, match_start: int, match_end: int,
                      markers: list[int],
                      snippet: str | None = None,
                      footer_pos: int | None = None) -> tuple[int, int]:
    """
    根據小節符找出 chunk 的 [start, end) 邊界。

    保證：
    - citation (match_start:match_end) 在 chunk 內
    - snippet 全文在 chunk 內（若可定位）
    - chunk <= MAX_CHUNK_LEN
    - 邊界偏好：小節符 > 句號(。) > 硬切
    - chunk 不超過 footer（中華民國...）
    """
    # 1. 必須包含的最小範圍
    must_start, must_end = match_start, match_end
    if snippet:
        s_start, s_end = _locate_snippet(text, snippet, match_start)
        must_start = min(must_start, s_start)
        must_end = max(must_end, s_end)

    # 2. 嘗試用小節符
    before = [m for m in markers if m < must_start]
    chunk_start = before[-1] if before else 0

    after = [m for m in markers if m > must_end]
    chunk_end = after[0] if after else len(text)

    # 3. 若 <= MAX_CHUNK_LEN，直接用
    if chunk_end - chunk_start <= MAX_CHUNK_LEN:
        return _apply_footer(chunk_start, chunk_end, footer_pos, must_end)

    # 4. 太長 → 用。作為 fallback 起點
    #    從 must_start 往前找最近的。，但不能太遠
    budget_before = MAX_CHUNK_LEN - (must_end - must_start)  # snippet 本身佔的空間之外可分配的
    budget_before = max(budget_before // 2, 50)  # 前文至少 50 字，至多一半預算

    period_search_from = max(0, must_start - budget_before)
    period_pos = text.rfind('。', period_search_from, must_start)
    if period_pos >= 0:
        chunk_start = period_pos + 1  # 句號之後
    else:
        chunk_start = max(0, must_start - budget_before)

    # 5. 後端：從 must_end 往後找最近的。或小節符
    remaining = MAX_CHUNK_LEN - (must_end - chunk_start)
    if remaining > 0:
        # 嘗試用小節符
        after_close = [m for m in after if m <= must_end + remaining]
        if after_close:
            chunk_end = after_close[0]
        else:
            # 找。
            period_after = text.find('。', must_end, must_end + remaining)
            if period_after >= 0:
                chunk_end = period_after + 1
            else:
                chunk_end = min(len(text), must_end + remaining)
    else:
        chunk_end = must_end

    # 6. 最終安全檢查：must_end 必須在 chunk 內
    if chunk_end < must_end:
        chunk_end = must_end
        chunk_start = max(0, chunk_end - MAX_CHUNK_LEN)

    return _apply_footer(chunk_start, chunk_end, footer_pos, must_end)


def _apply_footer(chunk_start: int, chunk_end: int,
                  footer_pos: int | None, must_end: int) -> tuple[int, int]:
    """若 chunk_end 超過 footer 且 citation 不在 footer 之後，截斷到 footer。"""
    if footer_pos is not None and chunk_end > footer_pos and must_end <= footer_pos:
        chunk_end = footer_pos
    return chunk_start, chunk_end


def merge_overlapping(items: list[tuple]) -> list[tuple]:
    """
    合併重疊的 chunks。
    items: [(citation_dict, chunk_start, chunk_end), ...]
    Returns: [(chunk_start, chunk_end, [citation_dicts]), ...]
    """
    if not items:
        return []

    items.sort(key=lambda x: x[1])
    merged = []
    cur_start, cur_end, cur_cites = items[0][1], items[0][2], [items[0][0]]

    for c, s, e in items[1:]:
        if s < cur_end:  # overlap
            cur_end = max(cur_end, e)
            cur_cites.append(c)
        else:
            merged.append((cur_start, cur_end, cur_cites))
            cur_start, cur_end, cur_cites = s, e, [c]

    merged.append((cur_start, cur_end, cur_cites))
    return merged


# ── 主程式 ─────────────────────────────────────────────────────────────────

def process_decision(conn, decision_id: int) -> list[dict]:
    """處理一篇 decision，回傳 chunk records (list of dicts)。"""
    d = conn.execute(
        "SELECT id, unit_norm, jyear, jcase_norm, jno, case_type, clean_text "
        "FROM decisions WHERE id = %s", (decision_id,)
    ).fetchone()
    if not d or not d["clean_text"]:
        return []

    text = d["clean_text"]
    ref = f"{d['unit_norm']} {d['jyear']}年{d['jcase_norm']}字第{d['jno']}號"

    cites = conn.execute(
        "SELECT id, match_start, match_end, target_id, target_authority_id, "
        "       raw_match, snippet "
        "FROM citations WHERE source_id = %s "
        "ORDER BY match_start",
        (decision_id,)
    ).fetchall()
    if not cites:
        return []

    markers = find_section_markers(text)
    footer_pos = find_footer_start(text)

    # 每個 citation 的 chunk 邊界
    items = []
    for c in cites:
        cs, ce = find_chunk_bounds(text, c["match_start"], c["match_end"],
                                   markers, snippet=c["snippet"],
                                   footer_pos=footer_pos)
        items.append((c, cs, ce))

    # 合併
    merged = merge_overlapping(items)

    # 輸出 records
    records = []
    for chunk_idx, (cs, ce, chunk_cites) in enumerate(merged):
        chunk_text = text[cs:ce]
        # 驗證每個 citation 是否在 chunk 範圍內
        for c in chunk_cites:
            snippet_inside = True
            if c["snippet"]:
                snippet_inside = c["snippet"][:40] in chunk_text

            records.append({
                "decision_id": decision_id,
                "source_ref": ref,
                "case_type": d["case_type"],
                "clean_text_len": len(text),
                "marker_count": len(markers),
                "chunk_index": chunk_idx,
                "chunk_start": cs,
                "chunk_end": ce,
                "chunk_len": ce - cs,
                "chunk_text": chunk_text,
                "citation_id": c["id"],
                "match_start": c["match_start"],
                "match_end": c["match_end"],
                "raw_match": c["raw_match"],
                "snippet": c["snippet"],
                "snippet_fully_inside": snippet_inside,
                "citation_inside_chunk": cs <= c["match_start"] and c["match_end"] <= ce,
                "target_id": c["target_id"],
                "target_authority_id": c["target_authority_id"],
                "citations_in_chunk": len(chunk_cites),
            })

    return records


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-type", type=int, default=5,
                        help="每個 case_type 抽幾篇 (default: 5)")
    parser.add_argument("--decision-id", type=int,
                        help="指定單一 decision_id")
    parser.add_argument("--output", type=str, default="scripts/validate_chunks.jsonl",
                        help="JSONL 輸出路徑 (default: scripts/validate_chunks.jsonl)")
    args = parser.parse_args()

    db_url = os.environ.get("DATABASE_URL",
                            "postgresql://postgres:postgres@localhost:5432/citations")
    conn = psycopg.connect(db_url, row_factory=dict_row)

    all_records = []

    if args.decision_id:
        all_records = process_decision(conn, args.decision_id)
    else:
        for case_type in ["民事", "刑事", "行政"]:
            rows = conn.execute("""
                SELECT c.source_id, COUNT(*) AS cnt
                FROM citations c
                JOIN decisions d ON d.id = c.source_id
                WHERE d.case_type = %s
                  AND d.clean_text IS NOT NULL
                GROUP BY c.source_id
                HAVING COUNT(*) BETWEEN 3 AND 8
                ORDER BY RANDOM()
                LIMIT %s
            """, (case_type, args.per_type)).fetchall()

            for r in rows:
                records = process_decision(conn, r["source_id"])
                all_records.extend(records)
                print(f"  {case_type} decision_id={r['source_id']}: "
                      f"{len(records)} citation-chunk records")

        # 加入前次 bug 案例
        for bug_id in [631711, 793057, 546750, 310299]:
            bug_records = process_decision(conn, bug_id)
            if bug_records:
                all_records.extend(bug_records)
                print(f"  [bug case] decision_id={bug_id}: {len(bug_records)} records")

    conn.close()

    # 寫 JSONL
    out_path = Path(args.output)
    with open(out_path, "w", encoding="utf-8") as f:
        for rec in all_records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # 摘要
    total = len(all_records)
    inside_ok = sum(1 for r in all_records if r["citation_inside_chunk"])
    snippet_ok = sum(1 for r in all_records if r["snippet_fully_inside"])
    chunk_lens = [r["chunk_len"] for r in all_records]

    print(f"\n--- 摘要 ---")
    print(f"Total records: {total}")
    print(f"Citation inside chunk: {inside_ok}/{total}")
    print(f"Snippet inside chunk:  {snippet_ok}/{total}")
    if chunk_lens:
        print(f"Chunk length: min={min(chunk_lens)}, max={max(chunk_lens)}, "
              f"avg={sum(chunk_lens)/len(chunk_lens):.0f}")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
