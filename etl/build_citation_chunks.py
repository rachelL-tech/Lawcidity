#!/usr/bin/env python3
"""
從 citations 的 match_start/end 切出 snippet-adjacent chunks，寫入 local PostgreSQL。

此表只存本機 DB，不同步 RDS。後續 index script 讀此表 → embed → 推 OS。

Usage:
  python etl/build_citation_chunks.py                # 全量處理
  python etl/build_citation_chunks.py --resume        # 跳過已處理的 decision
  python etl/build_citation_chunks.py --decision-id 12345  # 單一 decision
  python etl/build_citation_chunks.py --batch-size 500     # 每批 commit 500 筆
"""

import argparse
import os
import re
import sys
import time
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

FOOTER_RE = re.compile(r'中\s*華\s*民\s*國')

# 理由段標題（用於設定 chunk_start 下限）
REASONING_RE = re.compile(
    r'\n\s*(?:犯罪事實及|事實及|事實、)?理由(?:要領)?(?:\s*\n|\s*：)'
)
DISPOSITION_RE = re.compile(r'\n\s*主\s*文\s*\n')


# ── Chunking 邏輯 ─────────────────────────────────────────────────────────

def find_reasoning_floor(text: str) -> int:
    """找出理由段標題之後的位置，作為 chunk_start 下限。找不到則 fallback 到主文，再找不到回傳 0。"""
    m = REASONING_RE.search(text)
    if m:
        return m.end()
    m = DISPOSITION_RE.search(text)
    if m:
        return m.end()
    return 0


def find_section_markers(text: str) -> list[int]:
    """找出 text 中所有小節符的 offset（指向 marker 字元本身，不含前導 \\n）。"""
    result = []
    for m in SECTION_RE.finditer(text):
        pos = m.start()
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
    search_from = max(0, match_start - len(snippet) - 50)
    search_to = min(len(text), match_start + 50)
    prefix = snippet[:min(80, len(snippet))]
    pos = text.find(prefix, search_from, search_to)
    if pos >= 0:
        return pos, pos + len(snippet)
    return match_start, match_start


def _apply_bounds(chunk_start: int, chunk_end: int,
                  reasoning_floor: int, footer_pos: int | None,
                  must_start: int, must_end: int) -> tuple[int, int]:
    """統一套用 reasoning floor 和 footer 限制。"""
    if chunk_start < reasoning_floor <= must_start:
        chunk_start = reasoning_floor
    if footer_pos is not None and chunk_end > footer_pos and must_end <= footer_pos:
        chunk_end = footer_pos
    return chunk_start, chunk_end


def find_chunk_bounds(text: str, match_start: int, match_end: int,
                      markers: list[int],
                      snippet: str | None = None,
                      footer_pos: int | None = None,
                      reasoning_floor: int = 0) -> tuple[int, int]:
    """
    根據小節符找出 chunk 的 [start, end) 邊界。

    保證：
    - citation (match_start:match_end) 在 chunk 內
    - snippet 全文在 chunk 內（若可定位）
    - chunk <= MAX_CHUNK_LEN
    - 邊界偏好：小節符 > 句號(。) > 硬切
    - chunk 不超過 footer（中華民國...）
    - chunk 不早於 reasoning_floor（理由段標題之後）
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
        return _apply_bounds(chunk_start, chunk_end, reasoning_floor, footer_pos, must_start, must_end)

    # 4. 太長 → 用。作為 fallback 起點
    budget_before = MAX_CHUNK_LEN - (must_end - must_start)
    budget_before = max(budget_before // 2, 50)

    period_search_from = max(0, must_start - budget_before)
    period_pos = text.rfind('。', period_search_from, must_start)
    if period_pos >= 0:
        chunk_start = period_pos + 1
    else:
        chunk_start = max(0, must_start - budget_before)

    # 5. 後端：從 must_end 往後找最近的。或小節符
    remaining = MAX_CHUNK_LEN - (must_end - chunk_start)
    if remaining > 0:
        after_close = [m for m in after if m <= must_end + remaining]
        if after_close:
            chunk_end = after_close[0]
        else:
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

    return _apply_bounds(chunk_start, chunk_end, reasoning_floor, footer_pos, must_start, must_end)


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


# ── DB ─────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS citation_chunks (
  id                  BIGSERIAL PRIMARY KEY,
  decision_id         BIGINT NOT NULL,
  citation_id         BIGINT NOT NULL,
  target_id           BIGINT,
  target_authority_id BIGINT,
  chunk_index         INT NOT NULL,
  start_offset        INT NOT NULL,
  end_offset          INT NOT NULL,
  chunk_text          TEXT NOT NULL,
  case_type           TEXT,
  created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS cc_decision_citation_uniq
  ON citation_chunks(decision_id, citation_id);

CREATE INDEX IF NOT EXISTS cc_decision_chunk_idx
  ON citation_chunks(decision_id, chunk_index);
"""


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def get_db_connection():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(db_url, row_factory=dict_row)


# ── 處理邏輯 ───────────────────────────────────────────────────────────────

def process_decision(conn, decision_id: int) -> int:
    """處理一篇 decision，寫入 citation_chunks。回傳寫入筆數。"""
    d = conn.execute(
        "SELECT id, case_type, clean_text "
        "FROM decisions WHERE id = %s", (decision_id,)
    ).fetchone()
    if not d or not d["clean_text"]:
        return 0

    text = d["clean_text"]
    case_type = d["case_type"]

    cites = conn.execute(
        "SELECT id, match_start, match_end, target_id, target_authority_id, snippet "
        "FROM citations WHERE source_id = %s AND match_start IS NOT NULL "
        "ORDER BY match_start",
        (decision_id,)
    ).fetchall()
    if not cites:
        return 0

    markers = find_section_markers(text)
    footer_pos = find_footer_start(text)
    reasoning_floor = find_reasoning_floor(text)

    # 每個 citation 的 chunk 邊界
    items = []
    for c in cites:
        cs, ce = find_chunk_bounds(text, c["match_start"], c["match_end"],
                                   markers, snippet=c["snippet"],
                                   footer_pos=footer_pos,
                                   reasoning_floor=reasoning_floor)
        items.append((c, cs, ce))

    # 合併重疊
    merged = merge_overlapping(items)

    # 寫入 DB
    count = 0
    with conn.cursor() as cur:
        for chunk_idx, (cs, ce, chunk_cites) in enumerate(merged):
            chunk_text = text[cs:ce]
            for c in chunk_cites:
                cur.execute("""
                    INSERT INTO citation_chunks
                        (decision_id, citation_id, target_id, target_authority_id,
                         chunk_index, start_offset, end_offset, chunk_text, case_type)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (decision_id, citation_id) DO UPDATE SET
                        target_id = EXCLUDED.target_id,
                        target_authority_id = EXCLUDED.target_authority_id,
                        chunk_index = EXCLUDED.chunk_index,
                        start_offset = EXCLUDED.start_offset,
                        end_offset = EXCLUDED.end_offset,
                        chunk_text = EXCLUDED.chunk_text,
                        case_type = EXCLUDED.case_type
                """, (decision_id, c["id"], c["target_id"], c["target_authority_id"],
                      chunk_idx, cs, ce, chunk_text, case_type))
                count += 1

    return count


def main():
    parser = argparse.ArgumentParser(
        description="從 citations 的 match_start/end 切出 snippet-adjacent chunks")
    parser.add_argument("--resume", action="store_true",
                        help="跳過已有 citation_chunks 的 decision")
    parser.add_argument("--decision-id", type=int,
                        help="只處理指定 decision_id")
    parser.add_argument("--batch-size", type=int, default=200,
                        help="每幾筆 decision commit 一次 (default: 200)")
    args = parser.parse_args()

    conn = get_db_connection()
    ensure_table(conn)

    if args.decision_id:
        count = process_decision(conn, args.decision_id)
        conn.commit()
        print(f"decision_id={args.decision_id}: {count} rows")
        conn.close()
        return

    # 全量：找所有有 positioned citations 的 source decisions
    if args.resume:
        source_query = """
            SELECT DISTINCT c.source_id
            FROM citations c
            JOIN decisions d ON d.id = c.source_id
            WHERE c.match_start IS NOT NULL
              AND d.clean_text IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM citation_chunks cc WHERE cc.decision_id = c.source_id
              )
            ORDER BY c.source_id
        """
    else:
        # 全量重建：先清空
        with conn.cursor() as cur:
            cur.execute("TRUNCATE citation_chunks")
        conn.commit()
        source_query = """
            SELECT DISTINCT c.source_id
            FROM citations c
            JOIN decisions d ON d.id = c.source_id
            WHERE c.match_start IS NOT NULL
              AND d.clean_text IS NOT NULL
            ORDER BY c.source_id
        """

    sources = conn.execute(source_query).fetchall()
    total = len(sources)
    print(f"待處理: {total} decisions")

    t0 = time.time()
    total_rows = 0
    errors = 0

    for i, row in enumerate(sources):
        sid = row["source_id"]
        try:
            n = process_decision(conn, sid)
            total_rows += n
        except Exception as e:
            conn.rollback()
            errors += 1
            if errors <= 10:
                print(f"  ERROR decision_id={sid}: {e}", file=sys.stderr)
            continue

        if (i + 1) % args.batch_size == 0:
            conn.commit()
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            eta = (total - i - 1) / rate if rate > 0 else 0
            print(f"  [{i+1}/{total}] {total_rows} rows, "
                  f"{rate:.0f} decisions/s, ETA {eta:.0f}s")

    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print(f"\n--- 完成 ---")
    print(f"Decisions: {total}, Rows: {total_rows}, Errors: {errors}")
    print(f"Time: {elapsed:.1f}s ({total/elapsed:.0f} decisions/s)")


if __name__ == "__main__":
    main()
