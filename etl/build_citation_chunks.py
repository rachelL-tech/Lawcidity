#!/usr/bin/env python3
"""
從 citations 的 match_start/end 切出 snippet-adjacent chunks，寫入 local PostgreSQL。

此表只存本機 DB，不同步 RDS。後續 index script 讀此表 → embed → 推 OS。

Usage:
  python etl/build_chunks.py                # 全量處理（會先清空 chunks）
  python etl/build_chunks.py --resume        # 跳過已處理的 decision
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
BATCH_SIZE = 200

# 小節符 pattern（台灣法律文書常見）
# minor markers 要求行首（\n 或文首），避免句中誤判
SECTION_RE = re.compile(
    r'(?:^|\n)[ \t\u3000]*(?:'
    r'[㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩]'
    r'|[⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑⒒⒓⒔⒕⒖⒗⒘⒙⒚⒛]'
    r'|[①②③④⑤⑥⑦⑧⑨⑩⑪⑫⑬⑭⑮⑯⑰⑱⑲⑳]'
    r'|[⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽]'
    r'|[一二三四五六七八九十]+、'
    r'|[（(][一二三四五六七八九十]+[）)]'
    r'|[0-9０-９]{1,3}[.、](?![0-9０-９])'
    r')'
)

FOOTER_RE = re.compile(r'中\s*華\s*民\s*國')

# 理由段標題（用於設定 chunk_start 下限）
REASONING_RE = re.compile(
    r'\n\s*(?:犯罪事實及|事實及|事實、)?理由(?:要領)?(?:\s*\n|\s*：)'
)
DISPOSITION_RE = re.compile(r'\n\s*主\s*文\s*\n')

# 當事人欄（裁定等無理由/主文標題的文書用）
# 比對格式：\n  原  告  王小明\n
# 用 (?<=\n) lookbehind 而非消耗 \n，避免原告/被告等連續行只 match 第一行
PARTIES_RE = re.compile(
    r'(?<=\n)[ \t\u3000]*(?:原\s*告|被\s*告|上\s{0,2}訴\s{0,2}人|被\s*上\s*訴\s*人|'
    r'抗\s*告\s*人|相\s*對\s*人|聲\s*請\s*人|再\s*抗\s*告\s*人|'
    r'受\s*刑\s*人|債\s*務\s*人|債\s*權\s*人|異\s*議\s*人)[^\n]*\n'
)


# ── Chunking 邏輯 ─────────────────────────────────────────────────────────

def find_reasoning_floor(text: str) -> int:
    """找出理由段標題之後的位置，作為 chunk_start 下限。
    fallback 順序：理由標題 → 主文 → 當事人欄末行（裁定等短文書）→ 0
    """
    m = REASONING_RE.search(text)
    if m:
        return m.end()
    m = DISPOSITION_RE.search(text)
    if m:
        return m.end()
    # 裁定等無標題文書：找當事人欄最後一行，限搜索前 20% 或前 600 字
    search_limit = min(len(text), max(600, len(text) // 5))
    last_party = None
    for m in PARTIES_RE.finditer(text, 0, search_limit):
        last_party = m
    if last_party:
        # 跳過接續行（僅有縮排 + 名字，無角色關鍵字，例如「　　　彭春嬌」）
        pos = last_party.end()
        continuation = re.compile(r'[ \t\u3000]+[^\n]*\r?\n')
        while pos < search_limit:
            m = continuation.match(text, pos)
            if m:
                pos = m.end()
            else:
                break
        return pos
    return 0


def find_section_markers(text: str) -> list[int]:
    """找出 text 中所有小節符的 offset（指向 marker 字元本身，跳過前導 \\n 與空白）。"""
    result = []
    for m in SECTION_RE.finditer(text):
        pos = m.start()
        if text[pos] == '\n':
            pos += 1
        # 跳過縮排空白（如「  ㈡次按」的 2 個空格），指向 marker 字元本身
        while pos < m.end() and text[pos] in ' \t\u3000':
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
    if footer_pos is not None and must_end <= footer_pos < chunk_end:
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

    # 2. 嘗試用小節符（含 must_start 本身，避免 snippet 以小節符開頭時往前退一格）
    before = [m for m in markers if m <= must_start]
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
    period_pos = text.find('。', period_search_from, must_start)  # 最遠的句號（最多 context）
    if period_pos >= 0:
        chunk_start = period_pos + 1
    else:
        chunk_start = max(0, must_start - budget_before)

    # 5. 後端：從 must_end 往後找最遠的。或小節符
    remaining = MAX_CHUNK_LEN - (must_end - chunk_start)
    budget_end = must_end + remaining
    if remaining > 0:
        after_close = [m for m in after if m <= budget_end]
        if after_close:
            chunk_end = after_close[0]  # 最近的小節符
        else:
            period_after = text.rfind('。', must_end, budget_end)  # 最遠的句號（最多 context）
            if period_after >= 0:
                chunk_end = period_after + 1
            else:
                chunk_end = min(len(text), budget_end)
    else:
        chunk_end = must_end

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
    current_start, current_end, current_cites = items[0][1], items[0][2], [items[0][0]]

    for c, s, e in items[1:]:
        if s < current_end:  # overlap
            current_end = max(current_end, e)
            current_cites.append(c)
        else:
            merged.append((current_start, current_end, current_cites))
            current_start, current_end, current_cites = s, e, [c]

    merged.append((current_start, current_end, current_cites))
    return merged


# ── DB ─────────────────────────────────────────────────────────────────────

def get_db_connection():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(db_url, row_factory=dict_row)


# ── 處理邏輯 ───────────────────────────────────────────────────────────────

def process_decision(conn, decision_id: int) -> int:
    """處理一篇 decision，寫入 chunks。回傳寫入筆數。"""
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
        "FROM citations WHERE source_id = %s "
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
                    INSERT INTO chunks
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


def _parse_year_month(ym: str) -> tuple[str, str]:
    """'202501' → ('2025-01-01', '2025-02-01')"""
    if len(ym) != 6 or not ym.isdigit():
        raise ValueError(f"--year-month 格式應為 YYYYMM，例如 202501，got: {ym}")
    year, month = int(ym[:4]), int(ym[4:])
    date_from = f"{year:04d}-{month:02d}-01"
    if month == 12:
        date_to = f"{year + 1:04d}-01-01"
    else:
        date_to = f"{year:04d}-{month + 1:02d}-01"
    return date_from, date_to


def main():
    parser = argparse.ArgumentParser(
        description="從 citations 的 match_start/end 切出 snippet-adjacent chunks")
    parser.add_argument("--resume", action="store_true",
                        help="跳過已有 chunks 的 decision")
    parser.add_argument("--year-month", type=str,
                        help="只處理指定月份的 decision（格式：YYYYMM，例如 202501）")
    args = parser.parse_args()

    conn = get_db_connection()

    # 月份 filter
    date_filter = ""
    query_params: dict = {}
    if args.year_month:
        date_from, date_to = _parse_year_month(args.year_month)
        date_filter = "AND d.decision_date >= %(date_from)s AND d.decision_date < %(date_to)s"
        query_params = {"date_from": date_from, "date_to": date_to}
        print(f"月份 filter：{date_from} ~ {date_to}")

    # 全量：找所有有 citations 的 source decisions
    if args.resume:
        source_query = f"""
            SELECT DISTINCT c.source_id
            FROM citations c
            JOIN decisions d ON d.id = c.source_id
            WHERE d.clean_text IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM chunks cc WHERE cc.decision_id = c.source_id
              )
              {date_filter}
            ORDER BY c.source_id
        """
    elif args.year_month:
        # 月份模式：不 TRUNCATE 整張表，只刪除該月份的舊 chunks
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM chunks
                WHERE decision_id IN (
                    SELECT id FROM decisions
                    WHERE decision_date >= %(date_from)s AND decision_date < %(date_to)s
                )
            """, query_params)
            deleted = cur.rowcount
        conn.commit()
        print(f"已清除該月份舊 chunks：{deleted} rows")
        source_query = f"""
            SELECT DISTINCT c.source_id
            FROM citations c
            JOIN decisions d ON d.id = c.source_id
            WHERE d.clean_text IS NOT NULL
              {date_filter}
            ORDER BY c.source_id
        """
    else:
        # 全量重建：先清空
        with conn.cursor() as cur:
            cur.execute("TRUNCATE chunks")
        conn.commit()
        source_query = """
            SELECT DISTINCT c.source_id
            FROM citations c
            JOIN decisions d ON d.id = c.source_id
            WHERE d.clean_text IS NOT NULL
            ORDER BY c.source_id
        """

    sources = conn.execute(source_query, query_params).fetchall()
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

        if (i + 1) % BATCH_SIZE == 0:
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
