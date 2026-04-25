#!/usr/bin/env python3
"""
最高法院 / 最高行政法院判決全文切 chunk，寫入 chunks（chunk_type='supreme_reasoning'）。

與 build_chunks.py 不同：
  - 不需要 citation 錨點，直接切理由段全文
  - 起始位置：「得心證之理由」>「本院判斷」>「惟查」>「經查」>「理由」標題
  - chunk_type = 'supreme_reasoning'

Usage:
  python etl/build_supreme_chunks.py                      # 全量
  python etl/build_supreme_chunks.py --resume              # 跳過已處理的 decision
  python etl/build_supreme_chunks.py --decision-id 12345   # 單一 decision
  python etl/build_supreme_chunks.py --month 2024-01       # 只處理特定月份
  python etl/build_supreme_chunks.py --batch-size 500
"""

import argparse
import datetime
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

# 小節符 pattern（與 build_chunks.py 共用）
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

# 大標題 pattern（supreme chunk 專用）
# 匹配 一、二、三、... 或 壹、貳、參、...（含前導換行/空白）
MAJOR_SECTION_RE = re.compile(
    r'(?:^|\n)[ \t\u3000]*(?:'
    r'[一二三四五六七八九十百]+、'
    r'|[壹貳參肆伍陸柒捌玖拾]+、'
    r')'
)

FOOTER_RE = re.compile(r'中\s*華\s*民\s*國')

# 法院論述起始標記（依優先順序：具體 > 通用）
COURT_ANALYSIS_RE = re.compile(
    r'(?:得心證之理由|得心證的理由|本院之判斷|本院的判斷|本院判斷|惟查|經查)'
)

# 廣泛的理由段標題（fallback）
REASONING_RE = re.compile(
    r'\n\s*(?:犯罪事實及|事實及|事實、)?理由(?:要領)?(?:\s*\n|\s*：)'
)


# ── Chunking 邏輯 ─────────────────────────────────────────────────────────

def find_analysis_floor(text: str) -> int:
    """找法院分析起始位置。
    優先：「得心證之理由」「本院判斷」「惟查」「經查」等具體標記。
    Fallback：「理由」大標。
    """
    m = COURT_ANALYSIS_RE.search(text)
    if m:
        # 回退到該行的行首
        line_start = text.rfind('\n', 0, m.start())
        return line_start + 1 if line_start >= 0 else m.start()

    m = REASONING_RE.search(text)
    if m:
        return m.end()

    return 0


def find_section_markers(text: str) -> list[int]:
    """找出 text 中所有小節符的 offset（跳過前導 \\n 與空白）。"""
    result = []
    for m in SECTION_RE.finditer(text):
        pos = m.start()
        if pos < len(text) and text[pos] == '\n':
            pos += 1
        while pos < m.end() and text[pos] in ' \t\u3000':
            pos += 1
        result.append(pos)
    return result


def find_major_section_markers(text: str) -> list[int]:
    """找出 text 中所有大標題（一、二、壹、貳、）的 offset（跳過前導 \\n 與空白）。"""
    result = []
    for m in MAJOR_SECTION_RE.finditer(text):
        pos = m.start()
        if pos < len(text) and text[pos] == '\n':
            pos += 1
        while pos < m.end() and text[pos] in ' \t\u3000':
            pos += 1
        result.append(pos)
    return result


def find_footer_start(text: str) -> int | None:
    """找出判決書尾端 '中　華　民　國' 的位置（限最後 500 字）。"""
    search_from = max(0, len(text) - 500)
    m = FOOTER_RE.search(text, search_from)
    return m.start() if m else None


def chunk_text_by_sections(text: str, start: int, end: int) -> list[tuple[int, int]]:
    """
    在 [start, end) 範圍內用大標題切 chunk。
    短文直接一個 chunk；長文依大標題拆分；超長段落以句號硬切。
    """
    if end <= start:
        return []

    span = text[start:end].rstrip()
    span_len = len(span)

    # 整段 <= MAX_CHUNK_LEN → 單一 chunk
    if span_len <= MAX_CHUNK_LEN:
        return [(start, start + span_len)]

    # 找大標題位置（相對於 span 起始）
    markers = find_major_section_markers(span)

    chunks = []
    chunk_start = 0

    for marker_pos in markers:
        if marker_pos <= chunk_start:
            continue
        if marker_pos - chunk_start > MAX_CHUNK_LEN:
            # 目前段太長，用句號硬切
            chunks.extend(
                _hard_split(span, chunk_start, marker_pos, MAX_CHUNK_LEN)
            )
            chunk_start = marker_pos
        elif marker_pos - chunk_start > MAX_CHUNK_LEN * 0.6:
            # 接近上限，在此切
            chunks.append((chunk_start, marker_pos))
            chunk_start = marker_pos

    # 最後一段
    if chunk_start < span_len:
        remaining = span_len - chunk_start
        if remaining > MAX_CHUNK_LEN:
            chunks.extend(
                _hard_split(span, chunk_start, span_len, MAX_CHUNK_LEN)
            )
        else:
            chunks.append((chunk_start, span_len))

    # 轉換回原始 text offset
    return [(start + s, start + e) for s, e in chunks]


def _hard_split(text: str, start: int, end: int, max_len: int) -> list[tuple[int, int]]:
    """在 [start, end) 內以句號（。）為切點，每段不超過 max_len。"""
    result = []
    pos = start
    while pos < end:
        if end - pos <= max_len:
            result.append((pos, end))
            break
        # 在 [pos, pos+max_len] 範圍內找最後一個句號
        search_end = min(pos + max_len, end)
        cut = text.rfind('。', pos, search_end)
        if cut <= pos:
            cut = search_end  # 沒有句號，硬切
        else:
            cut += 1  # 包含句號
        result.append((pos, cut))
        pos = cut
    return result


# ── DB ─────────────────────────────────────────────────────────────────────

def get_db_conn():
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(db_url, row_factory=dict_row)


def process_decision(conn, decision: dict) -> int:
    """處理一筆最高法院判決，回傳產出的 chunk 數。"""
    text = decision["clean_text"]
    if not text or len(text) < 50:
        return 0

    floor = find_analysis_floor(text)
    footer = find_footer_start(text)
    end = footer if footer and footer > floor else len(text)

    chunks = chunk_text_by_sections(text, floor, end)
    if not chunks:
        return 0

    rows = []
    for idx, (s, e) in enumerate(chunks):
        chunk_text = text[s:e].strip()
        if len(chunk_text) < 150:
            continue
        rows.append((
            decision["id"],
            idx,
            s,
            e,
            chunk_text,
            decision["case_type"],
        ))

    if not rows:
        return 0

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO chunks
                (decision_id, chunk_index, start_offset, end_offset,
                 chunk_text, case_type, chunk_type)
            VALUES (%s, %s, %s, %s, %s, %s, 'supreme')
            ON CONFLICT (decision_id, chunk_index)
                WHERE chunk_type = 'supreme_reasoning'
            DO UPDATE SET
                start_offset = EXCLUDED.start_offset,
                end_offset   = EXCLUDED.end_offset,
                chunk_text   = EXCLUDED.chunk_text,
                case_type    = EXCLUDED.case_type,
                embedding    = NULL
        """, rows)

    return len(rows)


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build supreme court reasoning chunks"
    )
    parser.add_argument("--resume",       action="store_true",
                        help="跳過已有 supreme chunk 的 decision")
    parser.add_argument("--decision-id",  type=int,
                        help="只處理特定 decision")
    parser.add_argument("--month",        type=str,
                        help="只處理特定月份的判決，格式 YYYY-MM")
    parser.add_argument("--batch-size",   type=int, default=500)
    args = parser.parse_args()

    if args.month:
        try:
            year, mon = int(args.month[:4]), int(args.month[5:7])
            month_from = datetime.date(year, mon, 1)
            month_to   = datetime.date(year + mon // 12, mon % 12 + 1, 1)
        except (ValueError, IndexError):
            print(f"ERROR: --month 格式錯誤，應為 YYYY-MM，收到：{args.month!r}")
            sys.exit(1)
    else:
        month_from = month_to = None

    conn = get_db_conn()

    if args.decision_id:
        row = conn.execute("""
            SELECT id, clean_text, case_type
            FROM decisions
            WHERE id = %s
        """, (args.decision_id,)).fetchone()
        if not row:
            print(f"Decision {args.decision_id} not found")
            sys.exit(1)
        n = process_decision(conn, dict(row))
        conn.commit()
        print(f"Decision {args.decision_id}: {n} chunks")
        return

    # 全量或 resume（有 --month 時只刪該月份）
    if not args.resume:
        if month_from:
            print(f"Deleting supreme chunks for {args.month}...")
            conn.execute("""
                DELETE FROM chunks
                WHERE chunk_type = 'supreme_reasoning'
                  AND decision_id IN (
                      SELECT id FROM decisions
                      WHERE root_norm IN ('最高法院', '最高行政法院')
                        AND decision_date >= %(month_from)s
                        AND decision_date < %(month_to)s
                  )
            """, {"month_from": month_from, "month_to": month_to})
        else:
            print("Deleting existing supreme chunks...")
            conn.execute("DELETE FROM chunks WHERE chunk_type = 'supreme_reasoning'")
        conn.commit()

    # 查詢最高法院 + 最高行政法院
    extra = []
    extra_params: dict = {}

    if args.resume:
        extra.append("""
            d.id NOT IN (
                SELECT DISTINCT decision_id FROM chunks
                WHERE chunk_type = 'supreme_reasoning'
            )
        """)

    if month_from:
        extra.append("d.decision_date >= %(month_from)s AND d.decision_date < %(month_to)s")
        extra_params["month_from"] = month_from
        extra_params["month_to"]   = month_to

    extra_sql = ("AND " + " AND ".join(extra)) if extra else ""

    total = conn.execute(f"""
        SELECT COUNT(*) AS cnt FROM decisions d
        WHERE d.root_norm IN ('最高法院', '最高行政法院')
          AND d.jid IS NOT NULL
          AND d.clean_text IS NOT NULL
          AND length(d.clean_text) > 500
          {extra_sql}
    """, extra_params).fetchone()["cnt"]
    print(f"Decisions to process: {total}"
          + (f"  (month: {args.month})" if args.month else ""))

    cursor = conn.execute(f"""
        SELECT d.id, d.clean_text, d.case_type
        FROM decisions d
        WHERE d.root_norm IN ('最高法院', '最高行政法院')
          AND d.jid IS NOT NULL
          AND d.clean_text IS NOT NULL
          AND length(d.clean_text) > 500
          {extra_sql}
        ORDER BY d.id
    """, extra_params)

    t0 = time.time()
    processed = 0
    total_chunks = 0
    skipped = 0
    batch_count = 0

    for row in cursor:
        n = process_decision(conn, dict(row))
        if n > 0:
            total_chunks += n
        else:
            skipped += 1
        processed += 1
        batch_count += 1

        if batch_count >= args.batch_size:
            conn.commit()
            batch_count = 0
            elapsed = time.time() - t0
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            print(f"  {processed}/{total} decisions, "
                  f"{total_chunks} chunks, {skipped} skipped, "
                  f"{rate:.0f}/s, ETA {eta:.0f}s")

    conn.commit()
    conn.close()

    elapsed = time.time() - t0
    print(f"\n--- 完成 ---")
    print(f"Decisions: {processed}, Chunks: {total_chunks}, Skipped: {skipped}")
    if elapsed > 0:
        print(f"Time: {elapsed:.1f}s ({processed/elapsed:.0f} decisions/s)")


if __name__ == "__main__":
    main()
