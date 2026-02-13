"""
法條抽取腳本

功能：
  --decisions  : backfill decision_reason_statutes（從 decisions.clean_text）
  --citations  : backfill citation_snippet_statutes（從 citations.snippet）
  --all        : 兩者都跑（預設）

使用方式：
  python etl/extract_statutes.py           # 等同 --all
  python etl/extract_statutes.py --decisions
  python etl/extract_statutes.py --citations
"""
import sys
from pathlib import Path

import psycopg
from statute_parser import extract_statutes

DB_URL = "postgresql://postgres:postgres@localhost:5432/citations"


def get_conn():
    return psycopg.connect(DB_URL)


# =========================
# decision_reason_statutes
# =========================
def ingest_decision_statutes(conn, decision_id: int, clean_text: str) -> int:
    """
    從 clean_text 抽取法條，寫入 decision_reason_statutes。
    Returns: 寫入筆數
    """
    statutes = extract_statutes(clean_text)
    inserted = 0
    for law, article_raw, raw in statutes:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO decision_reason_statutes (decision_id, law, article_raw, raw_match)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (decision_id, law, article_raw) DO NOTHING
                """, (decision_id, law, article_raw, raw))
                conn.commit()
                inserted += 1
        except Exception as e:
            print(f"  錯誤：decision_id={decision_id} {law}第{article_raw}條 - {e}")
            conn.rollback()
    return inserted


def backfill_decisions(conn):
    """處理所有有 clean_text 但尚未抽取法條的判決"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT d.id, d.clean_text
            FROM decisions d
            WHERE d.clean_text IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM decision_reason_statutes s
                WHERE s.decision_id = d.id
              )
            ORDER BY d.id
        """)
        rows = cur.fetchall()

    print(f"待處理判決：{len(rows)} 筆")
    total = 0
    for i, (decision_id, clean_text) in enumerate(rows, 1):
        n = ingest_decision_statutes(conn, decision_id, clean_text)
        total += n
        if i % 100 == 0:
            print(f"  進度：{i}/{len(rows)}，累計 {total} 筆法條")

    print(f"decision_reason_statutes 完成，共寫入 {total} 筆")
    return total


# =========================
# citation_snippet_statutes
# =========================
def ingest_citation_statutes(conn, citation_id: int, snippet: str) -> int:
    """
    從 snippet 抽取法條，寫入 citation_snippet_statutes。
    Returns: 寫入筆數
    """
    statutes = extract_statutes(snippet)
    inserted = 0
    for law, article_raw, raw in statutes:
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO citation_snippet_statutes (citation_id, law, article_raw, raw_match)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (citation_id, law, article_raw) DO NOTHING
                """, (citation_id, law, article_raw, raw))
                conn.commit()
                inserted += 1
        except Exception as e:
            print(f"  錯誤：citation_id={citation_id} {law}第{article_raw}條 - {e}")
            conn.rollback()
    return inserted


def backfill_citations(conn):
    """處理所有有 snippet 但尚未抽取法條的 citation"""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.snippet
            FROM citations c
            WHERE c.snippet IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM citation_snippet_statutes s
                WHERE s.citation_id = c.id
              )
            ORDER BY c.id
        """)
        rows = cur.fetchall()

    print(f"待處理 citation：{len(rows)} 筆")
    total = 0
    for i, (citation_id, snippet) in enumerate(rows, 1):
        n = ingest_citation_statutes(conn, citation_id, snippet)
        total += n
        if i % 200 == 0:
            print(f"  進度：{i}/{len(rows)}，累計 {total} 筆法條")

    print(f"citation_snippet_statutes 完成，共寫入 {total} 筆")
    return total


# =========================
# 主程式
# =========================
def main():
    args = set(sys.argv[1:])
    do_decisions = '--decisions' in args or '--all' in args or not args
    do_citations = '--citations' in args or '--all' in args or not args

    conn = get_conn()
    print("✓ DB 連線成功")

    if do_decisions:
        print("\n--- decision_reason_statutes ---")
        backfill_decisions(conn)

    if do_citations:
        print("\n--- citation_snippet_statutes ---")
        backfill_citations(conn)

    conn.close()
    print("\n完成")


if __name__ == '__main__':
    main()
