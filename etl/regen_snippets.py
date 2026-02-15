"""
重新計算 citations.snippet（不重跑 ingest，只更新 snippet 欄位）

使用場景：citation_parser.py 的 extract_snippet() 邏輯更新後，
在不重新匯入判決的前提下，批次更新所有既有 citation 的 snippet。
"""
import sys
import psycopg
sys.path.insert(0, __file__.rsplit('/', 1)[0])

import re
from citation_parser import extract_snippet, preprocess_text

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "citations",
    "user": "postgres",
    "password": "postgres",
}


def main():
    conn_str = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    conn = psycopg.connect(conn_str)

    # 查出所有 citation，連同 raw_match 和來源 clean_text
    # match_start IS NULL 表示當初 PDF 折行導致反查失敗，這次也一起補救
    with conn.cursor() as cur:
        cur.execute("""
            SELECT c.id, c.match_start, c.match_end, c.raw_match,
                   d.clean_text
            FROM citations c
            JOIN decisions d ON d.id = c.source_id
            WHERE d.clean_text IS NOT NULL
        """)
        rows = cur.fetchall()

    print(f"共 {len(rows)} 筆 citation 需要重算 snippet")

    updated = 0
    skipped = 0
    recovered = 0  # 原本 match_start IS NULL、這次成功定位的

    with conn.cursor() as cur:
        for i, (cid, start, end, raw_match, clean_text) in enumerate(rows):
            try:
                # match_start IS NULL：PDF 折行，用 flexible pattern 重新定位
                if start is None and raw_match:
                    flexible = r'[\s\r\n]*'.join(re.escape(c) for c in raw_match)
                    flex = re.search(flexible, clean_text)
                    if flex:
                        start, end = flex.start(), flex.end()
                        cur.execute(
                            "UPDATE citations SET match_start=%s, match_end=%s WHERE id=%s",
                            (start, end, cid)
                        )
                        recovered += 1

                if start is None:
                    skipped += 1
                    continue

                new_snippet = extract_snippet(clean_text, start, end)
                cur.execute(
                    "UPDATE citations SET snippet = %s WHERE id = %s",
                    (new_snippet, cid)
                )
                updated += 1
            except Exception as e:
                print(f"  跳過 citation #{cid}：{e}")
                skipped += 1

            if (i + 1) % 500 == 0:
                conn.commit()
                print(f"  進度：{i + 1}/{len(rows)}")

    conn.commit()
    conn.close()
    print(f"\n完成！更新 {updated} 筆，補救 match_start {recovered} 筆，跳過 {skipped} 筆")


if __name__ == "__main__":
    main()
