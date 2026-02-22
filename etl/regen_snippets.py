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
            SELECT c.id, c.source_id, c.target_id, c.target_authority_id,
                   c.match_start, c.match_end, c.raw_match, d.clean_text
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
        for i, (cid, source_id, target_id, target_authority_id, start, end, raw_match, clean_text) in enumerate(rows):
            try:
                # stale match_start：offset 超出 clean_text 長度（舊版匯入殘留）→ 重置為 NULL
                if start is not None and start >= len(clean_text):
                    start = end = None
                    cur.execute(
                        "UPDATE citations SET match_start=NULL, match_end=NULL WHERE id=%s",
                        (cid,)
                    )

                # match_start IS NULL：PDF 折行，用 flexible pattern 重新定位
                if start is None and raw_match:
                    flexible = r'[\s\r\n]*'.join(re.escape(c) for c in raw_match)
                    flex = re.search(flexible, clean_text)
                    if flex:
                        new_start, new_end = flex.start(), flex.end()
                        # 位置已被同一（source, target）的其他 citation 佔用 → 這筆是重複，直接刪除
                        cur.execute(
                            "SELECT 1 FROM citations"
                            " WHERE source_id=%s"
                            " AND (target_id IS NOT DISTINCT FROM %s)"
                            " AND (target_authority_id IS NOT DISTINCT FROM %s)"
                            " AND match_start=%s AND id!=%s",
                            (source_id, target_id, target_authority_id, new_start, cid)
                        )
                        if cur.fetchone() is not None:
                            cur.execute("DELETE FROM citations WHERE id=%s", (cid,))
                            skipped += 1
                            continue
                        else:
                            start, end = new_start, new_end
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
                conn.rollback()  # 清除 aborted transaction，避免 cascade 失敗
                skipped += 1

            if (i + 1) % 500 == 0:
                conn.commit()
                print(f"  進度：{i + 1}/{len(rows)}")

    conn.commit()
    conn.close()
    print(f"\n完成！更新 {updated} 筆，補救 match_start {recovered} 筆，跳過 {skipped} 筆")

    # 重建被 cascade 刪除的 citation_snippet_statutes
    import os, subprocess
    script = os.path.join(os.path.dirname(__file__), "extract_statutes.py")
    print("\n重建 citation_snippet_statutes...")
    subprocess.run(["python", script, "--citations"], check=True)


if __name__ == "__main__":
    main()
