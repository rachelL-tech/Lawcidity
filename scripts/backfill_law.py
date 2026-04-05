"""
補跑特定法律名稱的法條抽取（快速版：COPY + temp table）

用法：
  python backfill_law.py 刑法
  python backfill_law.py 刑法 --db postgresql://user:pass@rds-host:5432/citations
"""
import sys, io, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'etl'))

import psycopg
from statute_parser import extract_statutes

DEFAULT_DB = "postgresql://postgres:postgres@localhost:5432/citations"


def backfill(conn, law_target: str):
    cur = conn.cursor()

    # --- decision_reason_statutes ---
    print(f"[DRS] 查詢含「{law_target}」的判決...")
    cur.execute("SELECT id, clean_text FROM decisions WHERE clean_text LIKE %s ORDER BY id",
                (f"%{law_target}%",))
    rows = cur.fetchall()
    print(f"[DRS] 共 {len(rows)} 筆判決")

    buf = io.StringIO()
    count = 0
    for did, text in rows:
        for law, art, sub, raw in extract_statutes(text):
            if law == law_target:
                # tab-separated, escape tabs/newlines in raw_match
                raw_esc = raw.replace("\\", "\\\\").replace("\t", " ").replace("\r", " ").replace("\n", " ")
                buf.write(f"{did}\t{law}\t{art}\t{sub}\t{raw_esc}\n")
                count += 1
    print(f"[DRS] 解析出 {count} 筆，開始 COPY...")

    cur.execute("CREATE TEMP TABLE _tmp_drs (decision_id int, law text, article_raw text, sub_ref text, raw_match text)")
    buf.seek(0)
    with cur.copy("COPY _tmp_drs FROM STDIN") as copy:
        copy.write(buf.getvalue().encode())

    cur.execute("""
        INSERT INTO decision_reason_statutes (decision_id, law, article_raw, sub_ref, raw_match)
        SELECT decision_id, law, article_raw, sub_ref, raw_match FROM _tmp_drs
        ON CONFLICT (decision_id, law, article_raw, sub_ref) DO NOTHING
    """)
    inserted = cur.rowcount
    cur.execute("DROP TABLE _tmp_drs")
    conn.commit()
    print(f"[DRS] 新增 {inserted} 筆")

    # --- citation_snippet_statutes ---
    print(f"\n[CSS] 查詢含「{law_target}」的 citation...")
    cur.execute("SELECT id, snippet FROM citations WHERE snippet LIKE %s ORDER BY id",
                (f"%{law_target}%",))
    rows = cur.fetchall()
    print(f"[CSS] 共 {len(rows)} 筆 citation")

    buf = io.StringIO()
    count = 0
    for cid, snippet in rows:
        for law, art, sub, raw in extract_statutes(snippet):
            if law == law_target:
                raw_esc = raw.replace("\\", "\\\\").replace("\t", " ").replace("\r", " ").replace("\n", " ")
                buf.write(f"{cid}\t{law}\t{art}\t{sub}\t{raw_esc}\n")
                count += 1
    print(f"[CSS] 解析出 {count} 筆，開始 COPY...")

    cur.execute("CREATE TEMP TABLE _tmp_css (citation_id int, law text, article_raw text, sub_ref text, raw_match text)")
    buf.seek(0)
    with cur.copy("COPY _tmp_css FROM STDIN") as copy:
        copy.write(buf.getvalue().encode())

    cur.execute("""
        INSERT INTO citation_snippet_statutes (citation_id, law, article_raw, sub_ref, raw_match)
        SELECT citation_id, law, article_raw, sub_ref, raw_match FROM _tmp_css
        ON CONFLICT (citation_id, law, article_raw, sub_ref) DO NOTHING
    """)
    inserted2 = cur.rowcount
    cur.execute("DROP TABLE _tmp_css")
    conn.commit()
    print(f"[CSS] 新增 {inserted2} 筆")

    cur.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法：python backfill_law.py 刑法 [--db CONNECTION_STRING]")
        sys.exit(1)

    law_target = sys.argv[1]
    db_url = DEFAULT_DB
    if "--db" in sys.argv:
        db_url = sys.argv[sys.argv.index("--db") + 1]

    conn = psycopg.connect(db_url)
    print(f"✓ 連線成功，補跑「{law_target}」")
    backfill(conn, law_target)
    conn.close()
    print("\n完成")
