"""
reconcile_null_placeholders.py

把「同案號同 case_type 的 NULL doc_type placeholder」的 citations 重導向真實判決。

處理策略：
  A. {(NULL), 判決/裁定} — NULL placeholder + 真實判決（jid IS NOT NULL）：
       直接重導，只有 34 筆，全部安全。

  B. {(NULL), 判決/裁定} — NULL placeholder + 明確 doc_type placeholder（兩者都 jid IS NULL）：
       跳過，等真實判決入庫後由升級邏輯自然合併。（共 2294 筆）

  C. {(NULL), 判決, 裁定} — 三元衝突：
       暫不處理，待後續統計或 snippet 比對後再決定。

用法：
  python etl/reconcile_null_placeholders.py          # dry-run，只印報告不寫入
  python etl/reconcile_null_placeholders.py --apply  # 實際寫入 DB
"""

import sys
import os
import psycopg

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_db_connection():
    url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    ).strip()
    return psycopg.connect(url)


def _fetch_redirectable(conn) -> list[dict]:
    """
    找「NULL placeholder + 真實判決（jid IS NOT NULL）同案號並存」的群組。
    回傳每筆的 ph_id, real_id, real_doc_type, 以及 NULL placeholder 上的 citation 數。
    """
    with conn.cursor() as cur:
        cur.execute("""
            WITH groups AS (
                SELECT unit_norm, jyear, jcase_norm, jno, case_type
                FROM decisions
                GROUP BY unit_norm, jyear, jcase_norm, jno, case_type
                HAVING COUNT(DISTINCT COALESCE(doc_type, '(NULL)')) = 2
                   AND bool_or(jid IS NULL  AND doc_type IS NULL)
                   AND bool_or(jid IS NOT NULL AND doc_type IS NOT NULL)
                   AND COUNT(DISTINCT CASE WHEN jid IS NOT NULL THEN doc_type END) = 1
            ),
            null_ph AS (
                SELECT d.id AS ph_id, g.unit_norm, g.jyear, g.jcase_norm, g.jno, g.case_type
                FROM decisions d
                JOIN groups g USING (unit_norm, jyear, jcase_norm, jno, case_type)
                WHERE d.jid IS NULL AND d.doc_type IS NULL
            ),
            real_row AS (
                SELECT d.id AS real_id, d.doc_type AS real_doc_type,
                       g.unit_norm, g.jyear, g.jcase_norm, g.jno, g.case_type
                FROM decisions d
                JOIN groups g USING (unit_norm, jyear, jcase_norm, jno, case_type)
                WHERE d.jid IS NOT NULL
            )
            SELECT
                np.ph_id,
                rr.real_id,
                rr.real_doc_type,
                np.unit_norm, np.jyear, np.jcase_norm, np.jno,
                COUNT(c.id) AS citation_count
            FROM null_ph np
            JOIN real_row rr USING (unit_norm, jyear, jcase_norm, jno, case_type)
            LEFT JOIN citations c ON c.target_id = np.ph_id
            GROUP BY np.ph_id, rr.real_id, rr.real_doc_type,
                     np.unit_norm, np.jyear, np.jcase_norm, np.jno
            ORDER BY rr.real_doc_type, null_citations DESC
        """.replace("null_citations", "COUNT(c.id)"))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def reconcile(apply: bool = False):
    conn = get_db_connection()
    rows = _fetch_redirectable(conn)

    total_cites = sum(r['citation_count'] for r in rows)
    has_cites   = [r for r in rows if r['citation_count'] > 0]

    print(f"找到 {len(rows)} 筆可重導（NULL placeholder + 真實判決並存）")
    print(f"  其中有 citation 的：{len(has_cites)} 筆，共 {total_cites} 筆 citation\n")

    for r in rows:
        label = (f"{r['unit_norm']} {r['jyear']}年{r['jcase_norm']}字第{r['jno']}號"
                 f"（{r['real_doc_type']}）")
        action = f"ph={r['ph_id']} → real={r['real_id']}  citations={r['citation_count']}"
        print(f"  {'APPLY' if apply else 'DRY  '} {label}  {action}")

        if apply:
            with conn.cursor() as cur:
                if r['citation_count'] > 0:
                    cur.execute(
                        "UPDATE citations SET target_id = %s WHERE target_id = %s",
                        (r['real_id'], r['ph_id']),
                    )
                cur.execute("DELETE FROM decisions WHERE id = %s", (r['ph_id'],))
            conn.commit()

    mode = "APPLY 完成" if apply else "DRY-RUN（加 --apply 才寫入）"
    print(f"\n{mode}：重導 {total_cites} 筆 citation，"
          f"刪除 {len(rows)} 個 NULL placeholder")

    conn.close()


if __name__ == '__main__':
    apply = '--apply' in sys.argv
    reconcile(apply=apply)
