#!/usr/bin/env python3
"""
一次性清理重複 placeholder（同字號不同 case_type 造成的重複 NULL 行）。

策略：
- 只刪 case_type=NULL 的 placeholder，non-NULL 的各自保留（民事/刑事可並存）
- NULL 的 citations：依 source 判決的 case_type remap 到對應 non-NULL placeholder
  - 若 source case_type 無對應 non-NULL → fallback 到最多引用的 non-NULL
- 刪除 NULL placeholder
"""
import os
from collections import defaultdict

import psycopg
from psycopg.rows import dict_row

DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/citations",
)


def main() -> None:
    with psycopg.connect(DB_URL, row_factory=dict_row) as conn:

        # Step 1：找所有有 NULL case_type placeholder 且同時有 non-NULL 的群組
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    unit_norm, jyear, jcase_norm, jno,
                    COALESCE(doc_type, '') AS doc_type_key,
                    array_agg(id ORDER BY id) AS ids,
                    array_agg(case_type ORDER BY id) AS case_types
                FROM decisions
                WHERE jid IS NULL
                GROUP BY unit_norm, jyear, jcase_norm, jno, COALESCE(doc_type, '')
                HAVING
                    bool_or(case_type IS NULL)
                    AND bool_or(case_type IS NOT NULL)
            """)
            groups = cur.fetchall()

        print(f"需處理群組：{len(groups)} 個")
        total_remapped = 0
        total_deleted = 0

        for g in groups:
            ids = g["ids"]
            case_types = g["case_types"]

            null_ids = [i for i, ct in zip(ids, case_types) if ct is None]
            non_null = {ct: i for i, ct in zip(ids, case_types) if ct is not None}
            # non_null: {case_type: id}，若同 case_type 多筆取最小 id（已 ORDER BY id）

            for null_id in null_ids:
                # Step 2：取出 NULL placeholder 的所有 citations
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT c.id AS cid, src.case_type AS src_ct
                        FROM citations c
                        JOIN decisions src ON src.id = c.source_id
                        WHERE c.target_id = %s
                    """, (null_id,))
                    cits = cur.fetchall()

                # 計算 fallback（non-NULL 中引用數最多的）
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT target_id, COUNT(*) AS cnt
                        FROM citations
                        WHERE target_id = ANY(%s)
                        GROUP BY target_id
                        ORDER BY cnt DESC
                        LIMIT 1
                    """, (list(non_null.values()),))
                    row = cur.fetchone()
                    fallback_id = row["target_id"] if row else list(non_null.values())[0]

                # Step 3：remap 各 citation
                remap: dict[int, int] = {}  # cid → winner_id
                for cit in cits:
                    src_ct = cit["src_ct"]
                    winner = non_null.get(src_ct, fallback_id)
                    remap[cit["cid"]] = winner

                if remap:
                    # 按 winner 分組批次 UPDATE
                    by_winner: dict[int, list[int]] = defaultdict(list)
                    for cid, wid in remap.items():
                        by_winner[wid].append(cid)

                    with conn.cursor() as cur:
                        for winner_id, cids in by_winner.items():
                            cur.execute(
                                "UPDATE citations SET target_id = %s WHERE id = ANY(%s)",
                                (winner_id, cids),
                            )
                            total_remapped += cur.rowcount

                # Step 4：刪 NULL placeholder
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM decisions WHERE id = %s", (null_id,))
                    total_deleted += cur.rowcount

            conn.commit()

        print(f"remap citations：{total_remapped}")
        print(f"delete decisions：{total_deleted}")

        # 確認結果
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) AS remaining_null
                FROM decisions
                WHERE jid IS NULL AND case_type IS NULL
            """)
            print(f"剩餘 NULL placeholder：{cur.fetchone()['remaining_null']}")


if __name__ == "__main__":
    main()
