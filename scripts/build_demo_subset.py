#!/usr/bin/env python3
# 用途：從完整的 citations 資料庫切出 demo 用的引用子集，複製到另一個（空的）資料庫。
# 說明：
# 1) 以法條挑出 source 判決（引用方），再帶入它們引用的 target，確保引用關係不斷。
# 2) chunks 只帶已有 embedding 的（RAG 只會用到這些）。
# 3) 目標資料庫需先建好 schema，且各表必須是空的：
#      docker exec lawcidity-db createdb -U postgres citations_demo
#      docker exec lawcidity-db pg_dump -U postgres -s citations \
#        | docker exec -i lawcidity-db psql -U postgres -d citations_demo

import os
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# demo 主題 → 用來挑 source 判決的 (law, article_raw)
DEMO_TOPICS: dict[str, list[tuple[str, str]]] = {
    "Murder & self-defense": [("刑法", "271"), ("刑法", "23")],
    "DUI": [("刑法", "185之3")],
    "Car accidents & negligence": [("刑法", "284"), ("刑法", "276"), ("民法", "217")],
    "Defamation": [("刑法", "310")],
    "Wrongful termination": [("勞動基準法", "11"), ("勞動基準法", "12")],
    "Divorce & custody": [("民法", "1052"), ("民法", "1055")],
}

# 依 FK 相依順序複製
COPY_PLAN: list[tuple[str, str]] = [
    ("court_units", "TRUE"),
    ("authorities", "TRUE"),
    ("decisions", "t.id IN (SELECT id FROM demo_decisions)"),
    ("decision_reason_statutes", "t.decision_id IN (SELECT id FROM demo_decisions)"),
    ("citations", "t.id IN (SELECT id FROM demo_citations)"),
    ("citation_snippet_statutes", "t.citation_id IN (SELECT id FROM demo_citations)"),
    ("chunks", "t.decision_id IN (SELECT id FROM demo_sources) AND t.embedding IS NOT NULL"),
]


def _select_subset(src: psycopg.Connection) -> None:
    statutes = [pair for pairs in DEMO_TOPICS.values() for pair in pairs]
    laws = [law for law, _ in statutes]
    articles = [article for _, article in statutes]

    # 同一 jid 重複匯入時只留 id 最小的那筆
    src.execute(
        """
        CREATE TEMP TABLE demo_sources AS
        SELECT id FROM (
            SELECT d.id,
                   row_number() OVER (
                       PARTITION BY COALESCE(d.jid, d.id::text) ORDER BY d.id
                   ) AS rn
            FROM decisions d
            WHERE d.clean_text IS NOT NULL
              AND EXISTS (SELECT 1 FROM citations c WHERE c.source_id = d.id)
              AND EXISTS (
                  SELECT 1
                  FROM decision_reason_statutes drs
                  JOIN unnest(%s::text[], %s::text[]) AS want(law, article_raw)
                    ON want.law = drs.law AND want.article_raw = drs.article_raw
                  WHERE drs.decision_id = d.id
              )
        ) ranked
        WHERE rn = 1
        """,
        (laws, articles),
    )
    src.execute(
        """
        CREATE TEMP TABLE demo_citations AS
        SELECT c.id, c.target_id, c.target_canonical_id
        FROM citations c
        WHERE c.source_id IN (SELECT id FROM demo_sources)
        """
    )
    src.execute(
        """
        CREATE TEMP TABLE demo_decisions AS
        SELECT id FROM demo_sources
        UNION
        SELECT target_id FROM demo_citations WHERE target_id IS NOT NULL
        UNION
        SELECT target_canonical_id FROM demo_citations WHERE target_canonical_id IS NOT NULL
        UNION
        SELECT unnest(ch.target_ids)
        FROM chunks ch
        WHERE ch.decision_id IN (SELECT id FROM demo_sources)
          AND ch.embedding IS NOT NULL
        """
    )
    # decisions.canonical_id 是自我參照的 FK，補到閉包為止
    while True:
        added = src.execute(
            """
            INSERT INTO demo_decisions
            SELECT DISTINCT d.canonical_id
            FROM decisions d
            WHERE d.id IN (SELECT id FROM demo_decisions)
              AND d.canonical_id IS NOT NULL
              AND NOT EXISTS (SELECT 1 FROM demo_decisions dd WHERE dd.id = d.canonical_id)
            """
        ).rowcount
        if not added:
            break
    # chunks.target_ids 可能指到已不存在的 decision，只留實際存在的
    src.execute(
        """
        DELETE FROM demo_decisions dd
        WHERE NOT EXISTS (SELECT 1 FROM decisions d WHERE d.id = dd.id)
        """
    )


def _insertable_columns(src: psycopg.Connection, table: str) -> list[str]:
    rows = src.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND is_generated = 'NEVER'
        ORDER BY ordinal_position
        """,
        (table,),
    ).fetchall()
    return [row[0] for row in rows]


def _copy_table(
    src: psycopg.Connection,
    dst: psycopg.Connection,
    table: str,
    where_sql: str,
) -> int:
    columns = ", ".join(_insertable_columns(src, table))
    select_sql = f"SELECT {columns} FROM {table} t WHERE {where_sql}"
    with src.cursor() as src_cur, dst.cursor() as dst_cur:
        with src_cur.copy(f"COPY ({select_sql}) TO STDOUT") as copy_out:
            with dst_cur.copy(f"COPY {table} ({columns}) FROM STDIN") as copy_in:
                for data in copy_out:
                    copy_in.write(data)
        return dst_cur.rowcount


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)

    source_url = os.environ.get(
        "SOURCE_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    )
    demo_url = os.environ.get(
        "DEMO_DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations_demo",
    )
    if source_url == demo_url:
        raise ValueError("SOURCE_DATABASE_URL 與 DEMO_DATABASE_URL 不可相同")

    with psycopg.connect(source_url) as src, psycopg.connect(demo_url) as dst:
        for table, _ in COPY_PLAN:
            if dst.execute(f"SELECT EXISTS (SELECT 1 FROM {table})").fetchone()[0]:
                raise RuntimeError(f"目標資料庫的 {table} 不是空的，為避免覆蓋資料已中止")

        print("[demo-subset] selecting subset ...")
        _select_subset(src)
        for name in ("demo_sources", "demo_citations", "demo_decisions"):
            count = src.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            print(f"[demo-subset] {name}={count}")

        for table, where_sql in COPY_PLAN:
            copied = _copy_table(src, dst, table, where_sql)
            print(f"[demo-subset] copied {table}: {copied}")

        # 子集規模用精確掃描即可；在空表上建好的 IVFFlat centroid 也不適用於新資料
        dst.execute("DROP INDEX IF EXISTS cc_embedding_ivfflat")
        dst.commit()

    with psycopg.connect(demo_url, autocommit=True) as dst:
        dst.execute("VACUUM ANALYZE")
        size = dst.execute(
            "SELECT pg_size_pretty(pg_database_size(current_database()))"
        ).fetchone()[0]
        print(f"[demo-subset] done, demo database size={size}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
