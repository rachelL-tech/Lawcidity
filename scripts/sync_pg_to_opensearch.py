#!/usr/bin/env python3
# 用途：將 PostgreSQL decisions 批次同步到 OpenSearch（可重跑、可分批）。
# 說明：
# 1) 以 source_id 作為 OpenSearch _id，重跑時會覆寫同 id（idempotent）。
# 2) 支援依 id/date/case_type 切片同步，便於先跑 2 個月再擴大。

import argparse
import json
import os
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row


BATCH_SIZE = 500
REQUEST_TIMEOUT = 120
REFRESH_EACH_BATCH = False


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _parse_iso_date(raw: str | None, name: str) -> date | None:
    if raw is None:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} 格式必須為 YYYY-MM-DD") from exc


def _build_opensearch_client():
    try:
        from opensearchpy import OpenSearch
    except Exception as exc:
        raise RuntimeError("缺少 opensearch-py 套件") from exc

    url = os.environ.get("OPENSEARCH_URL", "https://localhost:9200").strip()
    parsed = urlparse(url)
    scheme = (parsed.scheme or "https").lower()
    if scheme not in {"http", "https"}:
        raise RuntimeError("OPENSEARCH_URL 只支援 http 或 https")

    host = parsed.hostname or "localhost"
    port = parsed.port or 9200
    use_ssl = scheme == "https"
    verify_certs = _env_bool("OPENSEARCH_VERIFY_CERTS", False)

    username = os.environ.get("OPENSEARCH_USERNAME", "admin").strip()
    password = os.environ.get("OPENSEARCH_PASSWORD", "").strip()
    auth = (username, password) if username else None

    kwargs: dict[str, Any] = {
        "hosts": [{"host": host, "port": port}],
        "http_auth": auth,
        "use_ssl": use_ssl,
        "verify_certs": verify_certs,
    }
    if use_ssl and not verify_certs:
        kwargs["ssl_assert_hostname"] = False

    return OpenSearch(**kwargs)


def _fetch_decisions_batch(
    conn: psycopg.Connection,
    *,
    last_id: int,
    end_id: int | None,
    from_date: date | None,
    to_date: date | None,
) -> list[dict[str, Any]]:
    where_parts = [
        "d.id > %(last_id)s",
        "d.clean_text IS NOT NULL",
        "EXISTS (SELECT 1 FROM citations c WHERE c.source_id = d.id)",
    ]
    params: dict[str, Any] = {
        "last_id": last_id,
        "batch_size": BATCH_SIZE,
    }

    if end_id is not None:
        where_parts.append("d.id <= %(end_id)s")
        params["end_id"] = end_id
    if from_date is not None:
        where_parts.append("d.decision_date >= %(from_date)s")
        params["from_date"] = from_date
    if to_date is not None:
        where_parts.append("d.decision_date <= %(to_date)s")
        params["to_date"] = to_date

    sql = f"""
        SELECT
            d.id AS source_id,
            d.case_type,
            d.clean_text
        FROM decisions d
        WHERE {" AND ".join(where_parts)}
        ORDER BY d.id ASC
        LIMIT %(batch_size)s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _fetch_statutes_map(
    conn: psycopg.Connection,
    decision_ids: list[int],
) -> dict[int, list[dict[str, str]]]:
    if not decision_ids:
        return {}

    sql = """
        SELECT
            drs.decision_id,
            drs.law,
            drs.article_raw,
            NULLIF(drs.sub_ref, '') AS sub_ref
        FROM decision_reason_statutes drs
        WHERE drs.decision_id = ANY(%(decision_ids)s::bigint[])
        ORDER BY drs.decision_id, drs.id
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"decision_ids": decision_ids})
        rows = cur.fetchall()

    out: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        item = {
            "law": row["law"],
            "article_raw": row["article_raw"],
        }
        if row["sub_ref"] is not None:
            item["sub_ref"] = row["sub_ref"]
        out[int(row["decision_id"])].append(item)
    return out


def _bulk_index(
    client: Any,
    *,
    index_name: str,
    docs: list[dict[str, Any]],
) -> int:
    if not docs:
        return 0

    lines: list[str] = []
    for doc in docs:
        doc_id = str(doc["source_id"])
        lines.append(
            json.dumps(
                {"index": {"_index": index_name, "_id": doc_id}},
                ensure_ascii=False,
            )
        )
        lines.append(json.dumps(doc, ensure_ascii=False))
    body = "\n".join(lines) + "\n"

    response = client.bulk(
        body=body,
        request_timeout=REQUEST_TIMEOUT,
        refresh="wait_for" if REFRESH_EACH_BATCH else False,
    )

    if response.get("errors"):
        failures: list[dict[str, Any]] = []
        for item in response.get("items", []):
            info = item.get("index") or {}
            status = int(info.get("status", 0))
            if status >= 300:
                failures.append(
                    {
                        "id": info.get("_id"),
                        "status": status,
                        "error": info.get("error"),
                    }
                )
        sample = failures[:3]
        raise RuntimeError(f"bulk 失敗 {len(failures)} 筆，範例: {sample}")

    return len(docs)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PG -> OpenSearch bulk 同步（可重跑、可分批）"
    )
    parser.add_argument("--start-id", type=int, default=1, help="起始 decision id（含）")
    parser.add_argument("--end-id", type=int, default=None, help="結束 decision id（含）")
    parser.add_argument("--from-date", type=str, default=None, help="decision_date 起日（YYYY-MM-DD）")
    parser.add_argument("--to-date", type=str, default=None, help="decision_date 迄日（YYYY-MM-DD）")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    if args.start_id <= 0:
        raise ValueError("start-id 必須 >= 1")
    if args.end_id is not None and args.end_id < args.start_id:
        raise ValueError("end-id 不可小於 start-id")

    from_date = _parse_iso_date(args.from_date, "from-date")
    to_date = _parse_iso_date(args.to_date, "to-date")
    if from_date and to_date and from_date > to_date:
        raise ValueError("from-date 不可晚於 to-date")

    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    )
    index_name = os.environ.get("OPENSEARCH_INDEX", "decisions_v3")
    client = _build_opensearch_client()
    analyzer_env = os.environ.get("OPENSEARCH_ANALYZER", "").strip()
    search_analyzer_env = os.environ.get("OPENSEARCH_SEARCH_ANALYZER", "").strip()

    if not client.indices.exists(index=index_name):
        raise RuntimeError(
            f"OpenSearch index 不存在：{index_name}，請先執行 scripts/init_opensearch_index.sh"
        )

    print(
        "[sync] start",
        f"index={index_name}",
        f"batch_size={BATCH_SIZE}",
        f"start_id={args.start_id}",
        f"end_id={args.end_id}",
        f"from_date={from_date}",
        f"to_date={to_date}",
        "only_cited=True",
        f"analyzer={analyzer_env or 'unset'}",
        f"search_analyzer={search_analyzer_env or 'unset'}",
    )

    total_docs = 0
    total_batches = 0
    last_id = args.start_id - 1

    with psycopg.connect(db_url, row_factory=dict_row) as conn:
        while True:
            rows = _fetch_decisions_batch(
                conn,
                last_id=last_id,
                end_id=args.end_id,
                from_date=from_date,
                to_date=to_date,
            )
            if not rows:
                break

            decision_ids = [int(r["source_id"]) for r in rows]
            statutes_map = _fetch_statutes_map(conn, decision_ids)

            docs: list[dict[str, Any]] = []
            for row in rows:
                source_id = int(row["source_id"])
                clean_text = (row["clean_text"] or "").strip()
                if not clean_text:
                    continue
                docs.append(
                    {
                        "source_id": source_id,
                        "case_type": row["case_type"],
                        "clean_text": clean_text,
                        "statutes": statutes_map.get(source_id, []),
                    }
                )

            written = _bulk_index(
                client,
                index_name=index_name,
                docs=docs,
            )

            total_batches += 1
            total_docs += written
            last_id = decision_ids[-1]

            print(
                f"[sync] batch={total_batches} fetched={len(rows)} indexed={written} "
                f"last_id={last_id} total={total_docs}"
            )

    print(f"[sync] done batches={total_batches} indexed_docs={total_docs} final_last_id={last_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
