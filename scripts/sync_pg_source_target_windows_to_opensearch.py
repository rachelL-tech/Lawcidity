#!/usr/bin/env python3
# 用途：將 PostgreSQL citations 聚合為 source-target windows 並同步到 OpenSearch。

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import psycopg
from dotenv import load_dotenv
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

VALID_CASE_TYPES = {"民事", "刑事", "行政", "憲法"}
SNIPPET_FIELD = "window_text_snippet"


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


def _normalize_case_types(raw_values: list[str]) -> list[str]:
    values: list[str] = []
    for raw in raw_values:
        values.extend(x.strip() for x in raw.split(",") if x.strip())
    invalid = [x for x in values if x not in VALID_CASE_TYPES]
    if invalid:
        raise ValueError("case_type 僅支援：民事,刑事,行政,憲法")
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


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

    ca_certs = os.environ.get("OPENSEARCH_CA_CERTS", "").strip()
    if use_ssl and verify_certs and ca_certs:
        kwargs["ca_certs"] = ca_certs

    return OpenSearch(**kwargs)


def _fetch_source_ids_batch(
    conn: psycopg.Connection,
    *,
    last_source_id: int,
    batch_size: int,
    end_source_id: int | None,
    from_date: date | None,
    to_date: date | None,
    case_types: list[str],
) -> list[int]:
    where_parts = [
        "c.source_id > %(last_source_id)s",
        "src.clean_text IS NOT NULL",
        "(c.target_id IS NOT NULL OR c.target_authority_id IS NOT NULL)",
    ]
    params: dict[str, Any] = {
        "last_source_id": last_source_id,
        "batch_size": batch_size,
    }

    if end_source_id is not None:
        where_parts.append("c.source_id <= %(end_source_id)s")
        params["end_source_id"] = end_source_id
    if from_date is not None:
        where_parts.append("src.decision_date >= %(from_date)s")
        params["from_date"] = from_date
    if to_date is not None:
        where_parts.append("src.decision_date <= %(to_date)s")
        params["to_date"] = to_date
    if case_types:
        where_parts.append("src.case_type = ANY(%(case_types)s)")
        params["case_types"] = case_types

    sql = f"""
        SELECT DISTINCT c.source_id
        FROM citations c
        JOIN decisions src ON src.id = c.source_id
        WHERE {" AND ".join(where_parts)}
        ORDER BY c.source_id ASC
        LIMIT %(batch_size)s
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [int(row["source_id"]) for row in cur.fetchall()]


def _fetch_source_target_rows(
    conn: psycopg.Connection,
    *,
    source_ids: list[int],
) -> list[dict[str, Any]]:
    if not source_ids:
        return []

    sql = """
        SELECT
            c.id AS citation_id,
            c.source_id,
            COALESCE(td.canonical_id, c.target_id) AS canonical_target_id,
            c.target_authority_id,
            src.case_type,
            src.clean_text,
            c.snippet,
            c.match_start,
            c.match_end
        FROM citations c
        JOIN decisions src ON src.id = c.source_id
        LEFT JOIN decisions td ON td.id = c.target_id
        WHERE c.source_id = ANY(%(source_ids)s::bigint[])
          AND (c.target_id IS NOT NULL OR c.target_authority_id IS NOT NULL)
        ORDER BY c.source_id ASC, c.id ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"source_ids": source_ids})
        return cur.fetchall()


def _fetch_citation_statutes_map(
    conn: psycopg.Connection,
    citation_ids: list[int],
) -> dict[int, list[dict[str, str]]]:
    if not citation_ids:
        return {}

    sql = """
        SELECT
            css.citation_id,
            css.law,
            css.article_raw,
            NULLIF(css.sub_ref, '') AS sub_ref
        FROM citation_snippet_statutes css
        WHERE css.citation_id = ANY(%(citation_ids)s::bigint[])
        ORDER BY css.citation_id, css.id
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"citation_ids": citation_ids})
        rows = cur.fetchall()

    out: dict[int, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        item = {
            "law": row["law"],
            "article_raw": row["article_raw"],
        }
        if row["sub_ref"] is not None:
            item["sub_ref"] = row["sub_ref"]
        out[int(row["citation_id"])].append(item)
    return out


def _append_unique_text(items: list[str], seen: set[str], text: str, limit: int) -> None:
    normalized = (text or "").strip()
    if not normalized or normalized in seen or len(items) >= limit:
        return
    seen.add(normalized)
    items.append(normalized)


def _statute_sort_key(item: tuple[str, str, str | None]) -> tuple[str, str, str]:
    law, article_raw, sub_ref = item
    return (law, article_raw, sub_ref or "")


def _build_source_target_docs(
    rows: list[dict[str, Any]],
    statutes_map: dict[int, list[dict[str, str]]],
    *,
    max_windows_per_field: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[int, str], dict[str, Any]] = {}

    for row in rows:
        source_id = int(row["source_id"])
        canonical_target_id = row.get("canonical_target_id")
        target_authority_id = row.get("target_authority_id")
        if canonical_target_id is not None:
            target_type = "decision"
            target_value = int(canonical_target_id)
            target_id = target_value
            authority_id = None
        elif target_authority_id is not None:
            target_type = "authority"
            target_value = int(target_authority_id)
            target_id = None
            authority_id = target_value
        else:
            continue

        target_uid = f"{target_type}:{target_value}"
        key = (source_id, target_uid)
        doc = grouped.get(key)
        if doc is None:
            doc = {
                "source_id": source_id,
                "target_id": target_id,
                "target_authority_id": authority_id,
                "target_type": target_type,
                "target_uid": target_uid,
                "case_type": row["case_type"],
                "merged_citation_count": 0,
                "_window_lists": {
                    SNIPPET_FIELD: [],
                },
                "_window_seen": {
                    SNIPPET_FIELD: set(),
                },
                "_statutes": set(),
            }
            grouped[key] = doc

        doc["merged_citation_count"] += 1
        clean_text = row["clean_text"]
        _append_unique_text(
            doc["_window_lists"][SNIPPET_FIELD],
            doc["_window_seen"][SNIPPET_FIELD],
            row.get("snippet") or "",
            max_windows_per_field,
        )

        for statute in statutes_map.get(int(row["citation_id"]), []):
            statute_key = (
                statute["law"],
                statute["article_raw"],
                statute.get("sub_ref"),
            )
            doc["_statutes"].add(statute_key)

    docs: list[dict[str, Any]] = []
    for doc in grouped.values():
        output = {
            "source_id": doc["source_id"],
            "target_id": doc["target_id"],
            "target_authority_id": doc["target_authority_id"],
            "target_type": doc["target_type"],
            "target_uid": doc["target_uid"],
            "case_type": doc["case_type"],
            "merged_citation_count": doc["merged_citation_count"],
            "statutes": [
                {
                    **{"law": law, "article_raw": article_raw},
                    **({"sub_ref": sub_ref} if sub_ref is not None else {}),
                }
                for law, article_raw, sub_ref in sorted(doc["_statutes"], key=_statute_sort_key)
            ],
        }
        for field in [SNIPPET_FIELD]:
            output[field] = "\n".join(doc["_window_lists"][field])
        docs.append(output)
    return docs


def _bulk_index(
    client: Any,
    *,
    index_name: str,
    docs: list[dict[str, Any]],
    request_timeout: int,
    refresh_each_batch: bool,
) -> int:
    if not docs:
        return 0

    lines: list[str] = []
    for doc in docs:
        doc_id = f"{doc['source_id']}::{doc['target_uid']}"
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
        request_timeout=request_timeout,
        refresh="wait_for" if refresh_each_batch else False,
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
        raise RuntimeError(f"bulk 失敗 {len(failures)} 筆，範例: {failures[:3]}")
    return len(docs)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="PG -> OpenSearch source-target window bulk 同步（可重跑、可分批）"
    )
    parser.add_argument("--batch-size", type=int, default=500, help="每批 source 筆數，預設 500")
    parser.add_argument("--start-source-id", type=int, default=1, help="起始 source id（含）")
    parser.add_argument("--end-source-id", type=int, default=None, help="結束 source id（含）")
    parser.add_argument("--from-date", type=str, default=None, help="source decision_date 起日（YYYY-MM-DD）")
    parser.add_argument("--to-date", type=str, default=None, help="source decision_date 迄日（YYYY-MM-DD）")
    parser.add_argument("--case-type", action="append", default=[], help="可重複或逗號分隔：民事,刑事,行政,憲法")
    parser.add_argument("--max-batches", type=int, default=0, help="最多跑幾批（0=不限制）")
    parser.add_argument("--max-windows-per-field", type=int, default=8, help="每個 window 欄位最多保留幾段文字")
    parser.add_argument("--request-timeout", type=int, default=120, help="OpenSearch bulk timeout 秒數")
    parser.add_argument("--refresh-each-batch", action="store_true", help="每批寫完立即 refresh（較慢）")
    parser.add_argument("--dry-run", action="store_true", help="只讀取/計算，不寫入 OpenSearch")
    parser.add_argument("--env-file", type=str, help="讀取指定的 .env 檔案")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
    if args.env_file:
        load_dotenv(args.env_file, override=True)

    if args.batch_size <= 0:
        raise ValueError("batch-size 必須 > 0")
    if args.start_source_id <= 0:
        raise ValueError("start-source-id 必須 >= 1")
    if args.end_source_id is not None and args.end_source_id < args.start_source_id:
        raise ValueError("end-source-id 不可小於 start-source-id")
    if args.max_windows_per_field <= 0:
        raise ValueError("max-windows-per-field 必須 > 0")

    from_date = _parse_iso_date(args.from_date, "from-date")
    to_date = _parse_iso_date(args.to_date, "to-date")
    if from_date and to_date and from_date > to_date:
        raise ValueError("from-date 不可晚於 to-date")

    case_types = _normalize_case_types(args.case_type)
    db_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://postgres:postgres@localhost:5432/citations",
    )
    index_name = os.environ.get("OPENSEARCH_SOURCE_TARGET_INDEX", "source_target_windows_v2")
    client = _build_opensearch_client()

    if not client.indices.exists(index=index_name):
        raise RuntimeError(
            f"OpenSearch index 不存在：{index_name}，請先執行 scripts/init_opensearch_source_target_index.sh"
        )

    print(
        "[sync-source-target] start",
        f"index={index_name}",
        f"batch_size={args.batch_size}",
        f"start_source_id={args.start_source_id}",
        f"end_source_id={args.end_source_id}",
        f"from_date={from_date}",
        f"to_date={to_date}",
        f"case_types={case_types or 'ALL'}",
        f"max_windows_per_field={args.max_windows_per_field}",
        f"dry_run={args.dry_run}",
    )

    total_docs = 0
    total_batches = 0
    last_source_id = args.start_source_id - 1

    with psycopg.connect(db_url, row_factory=dict_row) as conn:
        while True:
            source_ids = _fetch_source_ids_batch(
                conn,
                last_source_id=last_source_id,
                batch_size=args.batch_size,
                end_source_id=args.end_source_id,
                from_date=from_date,
                to_date=to_date,
                case_types=case_types,
            )
            if not source_ids:
                break

            rows = _fetch_source_target_rows(conn, source_ids=source_ids)
            citation_ids = [int(row["citation_id"]) for row in rows]
            statutes_map = _fetch_citation_statutes_map(conn, citation_ids)
            docs = _build_source_target_docs(
                rows,
                statutes_map,
                max_windows_per_field=args.max_windows_per_field,
            )

            if args.dry_run:
                written = len(docs)
            else:
                written = _bulk_index(
                    client,
                    index_name=index_name,
                    docs=docs,
                    request_timeout=args.request_timeout,
                    refresh_each_batch=args.refresh_each_batch,
                )

            total_batches += 1
            total_docs += written
            last_source_id = source_ids[-1]
            print(
                f"[sync-source-target] batch={total_batches} sources={len(source_ids)} "
                f"citations={len(rows)} indexed={written} last_source_id={last_source_id} total={total_docs}"
            )

            if args.max_batches > 0 and total_batches >= args.max_batches:
                print(f"[sync-source-target] stop: reached max-batches={args.max_batches}")
                break

    print(
        f"[sync-source-target] done batches={total_batches} indexed_docs={total_docs} "
        f"final_last_source_id={last_source_id}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
