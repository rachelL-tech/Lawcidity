"""
搜尋業務邏輯層的 PostgreSQL 實作（不依賴 OpenSearch，給小資料集部署用）。

職責與 opensearch_service 相同，回傳格式也相同：
- Stage 1 召回：search_source_ids_postgres（clean_text LIKE + decision_reason_statutes）
- Stage 2 target ranking：search_target_rankings_step_down_postgres（msm 階梯式）

關鍵字比對用 LIKE（區分大小寫，與 ngram analyzer 行為一致；ILIKE 在全文上慢約 2 倍）。

與 OpenSearch 版的差異：
- source-target window 不預先建索引，查詢時直接從 citations 以 (source_id, target_uid) 分組
- 同一組 (source, target) 的所有 snippet 都會參與比對（OpenSearch 版每組最多 8 段）
- preview_source_ids 依「命中條件數多 → source_id 小」排序
"""

from typing import Any

from app.db import get_conn


def _build_statute_exists_sql(
    idx: int,
    statute: tuple[str, str | None, str | None],
    params: dict[str, Any],
    *,
    table: str,
    join_sql: str,
    key_prefix: str,
) -> str:
    law, article, sub_ref = statute
    law_key = f"{key_prefix}law_{idx}"
    params[law_key] = law
    inner = f"s.law = %({law_key})s"
    if article is not None:
        article_key = f"{key_prefix}article_{idx}"
        params[article_key] = article
        inner += f" AND s.article_raw = %({article_key})s"
    if sub_ref is not None:
        # prefix 比對：搜尋「第1項」可命中「第1項前段」、「第1項第1款」等
        sub_key = f"{key_prefix}sub_ref_{idx}"
        params[sub_key] = f"{sub_ref}%"
        inner += f" AND s.sub_ref LIKE %({sub_key})s"
    return f"EXISTS (SELECT 1 FROM {table} s WHERE {join_sql} AND {inner})"


def search_source_ids_postgres(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
) -> list[int]:
    params: dict[str, Any] = {}
    where_parts: list[str] = [
        "d.clean_text IS NOT NULL",
        "EXISTS (SELECT 1 FROM citations c WHERE c.source_id = d.id)",
    ]

    for idx, term in enumerate(query_terms):
        key = f"kw_{idx}"
        where_parts.append(f"d.clean_text LIKE %({key})s")
        params[key] = f"%{term}%"

    if case_types:
        where_parts.append("d.case_type = ANY(%(case_types)s)")
        params["case_types"] = case_types

    for idx, statute in enumerate(statute_filters):
        where_parts.append(
            _build_statute_exists_sql(
                idx, statute, params,
                table="decision_reason_statutes",
                join_sql="s.decision_id = d.id",
                key_prefix="",
            )
        )

    for idx, term in enumerate(exclude_terms):
        key = f"excl_kw_{idx}"
        where_parts.append(f"d.clean_text NOT LIKE %({key})s")
        params[key] = f"%{term}%"

    for idx, statute in enumerate(exclude_statute_filters):
        where_parts.append(
            "NOT " + _build_statute_exists_sql(
                idx, statute, params,
                table="decision_reason_statutes",
                join_sql="s.decision_id = d.id",
                key_prefix="excl_",
            )
        )

    sql = f"""
        SELECT d.id
        FROM decisions d
        WHERE {" AND ".join(where_parts)}
        ORDER BY d.id
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return [int(row["id"]) for row in cur.fetchall()]
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError("PostgreSQL source 召回查詢失敗") from exc


def search_target_rankings_step_down_postgres(
    *,
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    threshold: int = 200,
) -> list[dict[str, Any]]:
    """
    階梯式 step_down：msm=N → N-1 → ... → 1 → 0（filter-only）。
    一次查出每個 (source, target) 命中幾個條件，再在 Python 套用階梯：
    pool 累積達 threshold 的那一階就停，回傳時附 reached_at_msm。
    """
    if not source_ids:
        return []

    params: dict[str, Any] = {"source_ids": source_ids}
    score_parts: list[str] = []
    for idx, term in enumerate(query_terms):
        key = f"kw_{idx}"
        params[key] = f"%{term}%"
        score_parts.append(f"COALESCE(bool_or(c.snippet LIKE %({key})s), false)::int")
    for idx, statute in enumerate(statute_filters):
        exists_sql = _build_statute_exists_sql(
            idx, statute, params,
            table="citation_snippet_statutes",
            join_sql="s.citation_id = c.id",
            key_prefix="",
        )
        score_parts.append(f"bool_or({exists_sql})::int")

    should_count = len(score_parts)
    score_sql = " + ".join(score_parts) if score_parts else "0"

    sql = f"""
        WITH docs AS (
            SELECT
                c.source_id,
                CASE
                    WHEN c.target_id IS NOT NULL
                        THEN 'decision:' || COALESCE(c.target_canonical_id, c.target_id)
                    ELSE 'authority:' || c.target_authority_id
                END AS target_uid,
                {score_sql} AS score
            FROM citations c
            WHERE c.source_id = ANY(%(source_ids)s::bigint[])
            GROUP BY 1, 2
        )
        SELECT
            target_uid,
            MAX(score) AS max_score,
            (ARRAY_AGG(source_id ORDER BY score DESC, source_id))[1:5] AS preview_source_ids,
            (ARRAY_AGG(score ORDER BY score DESC, source_id))[1:5] AS preview_scores
        FROM docs
        GROUP BY target_uid
    """
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
    except RuntimeError:
        raise
    except Exception as exc:
        raise RuntimeError("PostgreSQL target ranking 查詢失敗") from exc

    # 找出 pool 首次達到 threshold 的那一階；都達不到就退到 0（filter-only）
    stop_level = 0
    for level in range(should_count, 0, -1):
        reached = sum(1 for row in rows if int(row["max_score"]) >= level)
        if reached >= threshold:
            stop_level = level
            break

    results: list[dict[str, Any]] = []
    for row in rows:
        max_score = int(row["max_score"])
        if max_score < stop_level:
            continue
        results.append({
            "target_uid": row["target_uid"],
            "preview_source_ids": [
                int(source_id)
                for source_id, score in zip(row["preview_source_ids"], row["preview_scores"])
                if int(score) >= stop_level
            ],
            "reached_at_msm": max_score,
        })
    return results
