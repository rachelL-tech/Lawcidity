"""
搜尋後端切換。

SEARCH_BACKEND=postgres 走 PostgreSQL（小資料集部署），未設定時走 OpenSearch。
環境變數在呼叫時才讀，避免 import 早於 load_dotenv 而讀不到 .env。
"""

import os
from typing import Any

from app.opensearch_service import (
    search_source_ids_opensearch,
    search_target_rankings_step_down as _search_target_rankings_step_down_opensearch,
)
from app.postgres_search_service import (
    search_source_ids_postgres,
    search_target_rankings_step_down_postgres,
)


def get_search_backend() -> str:
    return os.environ.get("SEARCH_BACKEND", "opensearch").strip().lower()


def search_source_ids(
    query_terms: list[str],
    case_types: list[str],
    statute_filters: list[tuple[str, str | None, str | None]],
    exclude_terms: list[str],
    exclude_statute_filters: list[tuple[str, str | None, str | None]],
) -> list[int]:
    search = (
        search_source_ids_postgres
        if get_search_backend() == "postgres"
        else search_source_ids_opensearch
    )
    return search(
        query_terms=query_terms,
        case_types=case_types,
        statute_filters=statute_filters,
        exclude_terms=exclude_terms,
        exclude_statute_filters=exclude_statute_filters,
    )


def search_target_rankings_step_down(
    *,
    query_terms: list[str],
    source_ids: list[int],
    statute_filters: list[tuple[str, str | None, str | None]],
    threshold: int = 200,
) -> list[dict[str, Any]]:
    search = (
        search_target_rankings_step_down_postgres
        if get_search_backend() == "postgres"
        else _search_target_rankings_step_down_opensearch
    )
    return search(
        query_terms=query_terms,
        source_ids=source_ids,
        statute_filters=statute_filters,
        threshold=threshold,
    )
