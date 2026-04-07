"""
搜尋結果快取。

用途：
- /search 完成後，把這次搜尋的 source_ids 與 target rankings 暫存在後端記憶體
- /citations/* 之後只需帶 search_cache_key，即可重用同一批 source_ids

目前先用單機 in-memory TTL cache，避免前端反覆傳大量 source_ids。
"""
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Any
import threading
import time
import uuid


_TTL_SECONDS = 10 * 60
_MAX_ENTRIES = 20


@dataclass
class _SearchSourceEntry:
    expires_at: float
    source_ids: list[int]
    rows: list[dict[str, Any]] | None = None
    ordered_indexes: dict[str, list[int]] | None = None


_LOCK = threading.Lock()
_CACHE: OrderedDict[str, _SearchSourceEntry] = OrderedDict()


def _copy_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
    if rows is None:
        return None

    copied_rows: list[dict[str, Any]] = []
    for row in rows:
        copied = dict(row)
        if "ranked_source_ids" in copied and copied["ranked_source_ids"] is not None:
            copied["ranked_source_ids"] = list(copied["ranked_source_ids"])
        copied_rows.append(copied)
    return copied_rows


def _copy_ordered_indexes(
    ordered_indexes: dict[str, list[int]] | None,
) -> dict[str, list[int]] | None:
    if ordered_indexes is None:
        return None
    return {
        sort: list(indexes)
        for sort, indexes in ordered_indexes.items()
    }


def _prune_expired(now: float) -> None:
    expired_keys = [key for key, entry in _CACHE.items() if entry.expires_at <= now]
    for key in expired_keys:
        _CACHE.pop(key, None)


def _evict_lru_if_needed() -> None:
    while len(_CACHE) > _MAX_ENTRIES:
        _CACHE.popitem(last=False)


def put_search_source_ids(
    source_ids: list[int],
    rows: list[dict[str, Any]] | None = None,
    ordered_indexes: dict[str, list[int]] | None = None,
) -> str:
    now = time.monotonic()
    cache_key = uuid.uuid4().hex
    with _LOCK:
        _prune_expired(now)
        _CACHE[cache_key] = _SearchSourceEntry(
            expires_at=now + _TTL_SECONDS,
            source_ids=list(source_ids),
            rows=_copy_rows(rows),
            ordered_indexes=_copy_ordered_indexes(ordered_indexes),
        )
        _CACHE.move_to_end(cache_key)
        _evict_lru_if_needed()
    return cache_key


def get_search_source_ids(cache_key: str | None) -> list[int] | None:
    if not cache_key:
        return None

    now = time.monotonic()
    with _LOCK:
        _prune_expired(now)
        entry = _CACHE.get(cache_key)
        if entry is None:
            return None
        _CACHE.move_to_end(cache_key)
        return list(entry.source_ids)


def get_search_rankings(cache_key: str | None) -> dict[str, Any] | None:
    if not cache_key:
        return None

    now = time.monotonic()
    with _LOCK:
        _prune_expired(now)
        entry = _CACHE.get(cache_key)
        if entry is None:
            return None
        if entry.rows is None:
            return None
        _CACHE.move_to_end(cache_key)
        return {
            "rows": _copy_rows(entry.rows),
            "ordered_indexes": _copy_ordered_indexes(entry.ordered_indexes),
        }


def put_search_rankings(
    cache_key: str | None,
    rows: list[dict[str, Any]],
    ordered_indexes: dict[str, list[int]],
) -> None:
    if not cache_key:
        return

    now = time.monotonic()
    with _LOCK:
        _prune_expired(now)
        entry = _CACHE.get(cache_key)
        if entry is None:
            return
        entry.rows = _copy_rows(rows)
        entry.ordered_indexes = _copy_ordered_indexes(ordered_indexes)
        entry.expires_at = now + _TTL_SECONDS
        _CACHE.move_to_end(cache_key)
        _evict_lru_if_needed()
