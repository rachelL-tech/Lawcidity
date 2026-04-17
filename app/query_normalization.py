"""
查詢條件正規化 helper。

職責：
- 關鍵字 dedupe / trim
- 法條條件 normalize + dedupe
- case_types 解析與驗證
"""

from etl.law_names import normalize_law_name


VALID_CASE_TYPES = {"民事", "刑事", "行政", "憲法"}


def _dedupe_keep_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value not in seen:
            seen.add(value)
            out.append(value)
    return out


def dedupe_query_terms(values: list[str]) -> list[str]:
    return _dedupe_keep_order([value.strip() for value in values if value and value.strip()])


def dedupe_statute_filters(
    values: list[tuple[str, str | None, str | None]]
) -> list[tuple[str, str | None, str | None]]:
    seen: set[tuple[str, str | None, str | None]] = set()
    out: list[tuple[str, str | None, str | None]] = []
    for law, article, sub_ref in values:
        normalized_law = normalize_law_name(law)
        key = (normalized_law, article, sub_ref)
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def parse_case_types(case_type_csv: str | None) -> list[str]:
    if not case_type_csv:
        return []
    values = [value.strip() for value in case_type_csv.split(",") if value.strip()]
    invalid = [value for value in values if value not in VALID_CASE_TYPES]
    if invalid:
        raise ValueError("case_type 僅支援：民事,刑事,行政,憲法")
    return _dedupe_keep_order(values)
