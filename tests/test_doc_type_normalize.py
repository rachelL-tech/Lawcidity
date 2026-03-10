"""
tests/test_doc_type_normalize.py — citation_parser doc_type 正規化測試

測試重點：
- 憲判字第N號後面寫「判決」時，doc_type 應回傳憲判字（T7）
- 裁判/理由 → None
- 判例 保留 raw（不 resolve 為本體）
- 判決/裁定 照字面
"""
import sys
sys.path.insert(0, '.')

import pytest
from etl.citation_parser import extract_citations


def _citations_from(text: str) -> list:
    return extract_citations(text)


def _first(text: str) -> dict:
    results = _citations_from(text)
    assert results, f"沒有 citation 被抽出：{text!r}"
    return results[0]


# ── T7：憲判字 + 後綴「判決」→ doc_type 應為憲判字 ──────────────────────────

def test_憲判字_suffix_判決_overrides():
    """憲法法庭112年度憲判字第2號判決 → doc_type='憲判字'，不是'判決'"""
    text = "憲法法庭112年度憲判字第2號判決意旨參照。"
    c = _first(text)
    assert c["jcase_norm"] == "憲判", f"jcase_norm={c['jcase_norm']!r}"
    assert c["doc_type"] == "憲判字", f"doc_type={c['doc_type']!r}，應為憲判字"


def test_憲判字_no_suffix():
    """憲法法庭112年度憲判字第2號（無後綴）→ doc_type='憲判字'"""
    text = "憲法法庭112年度憲判字第2號意旨參照。"
    c = _first(text)
    assert c["doc_type"] == "憲判字"


def test_憲判字_suffix_裁定_overrides():
    """憲判字後面寫「裁定」仍應回傳憲判字"""
    text = "憲法法庭112年度憲判字第3號裁定參照。"
    c = _first(text)
    assert c["doc_type"] == "憲判字"


# ── 裁判/理由 → None ──────────────────────────────────────────────────────────

def test_裁判_normalizes_to_none():
    """最高法院110年度台上字第1號裁判 → doc_type=None"""
    text = "最高法院110年度台上字第1號裁判意旨參照。"
    c = _first(text)
    assert c["doc_type"] is None, f"doc_type={c['doc_type']!r}，應為 None"


def test_理由_normalizes_to_none():
    """最高法院110年度台上字第1號理由 → doc_type=None"""
    text = "最高法院110年度台上字第1號理由參照。"
    c = _first(text)
    assert c["doc_type"] is None


# ── 判例 保留 raw ──────────────────────────────────────────────────────────────

def test_判例_preserved():
    """最高法院40年台上字第86號判例 → doc_type='判例'（citation metadata 保留）"""
    text = "最高法院40年台上字第86號判例意旨參照。"
    c = _first(text)
    assert c["doc_type"] == "判例"


# ── 判決/裁定 正常路徑 ────────────────────────────────────────────────────────

def test_判決_preserved():
    text = "最高法院110年度台上字第1號判決意旨參照。"
    c = _first(text)
    assert c["doc_type"] == "判決"


def test_裁定_preserved():
    text = "最高法院110年度台抗字第1號裁定參照。"
    c = _first(text)
    assert c["doc_type"] == "裁定"
