"""
tests/test_placeholder.py — upsert_target_placeholder() 邏輯測試

用 unittest.mock 模擬 DB 回傳，不需要真實 DB 連線。

測試重點（本體/metadata 分離）：
  T1: citation=判決，pool 無同 doc_type ph → INSERT 判決 placeholder
  T2: citation=判決，pool 有判決 ph → 回傳既有判決
  T3: citation=裁定，pool 有判決 ph → INSERT 裁定 placeholder（各自獨立）
  T4: citation=判例，pool 有判決 ph → INSERT NULL placeholder（不碰判決）
  T5: citation=None，pool 有裁定 ph → INSERT NULL placeholder（不碰裁定）
  T6: citation=判例，pool 有 NULL ph → 回傳 NULL placeholder
  T7b: citation=憲判字 → resolve 為憲判字，走 resolve 分支
"""
import sys
sys.path.insert(0, '.')
sys.path.insert(0, 'etl')

import pytest
from unittest.mock import MagicMock, patch

from etl.ingest_decisions import upsert_target_placeholder, _RESOLVABLE_DOC_TYPES


# ── Mock 工具 ─────────────────────────────────────────────────────────────────

def _make_conn(*fetchone_seq):
    """
    回傳 mock conn，每次 cursor.fetchone() 依序回傳 fetchone_seq 中的值。
    若序列耗盡則回傳 None。
    """
    conn = MagicMock()
    cur = MagicMock()
    remaining = list(fetchone_seq)

    def _fetchone():
        return remaining.pop(0) if remaining else None

    cur.fetchone.side_effect = _fetchone
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn


_BASE = dict(court="最高法院", jyear=110, jcase_norm="台上", jno=1,
             target_case_type="民事", source_case_type="民事")


# ── T1：citation=判決，pool 無同 doc_type ph → INSERT 判決 ───────────────────

def test_T1_判決_no_same_ph_inserts_判決():
    conn = _make_conn(None, None)  # full_row=None, ph=None
    with patch("etl.ingest_decisions._insert_placeholder", return_value=42) as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type="判決", **_BASE)
    mock_ins.assert_called_once_with(conn, "最高法院", 110, "台上", 1, "判決", "民事")
    assert result == 42


# ── T2：citation=判決，pool 有判決 ph → 回傳既有判決 ─────────────────────────

def test_T2_判決_same_ph_returns_existing():
    conn = _make_conn(None, (99,))  # full_row=None, ph=(99,)
    with patch("etl.ingest_decisions._insert_placeholder") as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type="判決", **_BASE)
    mock_ins.assert_not_called()
    assert result == 99


# ── T3：citation=裁定，pool 有判決 ph → INSERT 裁定（各自獨立）───────────────

def test_T3_裁定_ignores_判決_ph_inserts_裁定():
    # 查 doc_type='裁定' 的 full_row → None
    # 查 doc_type='裁定' 的 ph → None（不會去撈判決 ph）
    conn = _make_conn(None, None)
    with patch("etl.ingest_decisions._insert_placeholder", return_value=55) as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type="裁定", **_BASE)
    mock_ins.assert_called_once_with(conn, "最高法院", 110, "台上", 1, "裁定", "民事")
    assert result == 55


# ── T4：citation=判例，pool 有判決 ph → INSERT NULL（不碰判決）──────────────

def test_T4_判例_ignores_explicit_ph_inserts_null():
    # resolve_doc_type=None → 只查 doc_type IS NULL ph → None → INSERT NULL
    conn = _make_conn(None)  # null ph = None
    with patch("etl.ingest_decisions._insert_placeholder", return_value=77) as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type="判例", **_BASE)
    mock_ins.assert_called_once_with(conn, "最高法院", 110, "台上", 1, None, "民事")
    assert result == 77


# ── T5：citation=None，pool 有裁定 ph → INSERT NULL（不碰裁定）──────────────

def test_T5_None_ignores_explicit_ph_inserts_null():
    conn = _make_conn(None)  # null ph = None
    with patch("etl.ingest_decisions._insert_placeholder", return_value=88) as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type=None, **_BASE)
    mock_ins.assert_called_once_with(conn, "最高法院", 110, "台上", 1, None, "民事")
    assert result == 88


# ── T6：citation=判例，pool 有 NULL ph → 回傳 NULL placeholder ───────────────

def test_T6_判例_finds_null_ph():
    conn = _make_conn((33,))  # null ph = (33,)
    with patch("etl.ingest_decisions._insert_placeholder") as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type="判例", **_BASE)
    mock_ins.assert_not_called()
    assert result == 33


# ── T7b：citation=憲判字 → resolve 為憲判字，走 resolve 分支 ─────────────────

def test_T7b_憲判字_resolves_correctly():
    assert "憲判字" in _RESOLVABLE_DOC_TYPES
    conn = _make_conn(None, None)  # full_row=None, ph=None
    with patch("etl.ingest_decisions._insert_placeholder", return_value=11) as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type="憲判字", **_BASE)
    mock_ins.assert_called_once_with(conn, "最高法院", 110, "台上", 1, "憲判字", "民事")
    assert result == 11


# ── 額外：裁判 → resolve_doc_type=None（同 判例）────────────────────────────

def test_裁判_treated_as_unresolved():
    """裁判不在 RESOLVABLE_DOC_TYPES，應走 None 分支，只找/建 NULL placeholder"""
    conn = _make_conn(None)
    with patch("etl.ingest_decisions._insert_placeholder", return_value=22) as mock_ins:
        result = upsert_target_placeholder(conn, target_doc_type="裁判", **_BASE)
    mock_ins.assert_called_once_with(conn, "最高法院", 110, "台上", 1, None, "民事")
    assert result == 22
