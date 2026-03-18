"""
tests/test_citation_parser_next.py — citation_parser_next 穩定規則回歸測試
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from etl.citation_parser_next import extract_citations_next, find_snippet_start, find_snippet_end


def _pos(text: str, sub: str) -> tuple[int, int]:
    idx = text.find(sub)
    if idx == -1:
        pytest.fail(f"找不到 {sub!r} in text")
    return idx, idx + len(sub)


def extract_citations(text, **kw):
    return [c.to_dict() for c in extract_citations_next(text, **kw)]


def extract_snippet(text, start, end, **kw):
    ss = find_snippet_start(text, start)
    se = find_snippet_end(text, start, end)
    return text[ss:se]


def test_r011_judgment_can_kao_retained():
    text = (
        "理由\r\n"
        "一、按民法第184條規定，行為人應負損害賠償責任，"
        "最高法院88年度台上字第1346號判決可參）。\r\n"
        "二、被告抗辯..."
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert any(r["raw_match"].startswith("最高法院88年度台上字第1346號") for r in decision_results)


def test_r011_bare_page_reference_retained():
    text = (
        "理由\r\n"
        "一、按誠信原則之適用，最高法院98年度台上字第1235號判決第360頁等見解，"
        "可認本件請求為有理由。\r\n"
        "二、..."
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert any(r["raw_match"].startswith("最高法院98年度台上字第1235號") for r in decision_results)


def test_r011_in_juan_ke_can_still_filtered():
    text = (
        "理由\r\n"
        "一、原告另提出最高法院110年度台上字第4475號刑事判決"
        "（見本院卷第357至358頁）為證。\r\n"
        "二、..."
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert decision_results == []


def test_r008_yunyun_guard_does_not_cross_next_heading():
    text = (
        "理由\r\n"
        "一、按法律關係不明確時，得提起確認之訴，最高法院88年度台抗字第161號裁定意旨參照。\r\n"
        "二、聲請人聲請訴訟救助，固陳稱：伊一貧如洗，生活困難，無資力支付訴訟費用云云，"
        "尚非可採。\r\n"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert any(r["raw_match"].startswith("最高法院88年度台抗字第161號") for r in decision_results)


def test_authority_snippet_keeps_full_quoted_sentence():
    text = (
        "理由\r\n"
        "㈡憲法法庭111年憲判字第11號判決理由書闡釋略以：「依103年教師法規定，"
        "教師涉有解聘事由者，應依正當法律程序審理，與單純行政管理措施有別。」"
        "足供參照。\r\n"
    )
    start, end = _pos(text, "憲法法庭111年憲判字第11號判決")
    snip = extract_snippet(text, start, end, authority_mode=True)
    assert "理由書闡釋略以：" in snip
    assert snip.endswith("有別。」")
