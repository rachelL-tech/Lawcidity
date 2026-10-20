import pytest
from pydantic import ValidationError

from app.api.schemas import (
    MAX_CASE_TEXT_CHARS,
    MAX_ISSUE_CHARS,
    MAX_ISSUES,
    MAX_STATUTES,
    AnalyzeRequest,
    GenerateRequest,
)


def test_analyze_accepts_text_at_limit():
    req = AnalyzeRequest(text="字" * MAX_CASE_TEXT_CHARS)
    assert len(req.text) == MAX_CASE_TEXT_CHARS


def test_analyze_rejects_text_over_limit():
    with pytest.raises(ValidationError):
        AnalyzeRequest(text="字" * (MAX_CASE_TEXT_CHARS + 1))


def test_generate_accepts_normal_request():
    req = GenerateRequest(
        query="騎機車遭人假車禍，沒有行車紀錄器，如何主張無過失？",
        issues=["被告是否有過失", "損害賠償範圍"],
        statutes=[{"law": "民法", "article": "184"}],
    )
    assert req.result_limit_per_issue == 10


@pytest.mark.parametrize(
    "overrides",
    [
        {"query": "字" * (MAX_CASE_TEXT_CHARS + 1)},
        {"issues": ["爭點"] * (MAX_ISSUES + 1)},
        {"issues": ["字" * (MAX_ISSUE_CHARS + 1)]},
        {"statutes": [{"law": "民法", "article": "184"}] * (MAX_STATUTES + 1)},
        {"result_limit_per_issue": 0},
        {"result_limit_per_issue": 21},
    ],
)
def test_generate_rejects_oversized_request(overrides):
    with pytest.raises(ValidationError):
        GenerateRequest(**{"query": "案例事實", **overrides})
