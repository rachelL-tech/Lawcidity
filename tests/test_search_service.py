import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.opensearch_service import (
    build_base_citations_cte_sql,
    build_statute_hits_cte_sql,
)


def test_build_statute_hits_cte_sql_aggregates_citation_scores_once():
    params = {}
    sql_parts = build_statute_hits_cte_sql(
        [("民法", "184", "第1項"), ("民法", "185", None)],
        params,
        source_alias="c",
    )

    assert "statute_filter_inputs" in sql_parts["cte_sql"]
    assert "statute_hits AS" in sql_parts["cte_sql"]
    assert "COUNT(DISTINCT fi.filter_id) AS st_score" in sql_parts["cte_sql"]
    assert "EXISTS (" not in sql_parts["cte_sql"]
    assert sql_parts["join_sql"] == "LEFT JOIN statute_hits sh ON sh.citation_id = c.id"
    assert sql_parts["score_sql"] == "COALESCE(sh.st_score, 0)"
    assert params == {
        "st_law_0": "民法",
        "st_article_0": "184",
        "st_sub_ref_0": "第1項",
        "st_law_1": "民法",
        "st_article_1": "185",
    }


def test_build_statute_hits_cte_sql_returns_noop_parts_when_no_filters():
    sql_parts = build_statute_hits_cte_sql([], {}, source_alias="c")

    assert sql_parts == {
        "cte_sql": "",
        "join_sql": "",
        "score_sql": "0",
    }


def test_build_base_citations_cte_sql_reuses_citations_scan_and_scores():
    sql = build_base_citations_cte_sql(
        keyword_score_sql="(c.snippet ILIKE %(kw_0)s)::int",
        statute_score_sql="COALESCE(sh.st_score, 0)",
        statute_join_sql="LEFT JOIN statute_hits sh ON sh.citation_id = c.id",
    )

    assert "base_citations AS" in sql
    assert "FROM citations c" in sql
    assert "JOIN src s ON s.source_id = c.source_id" in sql
    assert "LEFT JOIN statute_hits sh ON sh.citation_id = c.id" in sql
    assert "(c.snippet ILIKE %(kw_0)s)::int AS kw_score" in sql
    assert "COALESCE(sh.st_score, 0) AS st_score" in sql
