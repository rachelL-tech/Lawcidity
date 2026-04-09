import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.opensearch_service import (
    chunk_source_ids,
    build_source_target_rerank_query,
    aggregate_source_target_hits_to_rankings,
    calculate_source_target_match_score,
    fetch_target_rankings_by_relevance,
    _fetch_authority_target_metadata,
)


class _AuthorityMetaCursor:
    def __init__(self):
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return [
            {
                "target_authority_id": 301,
                "court": "司法院",
                "display_title": "司法院解釋",
                "doc_type": "解釋",
                "total_citation_count": 12,
            }
        ]


class _AuthorityMetaConn:
    def __init__(self):
        self.cursor_obj = _AuthorityMetaCursor()

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


def test_fetch_authority_target_metadata_reads_denormalized_total_citation_count():
    conn = _AuthorityMetaConn()

    rows = _fetch_authority_target_metadata(conn, [301])

    sql, params = conn.cursor_obj.executed[0]
    assert "a.total_citation_count" in sql
    assert "COUNT(DISTINCT c.source_id)" not in sql
    assert params == {"authority_ids": [301]}
    assert rows[301]["total_citation_count"] == 12


def test_build_source_target_rerank_query_uses_dis_max_per_term():
    query = build_source_target_rerank_query(
        query_terms=["詐欺", "洗錢"],
        source_ids=[11, 22],
        statute_filters=[("洗錢防制法", "19", None)],
        exclude_terms=["不當得利"],
        exclude_statute_filters=[("民法", "184", None)],
        size=50,
    )

    assert query["size"] == 50
    assert "track_total_hits" not in query
    assert query["sort"] == ["_doc"]
    assert query["_source"] == [
        "source_id",
        "target_id",
        "target_authority_id",
        "target_uid",
    ]

    bool_query = query["query"]["bool"]
    assert {"terms": {"source_id": [11, 22]}} in bool_query["filter"]
    assert "should" in bool_query
    assert len(bool_query["should"]) == 3
    assert bool_query["should"][0] == {
        "match_phrase": {
            "window_text_snippet": {
                "query": "詐欺",
                "_name": "t0:snippet",
            }
        }
    }
    assert any(
        clause.get("nested", {}).get("path") == "statutes"
        for clause in bool_query["should"]
    )
    assert not any(
        filt.get("bool", {}).get("should")
        for filt in bool_query["filter"]
        if isinstance(filt, dict)
    )
    assert "must_not" not in bool_query


def test_build_source_target_rerank_query_keeps_statutes_as_optional_should_without_keywords():
    query = build_source_target_rerank_query(
        query_terms=[],
        source_ids=[11, 22],
        statute_filters=[("洗錢防制法", "19", None)],
        size=50,
    )

    bool_query = query["query"]["bool"]
    assert {"terms": {"source_id": [11, 22]}} in bool_query["filter"]
    assert any(
        clause.get("nested", {}).get("path") == "statutes"
        for clause in bool_query["should"]
    )
    assert not any(
        filt.get("bool", {}).get("should")
        for filt in bool_query["filter"]
        if isinstance(filt, dict)
    )


def test_build_source_target_rerank_query_moves_keyword_clauses_to_optional_should():
    query = build_source_target_rerank_query(
        query_terms=["中鼎"],
        source_ids=[11, 22],
        statute_filters=[],
        size=50,
    )

    bool_query = query["query"]["bool"]
    assert {"terms": {"source_id": [11, 22]}} in bool_query["filter"]
    assert "should" in bool_query
    assert len(bool_query["should"]) == 1
    assert bool_query["should"][0] == {
        "match_phrase": {
            "window_text_snippet": {
                "query": "中鼎",
                "_name": "t0:snippet",
            }
        }
    }
    assert not any(
        filt.get("bool", {}).get("should")
        for filt in bool_query["filter"]
        if isinstance(filt, dict)
    )


def test_build_source_target_rerank_query_applies_target_filters_when_provided():
    query = build_source_target_rerank_query(
        query_terms=["詐欺"],
        source_ids=[11, 22],
        statute_filters=[],
        size=50,
        target_ids=[9001, 9002],
        target_authority_ids=[77],
    )

    bool_query = query["query"]["bool"]
    assert {"terms": {"target_id": [9001, 9002]}} in bool_query["filter"]
    assert {"terms": {"target_authority_id": [77]}} in bool_query["filter"]


def test_build_source_target_rerank_query_sets_minimum_should_match_when_provided():
    query = build_source_target_rerank_query(
        query_terms=["詐欺"],
        source_ids=[11, 22],
        statute_filters=[],
        size=50,
        minimum_should_match=1,
    )

    assert query["query"]["bool"]["minimum_should_match"] == 1


def test_calculate_source_target_match_score_uses_best_bucket_per_term():
    score = calculate_source_target_match_score(
        [
            "t0:snippet",
            "t1:snippet",
            "st0",
            "st1",
            "ignored",
        ]
    )

    assert score == 4.0


def test_calculate_source_target_match_score_counts_each_statute_filter_once():
    score = calculate_source_target_match_score(
        [
            "t0:snippet",
            "st0",
            "st0",
            "st0",
        ]
    )

    assert score == 2.0


def test_search_source_target_hits_opensearch_builds_scores_from_matched_queries(monkeypatch):
    from app import opensearch_service

    class FakeClient:
        def __init__(self):
            self.cleared_scroll_ids = []

        def search(self, index, body, scroll=None):
            assert scroll == "1m"
            return {
                "_scroll_id": "dummy-scroll",
                "hits": {
                    "hits": [
                        {
                            "matched_queries": ["t0:snippet", "t1:post_200"],
                            "_source": {
                                "source_id": 10,
                                "target_id": 9001,
                                "target_authority_id": None,
                                "target_uid": "decision:9001",
                            },
                        },
                        {
                            "matched_queries": ["t0:post_100"],
                            "_source": {
                                "source_id": 11,
                                "target_id": 9001,
                                "target_authority_id": None,
                                "target_uid": "decision:9001",
                            },
                        },
                    ]
                }
            }

        def scroll(self, scroll_id, scroll=None):
            assert scroll_id == "dummy-scroll"
            assert scroll == "1m"
            return {
                "_scroll_id": "dummy-scroll-2",
                "hits": {"hits": []},
            }

        def clear_scroll(self, scroll_id):
            self.cleared_scroll_ids.append(scroll_id)

    fake_client = FakeClient()
    monkeypatch.setattr(opensearch_service, "_get_opensearch_client", lambda: fake_client)

    hits = opensearch_service.search_source_target_hits_opensearch(
        query_terms=["詐欺", "洗錢"],
        source_ids=[10, 11],
        statute_filters=[],
        exclude_terms=[],
        exclude_statute_filters=[],
        max_hits=10,
    )

    assert hits == [
        {
            "source_id": 10,
            "target_id": 9001,
            "target_authority_id": None,
            "score": 1.0,
        },
        {
            "source_id": 11,
            "target_id": 9001,
            "target_authority_id": None,
            "score": 0.0,
        },
    ]
    assert fake_client.cleared_scroll_ids == ["dummy-scroll"]


def test_search_source_target_hits_opensearch_stops_before_opening_zero_size_scroll(monkeypatch):
    from app import opensearch_service

    class FakeClient:
        def __init__(self):
            self.search_calls = []
            self.scroll_calls = []

        def search(self, index, body, scroll=None):
            self.search_calls.append(
                (
                    tuple(body["query"]["bool"]["filter"][0]["terms"]["source_id"]),
                    body["size"],
                    scroll,
                )
            )
            return {
                "_scroll_id": "dummy-scroll",
                "hits": {
                    "hits": [
                        {
                            "matched_queries": ["t0:snippet"],
                            "_source": {
                                "source_id": 10,
                                "target_id": 9001,
                                "target_authority_id": None,
                                "target_uid": "decision:9001",
                            },
                        }
                    ]
                }
            }

        def scroll(self, scroll_id, scroll=None):
            self.scroll_calls.append((scroll_id, scroll))
            return {
                "_scroll_id": "dummy-scroll",
                "hits": {"hits": []},
            }

        def clear_scroll(self, scroll_id):
            return None

    fake_client = FakeClient()
    monkeypatch.setattr(opensearch_service, "_get_opensearch_client", lambda: fake_client)
    monkeypatch.setenv("OPENSEARCH_SOURCE_TARGET_SOURCE_CHUNK_SIZE", "1")
    monkeypatch.setenv("OPENSEARCH_SOURCE_TARGET_SCROLL_TTL", "5m")

    hits = opensearch_service.search_source_target_hits_opensearch(
        query_terms=["詐欺"],
        source_ids=[10, 11],
        statute_filters=[],
        exclude_terms=[],
        exclude_statute_filters=[],
        max_hits=1,
    )

    assert len(hits) == 1
    assert fake_client.search_calls == [((10,), 1, "1m")]
    assert fake_client.scroll_calls == []


def test_aggregate_source_target_hits_to_rankings_averages_top5_distinct_sources_without_citation_dedupe():
    hits = [
        {"source_id": 10, "target_id": 9001, "target_authority_id": None, "score": 4.0},
        {"source_id": 11, "target_id": 9001, "target_authority_id": None, "score": 1.0},
        {"source_id": 12, "target_id": 9001, "target_authority_id": None, "score": 2.0},
        {"source_id": 13, "target_id": 9001, "target_authority_id": None, "score": 1.0},
        {"source_id": 14, "target_id": 9001, "target_authority_id": None, "score": 0.5},
        {"source_id": 15, "target_id": 9001, "target_authority_id": None, "score": 0.2},
        {"source_id": 21, "target_id": None, "target_authority_id": 301, "score": 1.2},
    ]
    decision_meta = {
        9001: {
            "canonical_id": 9001,
            "target_id": 9001,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": 123,
            "display_title": "最高法院111年度台上字第123號判決",
            "doc_type": "判決",
            "total_citation_count": 88,
        }
    }
    authority_meta = {
        301: {
            "target_id": None,
            "target_authority_id": 301,
            "court": "司法院",
            "court_level": None,
            "jyear": None,
            "jcase_norm": None,
            "jno": None,
            "display_title": "司法院解釋",
            "doc_type": "解釋",
            "total_citation_count": 12,
        }
    }

    rankings = aggregate_source_target_hits_to_rankings(
        hits,
        decision_meta,
        authority_meta,
    )

    assert rankings == [
        {
            "target_id": 9001,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": 123,
            "display_title": "最高法院111年度台上字第123號判決",
            "doc_type": "判決",
            "total_citation_count": 88,
            "matched_citation_count": 6,
            "score": 1.7,
            "ranked_source_ids": [10, 12, 11, 13, 14],
        },
        {
            "target_id": None,
            "target_authority_id": 301,
            "court": "司法院",
            "court_level": None,
            "jyear": None,
            "jcase_norm": None,
            "jno": None,
            "display_title": "司法院解釋",
            "doc_type": "解釋",
            "total_citation_count": 12,
            "matched_citation_count": 1,
            "score": 1.2,
            "ranked_source_ids": [21],
        },
    ]


def test_chunk_source_ids_splits_large_source_lists_without_losing_order():
    chunks = chunk_source_ids(list(range(1, 8)), chunk_size=3)

    assert chunks == [
        [1, 2, 3],
        [4, 5, 6],
        [7],
    ]


def test_fetch_target_rankings_by_relevance_returns_rankings_list(monkeypatch):
    hits = [
        {
            "source_id": 10,
            "target_id": 9001,
            "target_authority_id": None,
            "score": 4.0,
        },
        {
            "source_id": 11,
            "target_id": 9001,
            "target_authority_id": None,
            "score": 1.0,
        },
    ]
    decision_meta = {
        9001: {
            "canonical_id": 9001,
            "target_id": 9001,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": 123,
            "display_title": "最高法院111年度台上字第123號判決",
            "doc_type": "判決",
            "total_citation_count": 88,
        }
    }

    monkeypatch.setattr(
        "app.opensearch_service.search_source_target_hits_opensearch",
        lambda **kwargs: hits,
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_decision_target_metadata",
        lambda conn, ids: decision_meta,
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_authority_target_metadata",
        lambda conn, ids: {},
    )

    rankings = fetch_target_rankings_by_relevance(
        object(),
        [1, 2, 3],
        ["詐欺"],
        [],
        [],
        [],
    )

    assert isinstance(rankings, list)
    assert rankings == [
        {
            "target_id": 9001,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": 123,
            "display_title": "最高法院111年度台上字第123號判決",
            "doc_type": "判決",
            "total_citation_count": 88,
            "matched_citation_count": 2,
            "score": 2.5,
            "ranked_source_ids": [10, 11],
        }
    ]


def test_fetch_target_rankings_by_relevance_falls_back_when_strict_hits_empty(monkeypatch):
    decision_meta = {
        9001: {
            "canonical_id": 9001,
            "target_id": 9001,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": 123,
            "display_title": "最高法院111年度台上字第123號判決",
            "doc_type": "判決",
            "total_citation_count": 88,
        }
    }
    calls = []

    def fake_search_source_target_hits_opensearch(**kwargs):
        calls.append(kwargs["minimum_should_match"])
        if kwargs["minimum_should_match"] == 1:
            return []
        return [
            {
                "source_id": 10,
                "target_id": 9001,
                "target_authority_id": None,
                "score": 1.0,
            }
        ]

    monkeypatch.setattr(
        "app.opensearch_service.search_source_target_hits_opensearch",
        fake_search_source_target_hits_opensearch,
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_decision_target_metadata",
        lambda conn, ids: decision_meta,
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_authority_target_metadata",
        lambda conn, ids: {},
    )

    rankings = fetch_target_rankings_by_relevance(
        object(),
        [1, 2, 3],
        ["牛肉麵"],
        [],
        [],
        [],
    )

    assert calls == [1, None]
    assert rankings[0]["target_id"] == 9001


def test_fetch_target_rankings_by_relevance_appends_fallback_when_strict_rankings_insufficient(monkeypatch):
    decision_meta = {
        9001: {
            "canonical_id": 9001,
            "target_id": 9001,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": 123,
            "display_title": "最高法院111年度台上字第123號判決",
            "doc_type": "判決",
            "total_citation_count": 88,
        },
        9002: {
            "canonical_id": 9002,
            "target_id": 9002,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": 124,
            "display_title": "最高法院111年度台上字第124號判決",
            "doc_type": "判決",
            "total_citation_count": 55,
        },
    }
    calls = []

    def fake_search_source_target_hits_opensearch(**kwargs):
        calls.append(kwargs["minimum_should_match"])
        if kwargs["minimum_should_match"] == 1:
            return [
                {
                    "source_id": 10,
                    "target_id": 9001,
                    "target_authority_id": None,
                    "score": 2.0,
                }
            ]
        return [
            {
                "source_id": 11,
                "target_id": 9001,
                "target_authority_id": None,
                "score": 1.0,
            },
            {
                "source_id": 12,
                "target_id": 9002,
                "target_authority_id": None,
                "score": 1.0,
            },
        ]

    monkeypatch.setattr(
        "app.opensearch_service.search_source_target_hits_opensearch",
        fake_search_source_target_hits_opensearch,
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_decision_target_metadata",
        lambda conn, ids: decision_meta,
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_authority_target_metadata",
        lambda conn, ids: {},
    )

    rankings = fetch_target_rankings_by_relevance(
        object(),
        [1, 2, 3],
        ["損害賠償"],
        [],
        [],
        [],
    )

    assert calls == [1, None]
    assert [row["target_id"] for row in rankings] == [9001, 9002]


def test_fetch_target_rankings_by_relevance_skips_fallback_when_strict_rankings_reach_threshold(monkeypatch):
    decision_meta = {
        9000 + idx: {
            "canonical_id": 9000 + idx,
            "target_id": 9000 + idx,
            "target_authority_id": None,
            "court": "最高法院",
            "court_level": 1,
            "jyear": 111,
            "jcase_norm": "台上",
            "jno": idx,
            "display_title": f"最高法院111年度台上字第{idx}號判決",
            "doc_type": "判決",
            "total_citation_count": 88 - idx,
        }
        for idx in range(200)
    }
    calls = []

    def fake_search_source_target_hits_opensearch(**kwargs):
        calls.append(kwargs["minimum_should_match"])
        return [
            {
                "source_id": 1000 + idx,
                "target_id": 9000 + idx,
                "target_authority_id": None,
                "score": 1.0,
            }
            for idx in range(200)
        ]

    monkeypatch.setattr(
        "app.opensearch_service.search_source_target_hits_opensearch",
        fake_search_source_target_hits_opensearch,
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_decision_target_metadata",
        lambda conn, ids: {target_id: decision_meta[target_id] for target_id in ids},
    )
    monkeypatch.setattr(
        "app.opensearch_service._fetch_authority_target_metadata",
        lambda conn, ids: {},
    )

    rankings = fetch_target_rankings_by_relevance(
        object(),
        [1, 2, 3],
        ["損害賠償"],
        [],
        [],
        [],
    )

    assert calls == [1]
    assert len(rankings) == 200
