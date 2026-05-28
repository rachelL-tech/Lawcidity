import pytest
from app.rag_service import _aggregate

def test_md5_dedup():
    # arrange
    same_text = "依民法第184條規定，行為人應負損害賠償責任。"
    rows = [
        {"chunk_text": same_text, "distance": 0.1, "court_level": 1, "decision_id": 1,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
        {"chunk_text": same_text, "distance": 0.1, "court_level": 2, "decision_id": 2,
        "root_norm": "B", "display_title": "B", "doc_type": "X",
        "target_ids": [], "target_authority_ids": []},
    ]

    # act
    result = _aggregate(rows, result_limit=10)

    # assert
    assert len(result) == 1
    assert len(result[0]["other_sources"]) == 1

@pytest.mark.parametrize("rows, expected_id", [
    # court_level 小者勝
    ([
        {"chunk_text": "同文字", "distance": 0.1, "court_level": 2, "decision_id": 1,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
        {"chunk_text": "同文字", "distance": 0.1, "court_level": 1, "decision_id": 2,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
    ], 2),

    # None 排最後
    ([
          {"chunk_text": "同文字", "distance": 0.1, "court_level": None, "decision_id": 1,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
          {"chunk_text": "同文字", "distance": 0.1, "court_level": 2,    "decision_id": 2,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
    ], 2),

    # 平手時 decision_id 小者勝
    ([
          {"chunk_text": "同文字", "distance": 0.1, "court_level": 1, "decision_id": 3,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
          {"chunk_text": "同文字", "distance": 0.1, "court_level": 1, "decision_id": 1,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
    ], 1),
])
def test_primary(rows, expected_id):
    results = _aggregate(rows, result_limit=10)
    assert results[0]["decision_id"] == expected_id

def test_sim_and_limit():
    rows = [
        {"chunk_text": "測試1", "distance": 0.1, "court_level": 1, "decision_id": 1,
           "root_norm": "A", "display_title": "A", "doc_type": "X",
           "target_ids": [], "target_authority_ids": []},
        {"chunk_text": "測試2", "distance": 0.3, "court_level": 2, "decision_id": 2,
        "root_norm": "B", "display_title": "B", "doc_type": "X",
        "target_ids": [], "target_authority_ids": []},
        {"chunk_text": "測試3", "distance": 0.5, "court_level": 2, "decision_id": 3,
        "root_norm": "B", "display_title": "C", "doc_type": "X",
        "target_ids": [], "target_authority_ids": []},
    ]
    results = _aggregate(rows, result_limit=2)
    assert len(results) == 2
    assert results[0]["decision_id"] == 1
    assert results[1]["decision_id"] == 2  
