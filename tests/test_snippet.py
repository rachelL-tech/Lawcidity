"""
tests/test_snippet.py — extract_snippet() + extract_citations() 回歸測試

每個 case 來自真實 DB citation，說明當初的 bug 是什麼。
跑法：python -m pytest tests/test_snippet.py -v
"""
import sys
sys.path.insert(0, 'etl')

import pytest
from etl.citation_parser import extract_snippet, extract_citations


# ─── 工具 ─────────────────────────────────────────────────────────────────────

def _pos(text: str, sub: str) -> tuple[int, int]:
    """在 text 裡找 sub，回傳 (start, end)；找不到就 pytest.fail"""
    idx = text.find(sub)
    if idx == -1:
        pytest.fail(f"找不到 {sub!r} in text")
    return idx, idx + len(sub)


# ─── Case 1：標準按字開頭（最基本情境）─────────────────────────────────────────

def test_standard_按_prefix():
    """snippet 應從「按」開始，到 citation 參照 ）結束"""
    text = (
        "理由\r\n"
        "一、按不法侵害他人之權利，負損害賠償責任，民法第184條定有明文。"
        "最高法院113年度台上字第1號判決意旨參照）。\r\n"
        "二、被告行為..."
    )
    start, end = _pos(text, "最高法院113年度台上字第1號")
    snip = extract_snippet(text, start, end)
    assert "按不法侵害他人之權利" in snip
    assert "）" in snip
    assert "二、被告行為" not in snip


# ─── Case 2：引用鏈（#9286 的修法——不推進 actual_start）────────────────────────

def test_citation_chain_preserves_first():
    """引用鏈 A、B、C 三號，算 C 的 snippet 時不應跳過 A"""
    text = (
        "按法律關係不明確，原告有即受確認判決之法律上利益，"
        "最高法院42年台上字第1031號、52年台上字第1237號、52年台上字第1240號判例參照。\r\n"
        "二、..."
    )
    start, end = _pos(text, "52年台上字第1240號")
    snip = extract_snippet(text, start, end)
    # 三個號碼都應出現
    assert "42年台上字第1031號" in snip
    assert "52年台上字第1237號" in snip
    assert "52年台上字第1240號" in snip


# ─── Case 3：段落起頭 ㈢（前有空白）────────────────────────────────────────────

def test_para_start_with_leading_whitespace():
    """\\r\\n  ㈢ 前有兩個空白，仍應被辨識為段落起頭"""
    text = (
        "一、...\r\n"
        "  ㈢慰藉金：按不法侵害他人，應負賠償責任。"
        "最高法院100年台上字第1號判決意旨參照）。\r\n"
        "二、..."
    )
    start, end = _pos(text, "最高法院100年台上字第1號")
    snip = extract_snippet(text, start, end)
    assert "㈢慰藉金" in snip
    assert "二、" not in snip


# ─── Case 4：阿拉伯數字條號（#16566 類型）─────────────────────────────────────

def test_arabic_numeral_section_header():
    """\\r\\n8 精神慰撫金 格式應被辨識為段落起頭"""
    text = (
        "一、...\r\n"
        "8 精神慰撫金：又不法侵害他人，被害人得請求慰撫金。"
        "最高法院51年台上字第223號判例參照）。\r\n"
        "9 ..."
    )
    start, end = _pos(text, "最高法院51年台上字第223號")
    snip = extract_snippet(text, start, end)
    assert "8 精神慰撫金" in snip


# ─── Case 5：甲乙丙大綱字（#14038 類型）────────────────────────────────────────

def test_jia_yi_bing_section_header():
    """\\r\\n甲、程序部分：按... 應從「按」開始（非從「甲」）"""
    text = (
        "裁定如下：\r\n"
        "甲、程序部分：按「確認法律關係之訴」，民事訴訟法第247條。"
        "最高法院52年台上字第1240號判決意旨參照）。\r\n"
        "乙、實體部分：..."
    )
    start, end = _pos(text, "最高法院52年台上字第1240號")
    snip = extract_snippet(text, start, end)
    # 段落起頭從甲，sub_clause 從按——取較近的按
    assert "按「確認法律關係之訴」" in snip


# ─── Case 6：半形 ) 結尾（#15058 類型）─────────────────────────────────────────

def test_forward_halfwidth_paren():
    """citation 收尾為半形 )，不應擴展到後面的全形 ）"""
    text = (
        "按共同侵權行為人應連帶負責"
        "(最高法院78年度台上字第2479號判決要旨參照)。"
        "被告確有前開侵權行為，業如前述。\r\n（三）再按..."
    )
    start, end = _pos(text, "最高法院78年度台上字第2479號")
    snip = extract_snippet(text, start, end)
    assert snip.endswith("參照)")
    assert "被告確有前開侵權行為" not in snip


# ─── Case 7：（最高法院 不應被誤判為段落起頭 ─────────────────────────────────

def test_opening_paren_citation_not_para_start():
    """（最高法院56年... 的 （ 不應被誤認為 （一）(二) 的段落起頭"""
    text = (
        "一、按執票人向本票發票人行使追索權時，得聲請法院裁定後強制執行。"
        "又本票執票人...以資解決。\r\n"
        "    （最高法院56年度台抗字第714號、57年度台抗字第76號裁定意旨參照）。\r\n"
        "二、相對人聲請意旨..."
    )
    start, end = _pos(text, "57年度台抗字第76號")
    snip = extract_snippet(text, start, end)
    # 應包含前文法律論述，不應只從（最高法院 開始
    assert "執票人向本票發票人行使追索權" in snip or "以資解決" in snip


# ─── Case 8：意旨參照） 的向後截斷 ───────────────────────────────────────────

def test_forward_yizhi_paren():
    """向後找到 ）後截止，不繼續延伸"""
    text = (
        "按侵權行為，民法第184條定有明文。"
        "最高法院113年度台上字第999號判決意旨參照）。\r\n"
        "二、被告行為..."
    )
    start, end = _pos(text, "最高法院113年度台上字第999號")
    snip = extract_snippet(text, start, end)
    assert snip.endswith("參照）")
    assert "二、被告行為" not in snip


# ─── Case 9：前案程序史（駁回確定）不應產生 citation ─────────────────────────

def test_prior_case_dismissed_filtered():
    """
    被告前案歷史敘述（判決上訴駁回確定）不應被計入 citation。
    刑事判決常見：「並由最高法院XXX號判決上訴駁回確定」。
    """
    text = (
        "被告前因詐欺案件，經臺灣高等法院以108年度上訴字第100號判決後，"
        "並由最高法院109年度台上字第500號判決上訴駁回確定，"
        "於109年5月1日執行完畢等情，有前案紀錄表在卷可參。"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"前案程序史不應產生 decision citation，但得到：{decision_results}"
    )


# ─── Case 10：卷證附件引用（見本院卷）不應產生 citation ──────────────────────

def test_evidence_exhibit_filtered():
    """
    判決作為卷宗附件提出（見本院卷第N頁）時不應被計入 citation。
    行政/刑事判決常見：列舉附件時提到最高法院判決。
    """
    text = (
        "肆、兩造不爭之事實：\r\n"
        "如事實概要欄所述，業據提出臺灣新北地方法院108年度訴字第1060號刑事判決"
        "（見本院卷第345至348頁）、最高法院110年度台上字第4475號刑事判決"
        "（見本院卷第357至358頁）、原告113年3月22日申請書為證。"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"卷證附件引用不應產生 decision citation，但得到：{decision_results}"
    )


# ─── Case 11：裸「惟」子條款起頭（不接按/查/依）─────────────────────────────────

def test_bare_wei_sub_clause():
    """惟（不接按/查/依）應被辨識為子條款起頭，snippet 從「惟」開始"""
    text = (
        "一、按某某法律說明如下。\r\n"
        "惟行為人若具備責任能力，被害人仍得請求損害賠償。"
        "最高法院99年度台上字第1號判決意旨參照）。\r\n"
        "二、被告行為..."
    )
    start, end = _pos(text, "最高法院99年度台上字第1號")
    snip = extract_snippet(text, start, end)
    assert "惟行為人若" in snip
    assert "二、被告行為" not in snip


# ─── Case 12：釋字與 target 同句，Pass 2 不推進 actual_start ──────────────────

def test_grand_interp_same_clause_no_advance():
    """
    釋字與 target citation 同一子句（之間無句號/換行），
    actual_start 不應推進到釋字之後（避免只剩「及最高法院…」）。
    """
    text = (
        "按某法律規定。\r\n"
        "又依司法院大法官釋字第775號解釋及"
        "最高法院110年度台上大字第5660號裁定意旨，自應認定如此。\r\n"
        "二、..."
    )
    start, end = _pos(text, "最高法院110年度台上大字第5660號")
    snip = extract_snippet(text, start, end)
    # 同句 guard → actual_start 留在「又依」→ snippet 包含釋字
    assert "釋字第775號" in snip
    assert "又依" in snip


# ─── Case 13：座談會「研討結果參照）。」完整吸收 ─────────────────────────────────

def test_conference_result_trail_absorbed():
    """
    座談會 trailing「研討結果參照）。」應被 _AUTH_TRAIL_RE 完整吸收，
    不應以「果參照）。」截斷開頭。
    """
    text = (
        "按某法律規定。\r\n"
        "臺灣高等法院暨所屬法院112年法律座談會刑事類提案第3號研討結果參照）。"
        "最高法院96年度台上字第3409號判決意旨參照）。\r\n"
        "二、..."
    )
    start, end = _pos(text, "最高法院96年度台上字第3409號")
    snip = extract_snippet(text, start, end)
    # 修復前：_AUTH_TRAIL_RE 在「果」停下 → snippet 以「果參照）。最高法院…」開頭
    assert not snip.startswith("果")
    assert "判決意旨參照）" in snip


# ─── Case 14：原告主張段落 citation 應被過濾為 FP ─────────────────────────────────

def test_party_section_citation_filtered():
    """
    當事人陳述段落（一、原告主張：…）中的 citation 應被過濾，
    不產生 decision citation。
    """
    text = (
        "一、原告主張：按法律關係不明確，原告有即受確認判決之法律上利益，"
        "最高法院42年台上字第1031號判例參照。\r\n"
        "二、本院之判斷：經查..."
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"原告主張段落不應產生 decision citation，但得到：{decision_results}"
    )


# ─── Case 15：本院判斷段落 citation 不應被過濾 ────────────────────────────────────

def test_court_section_citation_not_filtered():
    """
    法院論斷段落（二、本院之判斷：…）中的 citation 不應被過濾。
    """
    text = (
        "一、原告主張：略以...\r\n"
        "二、本院之判斷：按法律關係不明確，"
        "最高法院42年台上字第1031號判例參照。"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 1, (
        f"本院判斷段落應保留 1 筆 decision citation，但得到 {len(decision_results)} 筆"
    )
