"""
tests/test_snippet.py — extract_snippet() + extract_citations() 回歸測試

每個 case 來自真實 DB citation，說明當初的 bug 是什麼。
跑法：python -m pytest tests/test_snippet.py -v
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pytest
from etl.citation_parser_next import (
    ACCEPT_STRICT_RE as _CITE_INTENT_RE,
    _REASON_SECTION_RE,
    extract_citations_next,
    find_snippet_start,
    find_snippet_end,
)


def extract_citations(text, **kw):
    return [c.to_dict() for c in extract_citations_next(text, **kw)]


def extract_snippet(text, start, end, **kw):
    ss = find_snippet_start(text, start)
    se = find_snippet_end(text, start, end)
    return text[ss:se]


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
    # citation_parser_next 從 _SUB_CLAUSE_RE 找到的「按」開始，段落標題不含在內
    assert "按不法侵害他人" in snip
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
    # citation_parser_next 從 _SUB_CLAUSE_RE 找到的「又」開始
    assert "又不法侵害他人" in snip


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
    # citation_parser_next 在 \r\n 後截斷，snippet 從同行的引用括號開始
    # 重點：不應把 （最高法院 誤認為段落起頭而過短
    assert "最高法院56年度台抗字第714號" in snip
    assert "57年度台抗字第76號" in snip


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
    # citation_parser_next find_snippet_end 延伸到句號
    assert "參照）" in snip
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


# ─── Case 14：暫停 party guard 後，原告主張段 citation 先保留 ───────────────────────

def test_party_section_citation_temporarily_retained_without_section_filter():
    """
    在 section-aware filter 上線前，party section guard 已停用；
    原告主張段中的 citation 目前先保留，避免大規模誤殺合法引用。
    """
    text = (
        "一、原告主張：按法律關係不明確，原告有即受確認判決之法律上利益，"
        "最高法院42年台上字第1031號判例參照。\r\n"
        "二、本院之判斷：經查..."
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 1, (
        f"party guard 停用後，此類 decision citation 目前應保留，但得到：{decision_results}"
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


# ─── Case 15-1：暫停 party guard 後，原告主張段 authority 先保留 ─────────────────

def test_party_section_authority_citation_temporarily_retained_without_section_filter():
    """
    在 section-aware filter 上線前，party section guard 已停用；
    原告主張段中的 authority citation 目前先保留，避免誤殺釋字與決議引用。
    """
    text = (
        "三、原告主張：員警要求酒測已違反警察職權行使法第8條，司法院釋字第699號解釋參照。\r\n"
        "四、本院之判斷：經查..."
    )
    results = extract_citations(text)
    authority_results = [r for r in results if r.get("citation_type") == "authority"]
    assert len(authority_results) == 1, (
        f"party guard 停用後，此類 authority citation 目前應保留，但得到：{authority_results}"
    )


# ─── Case 15-1b：原裁定略以段 citation 目前暫時保留 ────────────────────────────

def test_prior_ruling_summary_citation_temporarily_retained_without_section_filter():
    """
    在 section-aware filter 上線前，位置型 party/summary 過濾已停用；
    「原裁定略以」中的 citation 目前先保留，避免再次大規模誤殺。
    """
    text = (
        "理由\r\n"
        "二、原裁定略以：㈠本件抗告人即受判決人陳某對於本院維持第二審法院所為之原確定判決提起再審，係主張："
        "憲法法庭113年憲判字第8號（下稱憲判8號）判決主文及形成主文之主要理由具體諭知宣告，"
        "原確定判決所適用之法律以死刑為其唯一之法定刑違憲，並賦予抗告人特別救濟請求權，"
        "故得以該憲法法庭判決作為再審事由，請求予以救濟。況該憲法法庭判決主文及形成主文之主要理由"
        "具體諭知之死刑案件罪刑相當、迴避死刑之量刑基準、加重量刑因子及其所連結之刑法第63條、"
        "公政公約第6條、第14條、第36號一般性意見書等，應相當於憲法法庭112年憲判字第2號"
        "（下稱憲判2號）判決所諭知之減輕或免除其刑之法律規定。\r\n"
        "三、抗告意旨略以：原裁定違法。"
    )
    results = extract_citations(text, court_root_norm="最高法院")
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    # citation_parser_next R010 要求 ACCEPT_RE signal；
    # 原裁定略以段落內的憲判字無明確 signal → 被 R010 過濾
    assert len(decision_results) == 0, (
        f"citation_parser_next R010 過濾此段落內無 ACCEPT_RE 的憲判字，但得到：{decision_results}"
    )


def test_prior_judgment_summary_authority_citation_filtered():
    """
    「原判決略以」屬轉述原判決理由，不是本院自行援引。
    該段落中的 citation 應被過濾。
    """
    text = (
        "理由\r\n"
        "二、原判決略以：原告主張依憲法法庭113年憲判字第8號判決意旨，"
        "且憲法法庭112年憲判字第2號判決亦足作為救濟依據。\r\n"
        "三、本院認為：上訴無理由。"
    )
    results = extract_citations(text, court_root_norm="最高法院")
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"原判決略以段 decision citation 不應保留，但得到：{decision_results}"
    )


def test_prior_disposition_summary_citation_temporarily_retained_without_section_filter():
    """
    在 section-aware filter 上線前，位置型 party/summary 過濾已停用；
    「原處分略以」中的 citation 目前先保留，避免再次大規模誤殺。
    """
    text = (
        "理由\r\n"
        "二、原處分略以：相對人援引憲法法庭113年憲判字第8號判決理由，"
        "並主張憲法法庭112年憲判字第2號判決可資參照。\r\n"
        "三、本院認為：聲請駁回。"
    )
    results = extract_citations(text, court_root_norm="最高法院")
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 2, (
        f"原處分略以段 citation 在暫停位置型過濾後目前應保留，但得到：{decision_results}"
    )


def test_prior_instance_summary_authority_citation_filtered():
    """
    「原審略以」屬轉述原審理由，不是本院自行援引。
    該段落中的 citation 應被過濾。
    """
    text = (
        "理由\r\n"
        "二、原審略以：聲請人主張憲法法庭113年憲判字第8號判決可作為依據，"
        "且憲法法庭112年憲判字第2號判決與本件情形相當。\r\n"
        "三、本院認為：抗告無理由。"
    )
    results = extract_citations(text, court_root_norm="最高法院")
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"原審略以段 decision citation 不應保留，但得到：{decision_results}"
    )


# ─── Case 15-2：本院判斷段的 authority citation 不應被過濾 ───────────────────────

def test_court_section_authority_citation_not_filtered():
    """
    法院論斷段落中的 authority citation（如釋字）應保留。
    """
    text = (
        "三、原告主張：略以...\r\n"
        "四、本院之判斷：員警得予以攔停，要求駕駛人接受酒測之檢定，"
        "司法院釋字第699號解釋參照。"
    )
    results = extract_citations(text)
    authority_results = [r for r in results if r.get("citation_type") == "authority"]
    assert len(authority_results) == 1, (
        f"本院判斷段 authority citation 應保留 1 筆，但得到 {len(authority_results)} 筆"
    )


# ─── Case 15-3：party guard 不應因 clean/processed offset 錯位誤濾法院判斷段 ─────

def test_court_section_after_party_section_not_misfiltered_by_offset():
    """
    前文雖有「原告主張」大節，但 citation 實際位於「四、本院之判斷」段時，
    不應因 clean_text / processed offset 錯位而被 party-section guard 誤濾。
    """
    text = (
        "二、原告主張及聲明：\r\n"
        "㈠主張要旨：\r\n"
        + ("⒈原告甲○○主張程序違法，應撤銷原處分。\r\n" * 40)
        + (
        "四、本院之判斷：\r\n"
        "⒈按參諸道交條例第35條第9項修法歷程可知，\r\n"
        "又綜觀道交條例對「汽車所有人」設有處罰規定之立法體例，均明確表示處罰對象為「汽車所有人」。"
        "因此，主管機關依道交條例第35條第9項前段規定而對汽機車所有人吊扣其車輛牌照時，"
        "自當以汽機車所有人與駕駛人為同一人之時，始應適用道交條例第35條第9項前段規定對其為吊扣該汽機車牌照之處罰，"
        "以符合處罰法定原則（最高行政法院113年度交上統字第2號判決意旨參照）。\r\n"
        "五、綜上所述，原告訴請撤銷原處分及原告乙○○請求被告返還牌照，為有理由。\r\n"
        )
    )
    results = extract_citations(text)
    decision_results = [
        r for r in results
        if r.get("citation_type") == "decision"
        and r.get("court") == "最高行政法院"
        and r.get("jyear") == 113
        and r.get("jcase_norm") == "交上統"
        and r.get("jno") == 2
    ]
    assert len(decision_results) == 1, (
        f"本院判斷段 citation 不應被 party guard 誤濾，但得到：{decision_results}"
    )
    assert "又綜觀道交條例對「汽車所有人」設有處罰規定之立法體例" in decision_results[0]["snippet"], (
        f"snippet 應包含法院論斷段文字，實際為：{decision_results[0]['snippet'][:60]!r}"
    )


# ─── Case 16：以上正本證明與原本無異之後的 citation 應被過濾 ──────────────────────

def test_zhengben_area_filtered():
    """
    「以上正本證明與原本無異」之後為書記欄/附表區，
    該區段出現的 citation 不應被計入。
    理由段的 citation 應正常保留。
    """
    text = (
        "理由\r\n"
        "一、按最高法院110年度台上字第1號判決意旨參照。\r\n"
        "以上正本證明與原本無異\r\n"
        "【附表】\r\n"
        "臺灣高等法院113年度聲字第1200號裁定應執行有期徒刑16年\r\n"
    )
    results = extract_citations(text, court_root_norm="臺灣高雄地方法院")
    raw_matches = [r["raw_match"] for r in results]
    # 理由段的引用應保留
    assert any("台上字第1號" in rm for rm in raw_matches), "理由段引用應保留"
    # 附表區的引用應被過濾
    assert not any("聲字第1200號" in rm for rm in raw_matches), "附表區引用不應出現"


# ─── Case 17：主文段 citation 應被過濾 ────────────────────────────────────────────

def test_zhuwen_section_filtered():
    """
    主文段（理由段之前）出現的 citation 應被過濾；
    理由段的 citation 應正常保留。
    刑事格式：「犯罪事實及理由」也應作為邊界。
    """
    text = (
        "主文\r\n"
        "原告之訴駁回（依臺灣高等法院112年度上字第99號判決）。\r\n"
        "犯罪事實及理由\r\n"
        "一、按最高法院110年度台上字第1號判決意旨參照。\r\n"
    )
    results = extract_citations(text)
    raw_matches = [r["raw_match"] for r in results]
    # 理由段的引用應保留
    assert any("台上字第1號" in rm for rm in raw_matches), "理由段引用應保留"
    # 主文段的引用應被過濾
    assert not any("上字第99號" in rm for rm in raw_matches), "主文段引用不應出現"


# ─── Case 18：agency_opinion 不應穿越「研討第N號」誤吃到研審小組 ───────────────────

def test_agency_opinion_not_cross_research_no():
    """
    「第N屆司法事務官消債問題研討第12號初步研討結論」後面才接
    「司法院民事廳...研審小組意見」，_AGENCY_OPINION_RE 不應從研討號開頭
    一路吃到研審小組意見；應只匹配司法院廳開頭那筆。
    原始 bug：#147393 raw_match 包含研討號 + 研審小組，兩者被合併為一筆。
    """
    text = (
        "（99年11月10日臺灣高等法院暨所屬法院99年法律座談會民事類提案第42號審查意見"
        "與研討結果表決之多數說、"
        "99年11月29日廳民二字第0990002160號第2屆司法事務官消債問題研討第12號初步研討結論、"
        "司法院民事廳消費者債務清理條例法律問題研審小組意見同此見解）"
    )
    results = extract_citations(text)
    agency_results = [r for r in results if r.get("auth_type") == "agency_opinion"]

    assert len(agency_results) == 1, (
        f"應只有 1 筆 agency_opinion，但得到 {len(agency_results)} 筆：{[r['raw_match'] for r in agency_results]}"
    )
    raw = agency_results[0]["raw_match"]
    assert "司法院民事廳" in raw, "agency_opinion 應從司法院民事廳開頭"
    assert "研討第12號" not in raw, "agency_opinion 不應包含研討號"


# ─── Case 19：引號閉合後接段落標記（」⑶）應作為 snippet 起點 ──────────────────────

def test_closing_quote_para_start():
    """
    「。」⑶依上規定...」格式：⑶ 緊接在 」 後，無 \r\n，
    _CLOSING_QUOTE_PARA_RE 應將 ⑶ 辨識為段落起點。
    原始 bug：#102122 snippet 從引號內的條文文字開頭，應從「⑶依上規定」開始。
    """
    text = (
        "前項規定：「申請書1份、設立（變更）登記表2份。」"
        "⑶依上規定，足知公司登記採準則主義，主管機關應於公司備齊相關文件後，"
        "倘申請書件形式均符合依公司法所定方式，即應准予登記"
        "（最高行政法院106年度判字第676號判決意旨參照）"
    )
    start, end = _pos(text, "最高行政法院106年度判字第676號")
    snip = extract_snippet(text, start, end)
    # citation_parser_next fallback 在 period+1 起始，可能帶入前置 」
    assert "⑶依上規定" in snip, (
        f"snippet 應包含「⑶依上規定」，實際開頭：{snip[:30]!r}"
    )


# ─── Case 20：在卷可參 應被 _EVIDENCE_CITE_RE 過濾（#143780 類型）─────────────────

def test_zaijuan_kechan_filtered():
    """
    「有該裁定在卷可參」緊接 citation 後方，應被 _EVIDENCE_CITE_RE 過濾。
    原始 bug：_EVIDENCE_CITE_RE 只列 在卷可稽/在卷可查，未明示 在卷可參。
    """
    text = (
        "理由\r\n"
        "一、查最高行政法院99年度裁字第3302號裁定，為再審原告另聲請之再審，"
        "並經該裁定駁回再審原告之聲請，有該裁定在卷可參，審之該裁定並未提及相關見解。\r\n"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"在卷可參後的 citation 不應保留，但得到：{[r['raw_match'] for r in decision_results]}"
    )


# ─── Case 21：確定在案 應被 _PRIOR_CASE_RE 過濾（#102312 類型）──────────────────────

def test_queding_zaian_filtered():
    """
    citation 後方緊接「確定在案）」，應被 _PRIOR_CASE_RE 過濾。
    原始 bug：「確定」單字過廣，「原確定裁定」的「確定」先命中後被 _CITE_CLOSING_RE 誤放行。
    「確定在案」需明確列入使 search 優先命中更長模式。
    """
    text = (
        "理由\r\n"
        "一、查最高行政法院111年度聲再字第616號裁定駁回再審原告就原確定裁定提起再審部分確定在案）。\r\n"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"確定在案後的 citation 不應保留，但得到：{[r['raw_match'] for r in decision_results]}"
    )


# ─── Case 22：如附表所示 應被 _PRIOR_CASE_RE 過濾（#134134 類型）──────────────────

def test_fubiao_suoshi_filtered():
    """
    citation 後方緊接「等裁定（如附表所示）」，應被 _PRIOR_CASE_RE 過濾。
    附表引用屬程序史，非法律見解引用。
    """
    text = (
        "理由\r\n"
        "一、聲請人就最高行政法院114年度聲字第649號等裁定（如附表所示），向本院聲請再審。\r\n"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"如附表所示後的 citation 不應保留，但得到：{[r['raw_match'] for r in decision_results]}"
    )


# ─── Case 23：審理中 應被 _PRIOR_CASE_RE 過濾（#144010 類型）───────────────────────

def test_shenlizh_filtered():
    """
    citation 後方緊接「事件審理中」，應被 _PRIOR_CASE_RE 過濾。
    審理中表示案件程序進行中，非法律見解引用。
    """
    text = (
        "理由\r\n"
        "一、查最高行政法院113年度上字第640號、第641號事件審理中。\r\n"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"審理中後的 citation 不應保留，但得到：{[r['raw_match'] for r in decision_results]}"
    )


# ─── Case 24：事件終結前 應被 _PRIOR_CASE_RE 過濾（#144011 類型）──────────────────

def test_shijian_jiesuqian_filtered():
    """
    citation 後方緊接「行政訴訟事件終結前，裁定停止本件訴訟程序」，
    應被 _PRIOR_CASE_RE 過濾（在卷可稽出現在 citation 前，forward check 看不到；
    after 中的「事件終結前」為實際觸發 pattern）。
    """
    text = (
        "理由\r\n"
        "一、查有各該裁判書及前案查詢表在卷可稽，並據本院調取相關卷宗查明屬實，"
        "故本院認有於最高行政法院113年度上字第640號、113年度上字第641號"
        "行政訴訟事件終結前，裁定停止本件訴訟程序之必要，爰依首揭條文，裁定如主文。\r\n"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 0, (
        f"事件終結前後的 citation 不應保留，但得到：{[r['raw_match'] for r in decision_results]}"
    )


# ─── Case 25：citation 後接「刑事庭」不應誤判 target_case_type='刑事' ──────────────────

def test_after_context_shiting_not_extracted():
    """
    citation 收尾後緊接「業經本院刑事庭以...刑事判決判處被告罪刑在案」，
    _extract_target_case_type 不應從前後文抓到「刑事」。
    原始 bug：context window 向後延伸 +30 字，下一句的「刑事庭」被誤判為 target_case_type='刑事'。
    修法：只查 raw_match 本身，完全不查前後文。
    """
    text = (
        "加害人於共同侵害權利之目的範圍內，各自分擔實行行為之一部，"
        "而互相利用他人之行為，以達其目的者，仍不失為共同侵權行為人，"
        "而應對於全部所發生之結果，連帶負損害賠償責任"
        "（最高法院78年度台上字第2479號判決意旨參照）。"
        "㈡原告主張之前揭事實，業經本院刑事庭以113年度金易字第109號刑事判決判處被告罪刑在案。"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 1, f"應有 1 筆 decision citation，得到 {len(decision_results)} 筆"
    ct = decision_results[0].get("target_case_type")
    assert ct is None, (
        f"citation 後接刑事庭不應誤判 target_case_type，實際值：{ct!r}"
    )


# ─── Case 26：標題 + 按 的法院論述，不應被 Guard 4 當成 party section ──────────────────

def test_authority_citation_after_heading_and_an_should_not_be_filtered():
    """
    民事判決常見格式：
    「一、被告...」「二、原告得請求損害賠償之金額：」後接「按...」，
    雖未出現「本院之判斷」等字樣，仍屬法院法律論述，不應因前段有「原告主張」而被 Guard 4 過濾。
    """
    text = (
        "理由要領\r\n"
        "一、被告應負侵權行為損害賠償責任：原告主張被告於民國112年8月25日23時51分許，"
        "駕駛車輛倒車時未注意其他車輛之過失，擦撞原告承保車輛，應負侵權行為損害賠償責任。\r\n"
        "二、原告得請求損害賠償之金額：\r\n"
        "　　按物被毀損時，被害人除得依民法第196條請求賠償外，並不排除民法第213條至第215條之適用。"
        "依民法第196條請求賠償物被毀損所減少之價額，得以修復費用為估定之標準，但以必要者為限，例如："
        "修理材料以新品換舊品，應予折舊(最高法院77年度第9次民事庭會議決議（一）參照)。\r\n"
    )
    results = extract_citations(text)
    authority_results = [r for r in results if r.get("citation_type") == "authority"]
    assert len(authority_results) == 1, (
        f"標題 + 按 的法院論述中之決議引用不應被過濾，但得到：{authority_results}"
    )
    assert authority_results[0]["auth_type"] == "resolution"


# ─── Case 27：理由要領 / 事實及理由要領 應被視為理由段起點 ──────────────────────

def test_reason_section_accepts_yaoling_variants():
    """
    _REASON_SECTION_RE 應接受「理由要領」「事實及理由要領」，
    避免 reason_pos 抓不到，造成後續位置型 guard 不穩。
    """
    text = (
        "主文\r\n"
        "被告應給付原告新臺幣1萬元。\r\n"
        "事實及理由要領\r\n"
        "一、按侵權行為損害賠償，民法第184條定有明文。"
        "最高法院113年度台上字第999號判決意旨參照）。\r\n"
    )
    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]
    assert len(decision_results) == 1, (
        f"事實及理由要領下的 citation 應保留，但得到：{decision_results}"
    )


def test_reason_section_regex_matches_yaoling_variants():
    text1 = "主文\r\n被告應給付。\r\n理由要領\r\n一、按民法第184條。"
    text2 = "主文\r\n被告應給付。\r\n事實及理由要領\r\n一、按民法第184條。"

    m1 = _REASON_SECTION_RE.search(text1)
    m2 = _REASON_SECTION_RE.search(text2)

    assert m1 is not None, "理由要領 應被 _REASON_SECTION_RE 命中"
    assert m2 is not None, "事實及理由要領 應被 _REASON_SECTION_RE 命中"


def test_resolution_citation_should_survive_party_section_guard():
    text = (
        "事實及理由\r\n"
        "二、原告主張：被告駕駛車輛碰撞原告車輛，應負侵權行為損害賠償責任。\r\n"
        "四、本院之判斷：\r\n"
        "㈢茲就原告請求之損害賠償內容及金額部分，審酌如下：\r\n"
        "3.B車維修費用：\r\n"
        "原告主張其支出B車維修費用56,250元（均為零件費用）等語，並提出昶盈車業估價單1份為證，"
        "應堪認屬實。惟查，修復費用之賠償以必要者為限，則修理材料以新品換舊品，自應予以折舊"
        "（最高法院77年度第9次民事庭會議決議㈠意旨參照）。"
    )

    results = extract_citations(text)
    authority_results = [r for r in results if r.get("citation_type") == "authority"]

    assert any(r.get("auth_key") == "民事庭|77|9" for r in authority_results), (
        f"民事庭第77年度第9次決議不應被 party section guard 過濾，但得到：{authority_results}"
    )


def test_grand_interp_citation_should_survive_party_section_guard():
    text = (
        "犯罪事實及理由\r\n"
        "二、論罪科刑(二)被告前因違反毒品危害防制條例案件，經法院判處有期徒刑確定，"
        "於執行完畢後，5年以內故意再犯本件有期徒刑以上之罪，為刑法第47條第1項所定之累犯。"
        "茲依司法院釋字第775號解釋意旨，審酌被告因上述前案執行完畢後，仍未能謹慎守法，"
        "於5年內再犯本案，顯見其刑罰反應力薄弱，自我控制力及守法意識不佳，依其本案犯罪情節，"
        "亦無處以法定最輕本刑仍顯過苛之情形，爰依刑法第47條第1項規定加重其刑。"
    )

    results = extract_citations(text)
    authority_results = [r for r in results if r.get("citation_type") == "authority"]

    assert any(r.get("auth_type") == "grand_interp" and r.get("auth_key") == "釋字|775" for r in authority_results), (
        f"釋字第775號不應被 party section guard 過濾，但得到：{authority_results}"
    )


def test_constitutional_decision_citation_should_survive_party_section_guard():
    text = (
        "理　　由\r\n"
        "壹、證據能力之認定：本判決所引用之供述及非供述證據，均有證據能力。\r\n"
        "貳、有罪部分：\r\n"
        "一、認定犯罪事實所憑之證據及理由：上揭犯罪事實，業據被告於本院審理中坦承不諱。\r\n"
        "二、所犯法條及刑之酌科：\r\n"
        "㈣國家本即擁有不同方式及強度之公權力手段以達成公務目的，"
        "於人民當場辱罵公務員之情形，代表國家執行公務之公務員原即得透過其他之合法手段，"
        "以即時排除、制止此等言論對公務執行之干擾。例如執行職務之公務員本人或其在場之主管、同僚等，"
        "均得先警告或制止表意人，要求表意人停止其辱罵行為。"
        "人民以具有表意成分之肢體動作對公務員予以侮辱，不論是否觸及公務員身體，"
        "就其是否構成侮辱公務員罪，仍應由法院依本判決意旨於個案認定之"
        "（憲法法庭113年度憲判字第5號判決意旨參照）。"
    )

    results = extract_citations(text)
    decision_results = [r for r in results if r.get("citation_type") == "decision"]

    assert any(
        r.get("court") == "憲法法庭"
        and r.get("jyear") == 113
        and r.get("jcase_norm") == "憲判"
        and r.get("jno") == 5
        for r in decision_results
    ), f"憲法法庭113年度憲判字第5號判決不應被 party section guard 過濾，但得到：{decision_results}"


def test_cite_intent_regex_matches_tongzhi_variant():
    """「同旨」應被視為引用意圖，避免本院裁定同旨被漏掉。"""
    assert _CITE_INTENT_RE.search("同旨），聲請意旨")
    assert _CITE_INTENT_RE.search("裁定同旨")
