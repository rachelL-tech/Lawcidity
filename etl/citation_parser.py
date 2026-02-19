"""
從判決全文抽取引用判決的 citation（狀態機版本）

Week 1：TARGET_COURTS = {'最高法院'}，只 insert 此範圍的引用
Week 2+：擴充 TARGET_COURTS 即可，不需改演算法
"""
import re
from typing import List, Dict, Set, Optional

# =========================
# Regex patterns
# =========================

# 任意法院具名 citation
# group(1) = 法院名（含分院），group(2) = 年，group(3) = 字別，group(4) = 案號
#
# 法院名擴充（在原有基礎上）：
#   最高(?:行政)?法院 → 最高法院 / 最高行政法院
#   憲法法庭          → 以「法庭」結尾，不在一般「法院」範圍內，需明確列舉
#   (?:[\u4e00-\u9fff]+分院)? → 選擇性分院後綴（臺中分院、金門分院 等）
# 範例：最高法院 / 最高行政法院 / 憲法法庭 / 臺灣高等法院 / 臺灣高等法院臺中分院
#
# 大法庭修飾語（選擇性）：(?:(?:刑事|民事|行政)?大法庭)?
#   位置：法院名之後、年份之前（如「最高法院刑事大法庭110年度...」）
#   若修飾語在案號之後（如「第897號民事大法庭裁定」），屬 trailing text，regex 自然忽略
#
# 字別前綴擴充：[台臺大憲]
#   大 → 最高行政法院大法庭（「大字第1號」）
#   憲 → 憲法法庭（「憲判字第2號」）
#   {0,20}?（原 {1,20}?）：允許 0 字，支援「大字」直接接字的情況
# 字 可省略（字?）：支援「台上大第5660號」省略字的格式
ANY_COURT_CITATION = re.compile(
    r'((?:最高(?:行政)?法院|憲法法庭|(?:臺灣|台灣|福建)[\u4e00-\u9fff]*?法院)(?:[\u4e00-\u9fff]+分院)?)'
    r'(?:(?:刑事|民事|行政)?大法庭)?'
    r'\s*(\d{2,3})\s*年\s*度?\s*'
    r'([台臺大憲][^字]{0,20}?)\s*字?\s*第\s*(\d+)\s*號'
)

# 省略法院名的引用（承接前一個 citation 的 current_court）
# 開頭是分隔符號（、，等），後面直接接年份數字
# 當鏈中出現具名法院（漢字開頭）時，ABBR.match() 自然失敗，鏈中斷
# group(1) = 年，group(2) = 字別，group(3) = 案號
ABBR_CITATION = re.compile(
    r'[、，及與暨或,]\s*(\d{2,3})\s*年\s*度?\s*([台臺大憲][^字]{0,20}?)\s*字?\s*第\s*(\d+)\s*號'
)

# Week 1 抓最高法院、最高行政法院、憲法法庭；Week 2+ 在這裡擴充
TARGET_COURTS: Set[str] = {'最高法院', '最高行政法院', '憲法法庭'}

# 最高法院會議決議：「最高法院77年度第9次民事庭會議決議」
# group(1)=年, group(2)=次序, group(3)=庭別
RESOLUTION_RE = re.compile(
    r'最高法院'
    r'(\d{2,3})年度?'
    r'第(\d+)次'
    r'(民事庭|刑事庭|民刑事庭|民刑事庭總會|刑事庭總會|民事庭總會)'
    r'(?:會議)?決議'
)

# 司法院大法官釋字
# group(1) = 釋字號碼
# 支援：司法院大法官釋字 / 司法院釋字 / 大法官釋字 / 大法官會議釋字
GRAND_INTERP_RE = re.compile(
    r'(?:司法院(?:大法官(?:會議)?)?|大法官(?:會議)?)釋字第\s*(\d+)\s*號'
)

# 法律座談會
# group(1) = 年份，group(2) = 提案號（Arabic 或中文數字；無提案號時為 None）
#
# 機構（選擇性，在年份之前；若年份在前機構在後，靠 filler 吸收）：
#   高等行政法院（含「及地方法院行政訴訟庭」後綴）
#   司法院（後接 0–15 字主題名）
#   高等法院暨所屬法院 / 本院暨所屬法院
#
# filler（年份→法律座談會）：≤30 字漢字/數字，吸收「度高等行政法院」等
# filler（法律座談會→提案號）：≤40 字漢字/空白，吸收「民事類」「相關議題」等
#
# 提案號需有「提案」或「第」前置，避免誤抓年份數字（如「民國106年3月22日提案及...」
# 中的「及」讓 regex 無法匹配後方數字，不會產生誤報）
# 支援格式：提案第N號 / 提案第N / 提案N / 第N號（N 為 Arabic 或中文數字）
CONFERENCE_RE = re.compile(
    r'(?:'
    r'(?:(?:臺灣)?高等行政法院(?:及[\u4e00-\u9fff]{2,25}庭)?)'
    r'|(?:司法院[\u4e00-\u9fff]{0,15})'
    r'|(?:(?:臺灣)?高等法院(?:暨所屬法院)?)'
    r'|(?:本院(?:暨所屬法院)?)'
    r')?'
    r'(?:民國)?(\d{2,3})年'
    r'[\u4e00-\u9fff\d年月日]{0,30}?'
    r'法律座談會'
    r'(?:'
    r'[\u4e00-\u9fff\s]{0,40}?'
    r'(?:提案第?\s*|第\s*)'
    r'([一二三四五六七八九十百\d]+)'
    r'\s*號?'
    r')?'
)


# =========================
# 預處理
# =========================
def preprocess_text(text: str) -> str:
    """移除換行與多餘空白，讓 regex 更穩定。
    注意：只用來做 regex 抽取，snippet 要從原始 full_text 取。
    """
    text = text.replace('\r\n', '').replace('\n', '').replace('\r', '')
    text = re.sub(r'\s+', ' ', text)
    return text


# =========================
# 內部工具
# =========================
def _normalize_court(court: str) -> str:
    """正規化法院名（臺→台，去除多餘空白）"""
    return court.replace('臺', '台').strip()


_CN_DIGITS = {'一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
              '六': 6, '七': 7, '八': 8, '九': 9}


def cn_to_int(s: str) -> int:
    """將中文數字字串或純 Arabic 字串轉為整數。支援到 199。
    例：'十二' → 12，'三' → 3，'12' → 12
    """
    if s.isdigit():
        return int(s)
    total, cur = 0, 0
    for c in s:
        if c == '十':
            total += (cur or 1) * 10
            cur = 0
        elif c == '百':
            total += (cur or 1) * 100
            cur = 0
        elif c in _CN_DIGITS:
            cur = _CN_DIGITS[c]
    return total + cur


def _infer_organizer(raw_match: str) -> str:
    """從 raw_match 推斷法律座談會主辦機構類別（用於 auth_key）。
    當機構名在年份之後（regex 未能捕捉），靠字串搜尋補救。
    """
    if '行政法院' in raw_match:
        return '高等行政法院'
    if '司法院' in raw_match:
        return '司法院'
    return '高等法院'


def _make_result(
    court: str,
    raw_match: str,
    jyear_str: str,
    jcase_raw: str,
    jno_str: str,
    clean_text: str,
    processed: str,
    fallback_start: int,
    fallback_end: int,
    authority_mode: bool = False,
) -> Dict:
    """
    建構 citation result dict。
    1. 先在 clean_text 直接搜尋 raw_match（完整字串）
    2. 找不到（PDF 折行造成 \\r\\n 中斷）→ 改用 flexible pattern（允許任意空白）重新定位
    3. 兩者都失敗 → fallback 到 processed 偏移（snippet 品質最差）

    authority_mode=True：使用釋字/憲法法庭專用的向後邊界（參照）→ 句號）
    """
    jcase_norm = jcase_raw.replace(' ', '').replace('臺', '台')

    orig = re.search(re.escape(raw_match), clean_text)
    if orig:
        match_start = orig.start()
        match_end   = orig.end()
        snippet = extract_snippet(clean_text, match_start, match_end, authority_mode=authority_mode)
    else:
        # PDF 折行：citation 中間有 \r\n，逐字允許 \s* 重新定位
        flexible = r'[\s\r\n]*'.join(re.escape(c) for c in raw_match)
        flex = re.search(flexible, clean_text)
        if flex:
            match_start = flex.start()
            match_end   = flex.end()
            snippet = extract_snippet(clean_text, match_start, match_end, authority_mode=authority_mode)
        else:
            match_start = None
            match_end   = None
            snippet = extract_snippet(processed, fallback_start, fallback_end, authority_mode=authority_mode)

    return {
        "citation_type": "decision",
        "court": court,
        "raw_match": raw_match,
        "match_start": match_start,
        "match_end": match_end,
        "snippet": snippet,
        "jyear": int(jyear_str),
        "jcase_norm": jcase_norm,
        "jno": int(jno_str),
    }


# =========================
# 抽取 citations（狀態機）
# =========================
def extract_citations(
    clean_text: str,
    target_courts: Set[str] = TARGET_COURTS,
) -> List[Dict]:
    """
    從全文抽取引用判決的 citation（狀態機版）

    演算法：
    1. 在 preprocess_text(clean_text) 上線性掃描（移除換行以跨行 match citation）
    2. ① 嘗試 ABBR.match(pos)（只在 current_court 存在時）
          → 成功：繼承 current_court；若在 target_courts 內則 append；pos 前進
          → 失敗：進 ②
       ② ANY_COURT_CITATION.search(processed, pos)
          → 更新 current_court；若在 target_courts 內則 append；pos 跳到 match.end()
          → 找不到 → break
    3. 省略引用鏈遇到具名法院（漢字開頭）時，ABBR 自然失敗，current_court 由 ② 更新
    4. 偏移量修正至 clean_text；snippet 從 clean_text 取（保留換行、段落結構）

    Args:
        clean_text: clean_judgment_text() 處理後的全文
        target_courts: 要抓的被引用法院（預設 {'最高法院'}）

    Returns:
        List of dict，有兩種格式：

        decision citation:
        {
            "citation_type": "decision",
            "court": str,        # 被引用法院（正規化）
            "raw_match": str,    # 原始命中字串
            "match_start": int,  # 在 clean_text 的起點（PDF 折行無法定位時為 None）
            "match_end": int,    # 在 clean_text 的終點
            "snippet": str,
            "jyear": int,
            "jcase_norm": str,
            "jno": int,
        }

        authority citation（會議決議、釋字、法律座談會等）:
        {
            "citation_type": "authority",
            "auth_type": str,    # 'resolution' | 'grand_interp' | 'conference' | ...
            "auth_key": str,     # 自然鍵，如 '民事庭|77|9'、'釋字|144'、'高等法院|111|21'
            "display": str,      # 顯示用完整名稱
            "raw_match": str,
            "match_start": int or None,
            "match_end": int or None,
            "snippet": str,
        }
    """
    processed = preprocess_text(clean_text)
    results = []
    current_court: Optional[str] = None
    pos = 0

    while pos < len(processed):
        # ① 省略引用：只在 chain 進行中（current_court 存在）才嘗試
        if current_court is not None:
            abbr = ABBR_CITATION.match(processed, pos)
            if abbr:
                if current_court in target_courts:
                    if not _is_false_positive_citation(processed, abbr.end()):
                        # group(0) = 「、114年度台抗字第310號」，strip 掉開頭分隔符號
                        a_raw = abbr.group(0)[1:].lstrip()
                        results.append(_make_result(
                            court=current_court,
                            raw_match=a_raw,
                            jyear_str=abbr.group(1),
                            jcase_raw=abbr.group(2),
                            jno_str=abbr.group(3),
                            clean_text=clean_text,
                            processed=processed,
                            fallback_start=abbr.start(1),  # 年份起點，跳過分隔符
                            fallback_end=abbr.end(),
                            authority_mode=(current_court == '憲法法庭'),
                        ))
                pos = abbr.end()
                continue

        # ② 具名 citation：搜尋下一個（任意法院）
        full = ANY_COURT_CITATION.search(processed, pos)
        if full is None:
            break

        current_court = _normalize_court(full.group(1))
        if current_court in target_courts:
            # 憲法法庭：guard（match 前後 50 字內需有「參」或「見」）+ authority_mode snippet
            is_const_court = (current_court == '憲法法庭')
            if is_const_court:
                ctx_before = processed[max(0, full.start() - 10): full.start()]
                ctx_after  = processed[full.end(): full.end() + 50]
                if '參' not in (ctx_before + ctx_after) and '見' not in (ctx_before + ctx_after):
                    pos = full.end()
                    continue
            # False positive 過濾：前案程序史 / 卷證附件引用
            if not is_const_court and _is_false_positive_citation(processed, full.end()):
                pos = full.end()
                continue
            results.append(_make_result(
                court=current_court,
                raw_match=full.group(0),
                jyear_str=full.group(2),
                jcase_raw=full.group(3),
                jno_str=full.group(4),
                clean_text=clean_text,
                processed=processed,
                fallback_start=full.start(),
                fallback_end=full.end(),
                authority_mode=is_const_court,
            ))
        pos = full.end()

    # 掃描最高法院會議決議 → authority (resolution)（獨立掃描，不影響上方狀態機）
    for m in RESOLUTION_RE.finditer(processed):
        raw_match = m.group(0)
        jyear = int(m.group(1))
        seq_no = int(m.group(2))
        court_type = m.group(3)

        # 在 clean_text 定位（同 _make_result 邏輯）
        orig = re.search(re.escape(raw_match), clean_text)
        if orig:
            match_start = orig.start()
            match_end = orig.end()
            snippet = extract_snippet(clean_text, match_start, match_end)
        else:
            flexible = r'[\s\r\n]*'.join(re.escape(c) for c in raw_match)
            flex = re.search(flexible, clean_text)
            if flex:
                match_start = flex.start()
                match_end = flex.end()
                snippet = extract_snippet(clean_text, match_start, match_end)
            else:
                match_start = None
                match_end = None
                snippet = extract_snippet(processed, m.start(), m.end())

        results.append({
            "citation_type": "authority",
            "auth_type": "resolution",
            "auth_key": f"{court_type}|{jyear}|{seq_no}",
            "display": f"最高法院{jyear}年度第{seq_no}次{court_type}會議決議",
            "raw_match": raw_match,
            "match_start": match_start,
            "match_end": match_end,
            "snippet": snippet,
        })

    # 掃描司法院大法官釋字 → authority (grand_interp)（獨立掃描）
    for m in GRAND_INTERP_RE.finditer(processed):
        raw_match = m.group(0)
        no = m.group(1)

        # Guard：match 前 10 字 + 後 50 字內需有「參」或「見」，過濾純提及（非引用）
        # 前 10 字：捕捉「參照釋字第X號」前置模式，但短到跨不過一個句號
        # 後 50 字：捕捉「釋字第X號...參照」後置模式（最常見）
        ctx_before = processed[max(0, m.start() - 10): m.start()]
        ctx_after  = processed[m.end(): m.end() + 50]
        if '參' not in (ctx_before + ctx_after) and '見' not in (ctx_before + ctx_after):
            continue

        orig = re.search(re.escape(raw_match), clean_text)
        if orig:
            match_start = orig.start()
            match_end = orig.end()
            snippet = extract_snippet(clean_text, match_start, match_end, authority_mode=True)
        else:
            flexible = r'[\s\r\n]*'.join(re.escape(c) for c in raw_match)
            flex = re.search(flexible, clean_text)
            if flex:
                match_start = flex.start()
                match_end = flex.end()
                snippet = extract_snippet(clean_text, match_start, match_end, authority_mode=True)
            else:
                match_start = None
                match_end = None
                snippet = extract_snippet(processed, m.start(), m.end(), authority_mode=True)

        results.append({
            "citation_type": "authority",
            "auth_type": "grand_interp",
            "auth_key": f"釋字|{no}",
            "display": f"司法院大法官釋字第{no}號",
            "raw_match": raw_match,
            "match_start": match_start,
            "match_end": match_end,
            "snippet": snippet,
        })

    # 掃描法律座談會 → authority (conference)（獨立掃描）
    for m in CONFERENCE_RE.finditer(processed):
        raw_match = m.group(0)
        year = int(m.group(1))
        no_raw = m.group(2)   # 可能為 None
        org = _infer_organizer(raw_match)
        no_int = cn_to_int(no_raw) if no_raw else None
        auth_key = f"{org}|{year}|{no_int}" if no_int is not None else f"{org}|{year}"
        display = f"{org}{year}年法律座談會" + (f"提案第{no_int}號" if no_int is not None else "")

        orig = re.search(re.escape(raw_match), clean_text)
        if orig:
            match_start = orig.start()
            match_end = orig.end()
            snippet = extract_snippet(clean_text, match_start, match_end)
        else:
            flexible = r'[\s\r\n]*'.join(re.escape(c) for c in raw_match)
            flex = re.search(flexible, clean_text)
            if flex:
                match_start = flex.start()
                match_end = flex.end()
                snippet = extract_snippet(clean_text, match_start, match_end)
            else:
                match_start = None
                match_end = None
                snippet = extract_snippet(processed, m.start(), m.end())

        results.append({
            "citation_type": "authority",
            "auth_type": "conference",
            "auth_key": auth_key,
            "display": display,
            "raw_match": raw_match,
            "match_start": match_start,
            "match_end": match_end,
            "snippet": snippet,
        })

    return results


# =========================
# Snippet 擷取（混合策略）
# =========================
# 有編號的段落起點（一、二、壹、貳、㈠㈡、①②、⑴⑵、⒈⒉、(一) 等）
# 這些才是「真正段落起點」；非縮排的 PDF 折行 \r\n 後面不會接這些字元
# 加入半形 ( ：支援 (一)(二) 格式（舊式段落編號）
_PARA_START_RE = re.compile(
    r'\r\n[ \t　]{0,4}'      # 允許最多 4 個前導空白（修：\r\n  ㈢ 格式）
    r'(?='
    r'[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁戊己庚辛壬癸'  # 加：甲乙丙丁... 大綱字
    r'㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩'
    r'①②③④⑤⑥⑦⑧⑨⑩'
    r'⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽'   # 括號數字 U+2474–U+247D
    r'⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑'   # 數字句號 U+2488–U+2491
    r']'                     # 關閉字元集（（( 移至下方受限格式）
    r'|[（(][一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁戊己庚辛壬癸]'  # （一）(一) 但不配（最高法院...
    r'|[1-9][0-9]*[.、 　]'  # 阿拉伯數字條號（1. 2. 3、8 精神慰撫金 等）
    r')'                     # 關閉 lookahead
)

# 子條款起點：「再按」「復按」「又按」「次按」「末按」「且按」「惟按」「惟查」「惟依」
#             「再者」「所謂」「另」「按」（行首）
# 邊界允許 。 或 \r\n 作前導；前導後可有少量標點/PUA 字元（如 \uf6aa、㈠、⑴ 等）
# \u3200-\u32ff：Enclosed CJK（㈠㈡…㊿），出現於 \r\n 與關鍵字之間
# \u2460-\u24ff：Enclosed Alphanumerics（①②…、⑴⑵…），處理附表內「。 ⑴按」格式
# group(1) = 關鍵字起始位置（用於 actual_start）
_SUB_CLAUSE_RE = re.compile(
    r'(?:(?:。|\r\n|[：:])[\uf000-\uffff\u3000-\u303f\u3200-\u32ff\u2460-\u24ff\t 　]{0,6})'  # 加：[：:] 邊界（修：：　　按 格式）
    r'((?:再|復|又|次|末|且)按|惟(?:按|查|依)|又(?!按)|再者|所謂|另(?![行有外附])|按(?!照))'  # 加：惟查、惟依
    r'[：:，,「]?'
)

# =========================
# False positive 過濾（程序史引用、證據附件引用）
# =========================

# 前案程序史：「駁回確定」「駁回上訴確定」「判決上訴駁回」「裁判確定處N年」等
# 這些引用是描述被告/原告前案結果，不作為法律見解引用
_PRIOR_CASE_RE = re.compile(
    r'駁回(?:上訴|抗告)?確定'           # 駁回確定 / 駁回上訴確定 / 駁回抗告確定
    r'|上訴(?:駁回|不受理)'             # 上訴駁回（後通常接確定）
    r'|判決上訴駁回'                    # 判決上訴駁回（確定）
    r'|裁判確定(?:處\d|，|。)'          # 裁判確定處N年 / 裁判確定，
    r'|(?:判決|裁定)所載'               # 引用判決/裁定的記載內容（非見解）
)

# 卷證附件引用：「（見本院卷」「 本院卷一第」等——此時是把判決當作卷證提出，非引用法律見解
_EVIDENCE_CITE_RE = re.compile(
    r'[（( ](?:見)?(?:本院|偵查|原審|審理|上訴|抗告).{0,5}卷'
)

# 引用收尾標記：任何這些出現在 FP pattern 之前，代表引用已合法結束，不過濾
_CITE_CLOSING_RE = re.compile(r'[）)。]|意旨|參照|見解|裁定意旨|判決意旨')


def _is_false_positive_citation(processed: str, match_end: int) -> bool:
    """
    判斷此 citation 是否為 false positive（前案程序史 或 卷證附件引用）。

    統一套「收尾標記前才算」邏輯：
    若 FP 模式在 match_end 後 50 字內命中，且命中點之前沒有引用收尾標記 → False positive。
    例：
      「號刑事判決（見本院卷第357頁）」 → 無收尾 → 過濾 ✓
      「號判決意旨參照）。查...（本院卷第71頁）」→ ） 先出現 → 不過濾 ✓
      「號裁定參照）。本件...裁定所載」 → ） 先出現 → 不過濾 ✓
    """
    after = processed[match_end: match_end + 50]

    for pattern in (_PRIOR_CASE_RE, _EVIDENCE_CITE_RE):
        m = pattern.search(after)
        if m:
            before_fp = after[: m.start()]
            if not _CITE_CLOSING_RE.search(before_fp):
                return True

    return False


# authority citation 後允許接的 trailing text（Pass 2 look_back 推進用）
# 允許以決議/研討結果/解釋結尾，不強制參照/）
# 字元集而非詞組，避免漏網任意排列組合
_AUTH_TRAIL_RE = re.compile(r'[意旨參照解釋研討結決議裁判、，。 \t　）)]{0,30}')

# 引用收尾的「參照」短語（向後 boundary 用，decision 模式）
# 優先於裸 ）搜尋，解決 resolution 後 （一）/（1）誤截和截太遠兩個問題
# trailing char 必填（避免匹配「參照民法XX條」這種用法）
_CITE_REF_CLOSE = re.compile(r'(?:可資參照|足資參照|意旨參照|決議參照|要旨參照|可參|參照)[。，、,）)\]】\s]')

# authority_mode 向後邊界：「參照）」「判決）」「裁定）」「解釋）」「意旨）」等
# 僅用於釋字（grand_interp）與憲法法庭（憲法法庭 decision）；決議/座談會不用
_AUTH_CLOSE_RE = re.compile(r'(?:參照|判決|解釋|意旨|號)[）)]')


def extract_snippet(
    text: str,
    match_start: int,
    match_end: int,
    max_back: int = 3000,
    max_forward_paren: int = 150,
    authority_mode: bool = False,
) -> str:
    """
    以 citation match 為中心切出 snippet：

    向前：在 para_cap 窗口內，同時找子條款（再按/復按 等）和編號段落起點（一、二、㈠ 等），
          取兩者中最靠近 match_start 者（max 位置）。
          窗口內都找不到 → 退回更遠的段落起點（超過 para_cap 則硬切）→ 任意換行 fallback。

    向後（authority_mode=False，預設）：
          找 match_end 之後最近的 ）（citation 收尾括號），在那裡截止；
          fallback：找 。 或 \\r\\n；都沒有則取到 max_forward_paren。

    向後（authority_mode=True，釋字/憲法法庭專用）：
          ① 找「參照）」「判決）」「號」等收尾標記 → ② 最近句號 → ③ \\r\\n fallback
    """
    para_cap: int = 600
    look_back_start = max(0, match_start - max_back)
    look_back = text[look_back_start: match_start]

    # 在 para_cap 窗口內，同時找子條款和編號段落起點，取最靠近 match_start 者
    sub_window_pos = max(0, len(look_back) - para_cap)

    last_sub = None
    for m in _SUB_CLAUSE_RE.finditer(look_back, sub_window_pos):
        last_sub = m

    last_para_near = None
    for m in _PARA_START_RE.finditer(look_back, sub_window_pos):
        last_para_near = m

    sub_pos = look_back_start + last_sub.start(1) if last_sub is not None else None
    para_pos = look_back_start + last_para_near.start() + 2 if last_para_near is not None else None

    if sub_pos is not None and para_pos is not None:
        if 0 <= sub_pos - para_pos <= 20:
            # 同一段落單元（如 ㈠按、⑴按）：sub 緊接在 para 之後
            # 用 para_pos（包含段落標記，如 ㈠、⑴）
            actual_start = para_pos
        else:
            # 不同位置：取最靠近 match_start 者（max）
            actual_start = max(sub_pos, para_pos)
    elif sub_pos is not None:
        actual_start = sub_pos
    elif para_pos is not None:
        actual_start = para_pos
    else:
        # 完整 look_back 找最後一個編號段落起點（距離可能 > para_cap）
        last_para = None
        for m in _PARA_START_RE.finditer(look_back):
            last_para = m

        if last_para is not None:
            candidate = look_back_start + last_para.start() + 2  # skip \r\n
            if match_start - candidate <= para_cap:
                actual_start = candidate
            else:
                # 超過 para_cap：從硬切點往前最多 150 字找最近的句號
                hard_cut = match_start - para_cap
                search_start = max(look_back_start, hard_cut - 150)
                before_cut = text[search_start: hard_cut]
                period_pos = before_cut.rfind('。')
                if period_pos != -1:
                    actual_start = search_start + period_pos + 1
                else:
                    actual_start = hard_cut
        else:
            # 先找最後一個句號（比 \r\n 可靠，避免截在 PDF 折行的句子中間）
            last_period = look_back.rfind('。')
            if last_period != -1 and (len(look_back) - last_period - 1) <= para_cap:
                actual_start = look_back_start + last_period + 1
            else:
                # 任意換行 fallback
                any_newline = look_back.rfind('\r\n')
                actual_start = look_back_start + any_newline + 2 if any_newline != -1 else look_back_start

    # ★ 裁定書尾空行跳過：若 actual_start ~ match_start 之間有 3 個以上連續空行（法院頁尾），
    #   推進到最後一個空行塊之後（通常就是附表/正文起點）
    blank_cluster = re.compile(r'(?:\r\n){3,}')
    last_blank_m = None
    for m in blank_cluster.finditer(text, actual_start, match_start):
        last_blank_m = m
    if last_blank_m is not None:
        candidate = last_blank_m.end()
        while candidate < match_start and text[candidate] in '\r\n \t　':
            candidate += 1
        if candidate < match_start:
            actual_start = candidate

    # ★ 引用邊界後處理：推進 actual_start 到各引用收尾之後（跳過空白/換行）
    # Pass 1: ANY_COURT_CITATION（同時檢查全形 ）和半形 )，取較早出現者）
    in_lb_start = actual_start - look_back_start
    for m in ANY_COURT_CITATION.finditer(look_back, in_lb_start):
        after_cite = look_back_start + m.end()
        # ★ 同一引用鏈：after_cite 到 match_start 之間無句號/換行 → 不推進
        between = text[after_cite: match_start]
        if '。' not in between and '\r\n' not in between:
            continue
        window = text[after_cite: after_cite + 80]
        paren_fw = window.find('）')
        paren_hw = window.find(')')
        candidates_p = [p for p in [paren_fw, paren_hw] if p != -1]
        paren_pos = min(candidates_p) if candidates_p else -1
        end_pos = after_cite + paren_pos + 1 if paren_pos != -1 else after_cite
        while end_pos < match_start and text[end_pos] in '。\r\n \t　':
            end_pos += 1
        if end_pos < match_start:
            actual_start = end_pos

    # Pass 2: authority citations in look_back（決議、釋字、座談會，不被 ANY_COURT_CITATION 匹配）
    # 允許 match 後接決議/研討結果/解釋等 trailing text，不強制參照/）結尾
    in_lb_start2 = actual_start - look_back_start
    for auth_re in (RESOLUTION_RE, GRAND_INTERP_RE, CONFERENCE_RE):
        for m in auth_re.finditer(look_back, in_lb_start2):
            end_pos = look_back_start + m.end()
            # 吸收 match 後的 trailing text（意旨、參照、解釋、研討結果 等，最多 30 字）
            tail = _AUTH_TRAIL_RE.match(text, end_pos)
            if tail and tail.end() > end_pos:
                end_pos = tail.end()
            while end_pos < match_start and text[end_pos] in '。\r\n \t　':
                end_pos += 1
            if end_pos < match_start:
                actual_start = end_pos

    look_forward = text[match_end: match_end + max_forward_paren]

    if authority_mode:
        # 釋字/憲法法庭 專用：① 參照）/ 判決）/ 裁定）等 → ② 最近句號 → ③ \r\n fallback
        auth_close = _AUTH_CLOSE_RE.search(look_forward)
        if auth_close:
            actual_end = match_end + auth_close.end()
        else:
            period_pos = look_forward.find('。')
            if period_pos != -1:
                actual_end = match_end + period_pos + 1
            else:
                nl_pos = look_forward.find('\r\n')
                actual_end = match_end + (nl_pos + 2 if nl_pos != -1 else len(look_forward))
    else:
        # decision 模式（既有邏輯）
        # ① 先找「參照」類結尾（解決 resolution 後 （一）誤截 / 截太遠問題）
        ref_close = _CITE_REF_CLOSE.search(look_forward)
        if ref_close:
            actual_end = match_end + ref_close.end()
        else:
            # ② 找 ）或 )（citation 的收尾括號），取最早出現者
            paren_fw = look_forward.find('）')
            paren_hw = look_forward.find(')')
            candidates_p = [p for p in [paren_fw, paren_hw] if p != -1]
            paren_pos = min(candidates_p) if candidates_p else -1
            if paren_pos != -1:
                actual_end = match_end + paren_pos + 1
            else:
                # fallback：找 。 或 \r\n
                candidates = []
                if '。' in look_forward:
                    candidates.append(look_forward.find('。'))
                if '\r\n' in look_forward:
                    candidates.append(look_forward.find('\r'))
                actual_end = match_end + (min(candidates) + 1 if candidates else max_forward_paren)

    return text[actual_start: actual_end]


# =========================
# 測試
# =========================
if __name__ == "__main__":
    cases = [
        # 標準格式
        ("標準", "本件依最高法院113年度台上字第3527號判決意旨，認定如下。"),
        # 無「度」字
        ("無度字", "參照最高法院40年台上字第86號判例，本院認為。"),
        # 大法庭：刑事大法庭前置修飾語，有字
        ("大法庭_前置有字", "最高法院刑事大法庭110年度台上大字第5660號裁定意旨。"),
        # 大法庭：刑事大法庭前置修飾語，省略字
        ("大法庭_前置無字", "最高法院刑事大法庭110年度台上大第5660號裁定。"),
        # 大法庭：民事大法庭後置修飾語
        ("大法庭_後置", "最高法院108年度台抗大字第897號民事大法庭裁定。"),
        # 大法庭：大法庭前置（無刑/民前綴）
        ("大法庭_簡", "最高法院大法庭113年度台上大字第4096號裁定意旨。"),
        # 最高行政法院大法庭
        ("行政大法庭", "最高行政法院大法庭111年度大字第1號裁定結論參照。"),
        # 憲法法庭
        ("憲法法庭", "憲法法庭112年度憲判字第2號判決意旨參照。"),
        # 省略引用（應抓到 2 筆：100號、200號）
        ("省略引用", "依最高法院112年度台抗字第100號、114年度台上字第200號判決。"),
        # 複雜鏈：118號是最高法院，後接高等法院，108、96年度繼承高等法院（不應捕捉）
        ("複雜鏈", (
            "最高法院113年度台抗字第118號、臺灣高等法院110年度抗字第1441號、"
            "108年度抗字第912號、96年度抗字第783號民事裁定意旨參照。"
        )),
        # 不應匹配（高等法院，無最高法院）
        ("高等法院", "臺灣高等法院113年度上字第100號判決。"),
        # 會議決議
        ("決議", "最高法院77年度第9次民事庭會議決議參照。"),
        # 釋字（guard：50字內需有「參」或「見」）
        ("釋字_全名", "司法院大法官釋字第144號解釋意旨參照，本院認為如下。"),
        ("釋字_短名", "大法官釋字第679號解釋，可資參見。"),
        # 釋字_無參見 → guard 應過濾，不產生 citation
        ("釋字_無引用", "本件不涉及司法院大法官釋字第144號所解釋之問題。"),
        # 法律座談會：有提案號
        ("座談會_有號", "臺灣高等法院暨所屬法院111年法律座談會民事類提案第21號研討結果。"),
        # 法律座談會：中文數字號
        ("座談會_中文號", "102年度高等行政法院法律座談會提案十二研討結果。"),
        # 法律座談會：無提案號
        ("座談會_無號", "臺灣高等法院暨所屬法院民國94年11月25日94年度法律座談會決議。"),
    ]

    for label, text in cases:
        print(f"\n【{label}】")
        results = extract_citations(text)
        if results:
            for r in results:
                if r["citation_type"] == "decision":
                    print(f"  ✅ [decision] [{r['court']}] {r['raw_match']}")
                    print(f"     jyear={r['jyear']}, jcase={r['jcase_norm']}, jno={r['jno']}")
                    print(f"     offset: {r['match_start']}~{r['match_end']}")
                else:
                    print(f"  ✅ [authority/{r['auth_type']}] {r['display']}")
                    print(f"     auth_key={r['auth_key']}")
                    print(f"     raw={r['raw_match']!r}  offset={r['match_start']}~{r['match_end']}")
        else:
            print("  （無命中）")
