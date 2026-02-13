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
# [\u4e00-\u9fff]+法院  → 比對任意漢字序列 + 「法院」
# (?:[\u4e00-\u9fff]+分院)?  → 選擇性比對分院後綴（臺中分院、金門分院 等）
# 範例：最高法院 / 臺灣高等法院 / 臺灣高等法院臺中分院 / 福建高等法院金門分院
ANY_COURT_CITATION = re.compile(
    r'((?:最高法院|(?:臺灣|台灣|福建)[\u4e00-\u9fff]*?法院)(?:[\u4e00-\u9fff]+分院)?)'
    r'\s*(\d{2,3})\s*年\s*度?\s*'
    r'([台臺][^字]+?)\s*字\s*第\s*(\d+)\s*號'
)

# 省略法院名的引用（承接前一個 citation 的 current_court）
# 開頭是分隔符號（、，等），後面直接接年份數字
# 當鏈中出現具名法院（漢字開頭）時，ABBR.match() 自然失敗，鏈中斷
# group(1) = 年，group(2) = 字別，group(3) = 案號
ABBR_CITATION = re.compile(
    r'[、，及與暨或,]\s*(\d{2,3})\s*年\s*度?\s*([台臺][^字]+?)\s*字\s*第\s*(\d+)\s*號'
)

# Week 1 只抓最高法院；Week 2+ 在這裡擴充
TARGET_COURTS: Set[str] = {'最高法院'}


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


def _make_result(
    court: str,
    raw_match: str,
    jyear_str: str,
    jcase_raw: str,
    jno_str: str,
    full_text: str,
    processed: str,
    fallback_start: int,
    fallback_end: int,
) -> Dict:
    """
    建構 citation result dict。
    先嘗試在原始 full_text 修正偏移量；找不到時退回 processed 的位置。
    """
    jcase_norm = jcase_raw.replace(' ', '').replace('臺', '台')

    orig = re.search(re.escape(raw_match), full_text)
    if orig:
        match_start = orig.start()
        match_end = orig.end()
        snippet = extract_snippet(full_text, match_start, match_end)
    else:
        match_start = None
        match_end = None
        snippet = extract_snippet(processed, fallback_start, fallback_end)

    return {
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
    full_text: str,
    target_courts: Set[str] = TARGET_COURTS,
) -> List[Dict]:
    """
    從全文抽取引用判決的 citation（狀態機版）

    演算法：
    1. 在預處理文字上線性掃描
    2. ① 嘗試 ABBR.match(pos)（只在 current_court 存在時）
          → 成功：繼承 current_court；若在 target_courts 內則 append；pos 前進
          → 失敗：進 ②
       ② ANY_COURT_CITATION.search(processed, pos)
          → 更新 current_court；若在 target_courts 內則 append；pos 跳到 full.end()
          → 找不到 → break
    3. 省略引用鏈遇到具名法院（漢字開頭）時，ABBR 自然失敗，current_court 由 ② 更新
    4. 偏移量修正至原始 full_text；snippet 從原始 full_text 取（保留換行）

    Args:
        full_text: 原始全文
        target_courts: 要抓的被引用法院（預設 {'最高法院'}）

    Returns:
        List of {
            "court": str,        # 被引用法院（正規化）
            "raw_match": str,    # 原始命中字串
            "match_start": int,  # 在原始 full_text 的起點（None 若找不到）
            "match_end": int,    # 在原始 full_text 的終點（None 若找不到）
            "snippet": str,
            "jyear": int,
            "jcase_norm": str,
            "jno": int,
        }
    """
    processed = preprocess_text(full_text)
    results = []
    current_court: Optional[str] = None
    pos = 0

    while pos < len(processed):
        # ① 省略引用：只在 chain 進行中（current_court 存在）才嘗試
        if current_court is not None:
            abbr = ABBR_CITATION.match(processed, pos)
            if abbr:
                if current_court in target_courts:
                    # group(0) = 「、114年度台抗字第310號」，strip 掉開頭分隔符號
                    a_raw = abbr.group(0)[1:].lstrip()
                    results.append(_make_result(
                        court=current_court,
                        raw_match=a_raw,
                        jyear_str=abbr.group(1),
                        jcase_raw=abbr.group(2),
                        jno_str=abbr.group(3),
                        full_text=full_text,
                        processed=processed,
                        fallback_start=abbr.start(1),  # 年份起點，跳過分隔符
                        fallback_end=abbr.end(),
                    ))
                pos = abbr.end()
                continue

        # ② 具名 citation：搜尋下一個（任意法院）
        full = ANY_COURT_CITATION.search(processed, pos)
        if full is None:
            break

        current_court = _normalize_court(full.group(1))
        if current_court in target_courts:
            results.append(_make_result(
                court=current_court,
                raw_match=full.group(0),
                jyear_str=full.group(2),
                jcase_raw=full.group(3),
                jno_str=full.group(4),
                full_text=full_text,
                processed=processed,
                fallback_start=full.start(),
                fallback_end=full.end(),
            ))
        pos = full.end()

    return results


# =========================
# Snippet 擷取（混合策略）
# =========================
# 有編號的段落起點（一、二、壹、貳、㈠㈡、①②、⑴⑵、⒈⒉ 等）
# 這些才是「真正段落起點」；非縮排的 PDF 折行 \r\n 後面不會接這些字元
_PARA_START_RE = re.compile(
    r'\r\n(?=[一二三四五六七八九十壹貳參肆伍陸柒捌玖'
    r'㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩'
    r'①②③④⑤⑥⑦⑧⑨⑩'
    r'⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽'   # 括號數字 U+2474–U+247D
    r'⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑'   # 數字句號 U+2488–U+2491
    r'（])'
)

# 子條款起點：「再按」「復按」「又按」「次按」「末按」「且按」「惟按」
#             「再者」「所謂」「另」「按」（行首）
# 邊界允許 。 或 \r\n 作前導；前導後可有少量標點/PUA 字元（如 \uf6aa、）
# group(1) = 關鍵字起始位置（用於 actual_start）
_SUB_CLAUSE_RE = re.compile(
    r'(?:(?:。|\r\n)[\uf000-\uffff\u3000-\u303f\t 　]{0,6})'
    r'((?:再|復|又|次|末|且|惟)按|再者|所謂|另(?![行有外附])|按(?!照))'
    r'[：:，,「]?'
)


def extract_snippet(
    text: str,
    match_start: int,
    match_end: int,
    max_back: int = 3000,
    max_forward_paren: int = 150,
) -> str:
    """
    以 citation match 為中心切出 snippet：

    向前優先順序：
    ① 子條款（再按/復按/又按）：match_start 前 para_cap 字內，最靠近 match_start 者
    ② 編號段落起點（一、二、壹、㈠ 等）：全 look_back 內最後一個
       距離 ≤ para_cap → 直接用
       距離 > para_cap → 從硬切點往前找最近 。，從句號後起頭
    ③ 任意 \\r\\n（fallback）
    ④ 固定距離 look_back_start（最終 fallback）

    向後：找 match_end 之後最近的 ）（citation 收尾括號），在那裡截止
          fallback：找 。 或 \\r\\n；都沒有則取到 max_forward_paren
    """
    para_cap: int = 600
    look_back_start = max(0, match_start - max_back)
    look_back = text[look_back_start: match_start]

    # ① 子條款：在最後 para_cap 字內找，取最靠近 match_start 的那個
    sub_window_pos = max(0, len(look_back) - para_cap)
    last_sub = None
    for m in _SUB_CLAUSE_RE.finditer(look_back, sub_window_pos):
        last_sub = m

    if last_sub is not None:
        actual_start = look_back_start + last_sub.start(1)  # group(1) = 關鍵字起點

    else:
        # ② 編號段落：全 look_back 找最後一個段落起點
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
            # ③ 任意換行
            any_newline = look_back.rfind('\r\n')
            actual_start = look_back_start + any_newline + 2 if any_newline != -1 else look_back_start

    # ★ 引用邊界後處理：若 actual_start ~ match_start 之間有其他法院具名引用，
    #   推進 actual_start 到最後一個引用收尾 ）之後（跳過空白/換行）
    in_lb_start = actual_start - look_back_start
    for m in ANY_COURT_CITATION.finditer(look_back, in_lb_start):
        after_cite = look_back_start + m.end()
        window = text[after_cite: after_cite + 80]
        paren_pos = window.find('）')
        end_pos = after_cite + paren_pos + 1 if paren_pos != -1 else after_cite
        # 跳過緊接的 。\r\n 空白
        while end_pos < match_start and text[end_pos] in '。\r\n \t　':
            end_pos += 1
        if end_pos < match_start:
            actual_start = end_pos

    # 向後：找 ）（citation 的收尾括號，如「意旨參照）」）
    look_forward = text[match_end: match_end + max_forward_paren]
    paren_pos = look_forward.find('）')
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
        # 大法庭
        ("大法庭", "最高法院110年度台上大字第5660號裁定意旨。"),
        # 省略引用（應抓到 2 筆：100號、200號）
        ("省略引用", "依最高法院112年度台抗字第100號、114年度台上字第200號判決。"),
        # 複雜鏈：118號是最高法院，後接高等法院，108、96年度繼承高等法院（不應捕捉）
        ("複雜鏈", (
            "最高法院113年度台抗字第118號、臺灣高等法院110年度抗字第1441號、"
            "108年度抗字第912號、96年度抗字第783號民事裁定意旨參照。"
        )),
        # 不應匹配（高等法院，無最高法院）
        ("高等法院", "臺灣高等法院113年度上字第100號判決。"),
    ]

    for label, text in cases:
        print(f"\n【{label}】")
        results = extract_citations(text)
        if results:
            for r in results:
                print(f"  ✅ [{r['court']}] {r['raw_match']}")
                print(f"     jyear={r['jyear']}, jcase={r['jcase_norm']}, jno={r['jno']}")
                print(f"     offset: {r['match_start']}~{r['match_end']}")
        else:
            print("  （無命中）")
