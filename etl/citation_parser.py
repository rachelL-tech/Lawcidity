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
    r'([台臺][^字]{1,20}?)\s*字\s*第\s*(\d+)\s*號'
)

# 省略法院名的引用（承接前一個 citation 的 current_court）
# 開頭是分隔符號（、，等），後面直接接年份數字
# 當鏈中出現具名法院（漢字開頭）時，ABBR.match() 自然失敗，鏈中斷
# group(1) = 年，group(2) = 字別，group(3) = 案號
ABBR_CITATION = re.compile(
    r'[、，及與暨或,]\s*(\d{2,3})\s*年\s*度?\s*([台臺][^字]{1,20}?)\s*字\s*第\s*(\d+)\s*號'
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
    clean_text: str,
    processed: str,
    fallback_start: int,
    fallback_end: int,
) -> Dict:
    """
    建構 citation result dict。
    1. 先在 clean_text 直接搜尋 raw_match（完整字串）
    2. 找不到（PDF 折行造成 \\r\\n 中斷）→ 改用 flexible pattern（允許任意空白）重新定位
    3. 兩者都失敗 → fallback 到 processed 偏移（snippet 品質最差）
    """
    jcase_norm = jcase_raw.replace(' ', '').replace('臺', '台')

    orig = re.search(re.escape(raw_match), clean_text)
    if orig:
        match_start = orig.start()
        match_end   = orig.end()
        snippet = extract_snippet(clean_text, match_start, match_end)
    else:
        # PDF 折行：citation 中間有 \r\n，逐字允許 \s* 重新定位
        flexible = r'[\s\r\n]*'.join(re.escape(c) for c in raw_match)
        flex = re.search(flexible, clean_text)
        if flex:
            match_start = flex.start()
            match_end   = flex.end()
            snippet = extract_snippet(clean_text, match_start, match_end)
        else:
            match_start = None
            match_end   = None
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
        List of {
            "court": str,        # 被引用法院（正規化）
            "raw_match": str,    # 原始命中字串
            "match_start": int,  # 在 clean_text 的起點（PDF 折行無法定位時為 None）
            "match_end": int,    # 在 clean_text 的終點
            "snippet": str,
            "jyear": int,
            "jcase_norm": str,
            "jno": int,
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
                clean_text=clean_text,
                processed=processed,
                fallback_start=full.start(),
                fallback_end=full.end(),
            ))
        pos = full.end()

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

# 子條款起點：「再按」「復按」「又按」「次按」「末按」「且按」「惟按」
#             「再者」「所謂」「另」「按」（行首）
# 邊界允許 。 或 \r\n 作前導；前導後可有少量標點/PUA 字元（如 \uf6aa、㈠、⑴ 等）
# \u3200-\u32ff：Enclosed CJK（㈠㈡…㊿），出現於 \r\n 與關鍵字之間
# \u2460-\u24ff：Enclosed Alphanumerics（①②…、⑴⑵…），處理附表內「。 ⑴按」格式
# group(1) = 關鍵字起始位置（用於 actual_start）
_SUB_CLAUSE_RE = re.compile(
    r'(?:(?:。|\r\n|[：:])[\uf000-\uffff\u3000-\u303f\u3200-\u32ff\u2460-\u24ff\t 　]{0,6})'  # 加：[：:] 邊界（修：：　　按 格式）
    r'((?:再|復|又|次|末|且|惟)按|又(?!按)|再者|所謂|另(?![行有外附])|按(?!照))'              # 加：又(?!按) 獨立關鍵字
    r'[：:，,「]?'
)

# 非標準法院引用（憲法法庭等）的結尾標記：「意旨參照）」
# 用於 extract_snippet 的 Pass 2 邊界修正
_CITE_TAIL_RE = re.compile(r'(?:意旨|決議)參照[）)]')


def extract_snippet(
    text: str,
    match_start: int,
    match_end: int,
    max_back: int = 3000,
    max_forward_paren: int = 150,
) -> str:
    """
    以 citation match 為中心切出 snippet：

    向前：在 para_cap 窗口內，同時找子條款（再按/復按 等）和編號段落起點（一、二、㈠ 等），
          取兩者中最靠近 match_start 者（max 位置）。
          窗口內都找不到 → 退回更遠的段落起點（超過 para_cap 則硬切）→ 任意換行 fallback。

    向後：找 match_end 之後最近的 ）（citation 收尾括號），在那裡截止；
          fallback：找 。 或 \\r\\n；都沒有則取到 max_forward_paren。
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

    # Pass 2: 意旨參照）（非標準法院如憲法法庭，不被 ANY_COURT_CITATION 匹配）
    in_lb_start2 = actual_start - look_back_start
    for m in _CITE_TAIL_RE.finditer(look_back, in_lb_start2):
        end_pos = look_back_start + m.end()
        while end_pos < match_start and text[end_pos] in '。\r\n \t　':
            end_pos += 1
        if end_pos < match_start:
            actual_start = end_pos

    # 向後：找 ）或 )（citation 的收尾括號），取最早出現者
    look_forward = text[match_end: match_end + max_forward_paren]
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
