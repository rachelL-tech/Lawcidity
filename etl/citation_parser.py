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
def extract_snippet(
    text: str,
    match_start: int,
    match_end: int,
    base_window: int = 200,
    expand_limit: int = 100,
    max_window: int = 500,
) -> str:
    """
    以 citation match 為中心，取前後 base_window 個字，
    再往外擴張到最近的句子邊界（\\n 或 。），
    額外擴張不超過 expand_limit，總上限 max_window。
    """
    raw_start = max(0, match_start - base_window)
    raw_end = min(len(text), match_end + base_window)

    # 向前擴張
    expand_back = min(expand_limit, raw_start)
    search_back = text[raw_start - expand_back: raw_start]
    boundary_back = max(search_back.rfind('\n'), search_back.rfind('。'))
    if boundary_back != -1:
        actual_start = raw_start - expand_back + boundary_back + 1
    else:
        actual_start = raw_start

    # 向後擴張
    expand_forward = min(expand_limit, len(text) - raw_end)
    search_forward = text[raw_end: raw_end + expand_forward]
    boundary_forward = min(
        search_forward.find('\n') if '\n' in search_forward else expand_forward,
        search_forward.find('。') if '。' in search_forward else expand_forward,
    )
    actual_end = raw_end + boundary_forward

    # max_window 上限
    actual_start = max(actual_start, match_start - max_window)
    actual_end = min(actual_end, match_end + max_window)

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
