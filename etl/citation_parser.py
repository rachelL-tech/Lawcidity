"""
從判決全文抽取引用最高法院判決的 citation

Week 1 只抓「最高法院」，其他法院忽略（記 log）
"""
import re
from typing import List, Dict, Optional

# =========================
# 核心 regex
# =========================
# 匹配格式：最高法院 113 年度台上字第 3527 號
# 涵蓋：標準格式、無「度」字、大法庭（台上大）、判例（忽略「判例」後綴）
#
# 三個捕捉群組：
#  - (\d{2,3}) → 年份（2-3 位數，例如 113、40）
#  - ([台臺][^字]+?) → 字別（非貪婪，遇到「字」就停，不會跨引用吃掉下一個）
#  - (\d+) → 案號（例如 3527）
#
# [^字]+? 比 \w+ 更安全：\w+ 在 Unicode 會貪婪到空白以外的所有字，
# 容易把「台抗字第100號及最高法院113年度台上」整個吃掉

CITATION_PATTERN = re.compile(
    r'最高法院\s*(\d{2,3})\s*年\s*度?\s*([台臺][^字]+?)\s*字\s*第\s*(\d+)\s*號'
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
# 抽取 citations
# =========================
def extract_citations(full_text: str) -> List[Dict]:
    """
    從全文抽取所有引用最高法院判決的 citation

    流程：
    1. 在預處理文字上跑 regex（避免換行斷開 match）
    2. 用 raw_match 回頭在原始 full_text 搜尋，取得正確偏移量
    3. snippet 從原始 full_text 取（保留換行，方便邊界擴張）

    Returns:
        List of {
            "raw_match": str,    # 原始命中字串
            "match_start": int,  # 在原始 full_text 的起點（None 若找不到）
            "match_end": int,    # 在原始 full_text 的終點（None 若找不到）
            "snippet": str,      # ±200 字 + 邊界擴張的上下文
            "jyear": int,
            "jcase_norm": str,
            "jno": int,
        }
    """
    processed = preprocess_text(full_text)
    results = []

    for m in CITATION_PATTERN.finditer(processed):
        jyear_str, jcase_raw, jno_str = m.group(1), m.group(2), m.group(3)

        # 正規化 jcase：移除空白、臺→台
        jcase_norm = jcase_raw.replace(' ', '').replace('臺', '台')

        raw_match = m.group(0)

        # 偏移量修正：在原始 full_text 裡搜尋 raw_match
        orig = re.search(re.escape(raw_match), full_text)
        if orig:
            match_start = orig.start()
            match_end = orig.end()
            snippet = extract_snippet(full_text, match_start, match_end)
        else:
            # 原始文字找不到（例如換行把字串切斷），退回預處理文字的位置
            match_start = None
            match_end = None
            snippet = extract_snippet(processed, m.start(), m.end())

        results.append({
            "raw_match": raw_match,
            "match_start": match_start,
            "match_end": match_end,
            "snippet": snippet,
            "jyear": int(jyear_str),
            "jcase_norm": jcase_norm,
            "jno": int(jno_str),
        })

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

    Args:
        text: 原始全文（建議保留換行，方便邊界擴張）
        match_start: citation 起點
        match_end: citation 終點
        base_window: 基礎前後各取字數（預設 200）
        expand_limit: 邊界擴張上限（預設 100）
        max_window: 前後各最大字數（預設 500）

    Returns:
        snippet 字串
    """
    # 基礎視窗
    raw_start = max(0, match_start - base_window)
    raw_end = min(len(text), match_end + base_window)

    # 向前擴張到最近的 \n 或 。（往 raw_start 左邊找）
    expand_back = min(expand_limit, raw_start)
    search_back = text[raw_start - expand_back: raw_start]
    # 從右往左找邊界（離 match 最近的邊界）
    boundary_back = max(
        search_back.rfind('\n'),
        search_back.rfind('。')
    )
    if boundary_back != -1:
        # +1 跳過邊界字元本身
        actual_start = raw_start - expand_back + boundary_back + 1
    else:
        actual_start = raw_start

    # 向後擴張到最近的 \n 或 。（往 raw_end 右邊找）
    expand_forward = min(expand_limit, len(text) - raw_end)
    search_forward = text[raw_end: raw_end + expand_forward]
    boundary_forward = min(
        search_forward.find('\n') if '\n' in search_forward else expand_forward,
        search_forward.find('。') if '。' in search_forward else expand_forward,
    )
    actual_end = raw_end + boundary_forward

    # 套用 max_window 上限
    actual_start = max(actual_start, match_start - max_window)
    actual_end = min(actual_end, match_end + max_window)

    return text[actual_start: actual_end]


# =========================
# 測試
# =========================
if __name__ == "__main__":
    sample_texts = [
        # 標準格式
        "本件依最高法院113年度台上字第3527號判決意旨，認定如下。",
        # 無「度」字（舊案）
        "參照最高法院40年台上字第86號判例，本院認為。",
        # 大法庭
        "最高法院110年度台上大字第5660號裁定意旨。",
        # 多個引用（先前 \w+ 會跨引用吃掉，現在 [^字]+? 應正確分開）
        "依最高法院112年度台抗字第100號及最高法院113年度台上字第200號判決。",
        # 不應匹配（高等法院）
        "臺灣高等法院113年度上字第100號判決。",
    ]

    for text in sample_texts:
        print(f"\n輸入：{text[:60]}...")
        results = extract_citations(text)
        if results:
            for r in results:
                print(f"  ✅ {r['raw_match']} → jyear={r['jyear']}, jcase={r['jcase_norm']}, jno={r['jno']}")
                print(f"     offset: {r['match_start']}~{r['match_end']}")
        else:
            print("  （無命中）")
