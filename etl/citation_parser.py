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
CITATION_PATTERN = re.compile(
    r'最高法院\s*(\d{2,3})\s*年\s*度?\s*([台臺]\s*\w+)\s*字\s*第\s*(\d+)\s*號'
)

# 用於從 raw_match 解析自然鍵
PARSE_PATTERN = re.compile(
    r'最高法院\s*(\d{2,3})\s*年\s*度?\s*([台臺][^\s字]*)\s*字\s*第\s*(\d+)\s*號'
)


# =========================
# 預處理
# =========================
def preprocess_text(text: str) -> str:
    """移除換行與多餘空白，讓 regex 更穩定"""
    text = text.replace('\r\n', '').replace('\n', '').replace('\r', '')
    text = re.sub(r'\s+', ' ', text)
    return text


# =========================
# 抽取 citations
# =========================
def extract_citations(full_text: str) -> List[Dict]:
    """
    從全文抽取所有引用最高法院判決的 citation

    Returns:
        List of {
            "raw_match": str,    # 原始命中字串
            "match_start": int,  # 在原始 full_text 的起點
            "match_end": int,    # 在原始 full_text 的終點
            "snippet": str,      # ±200 字的上下文
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

        snippet = extract_snippet(processed, m.start(), m.end())

        results.append({
            "raw_match": m.group(0),
            "match_start": m.start(),
            "match_end": m.end(),
            "snippet": snippet,
            "jyear": int(jyear_str),
            "jcase_norm": jcase_norm,
            "jno": int(jno_str),
        })

    return results


# =========================
# Snippet 擷取
# =========================
def extract_snippet(text: str, match_start: int, match_end: int, window: int = 200) -> str:
    """
    以 citation match 為中心，取前後 window 個字

    Args:
        text: 預處理後的全文
        match_start: regex match 起點
        match_end: regex match 終點
        window: 前後各取幾個字（預設 200）

    Returns:
        snippet 字串
    """
    start = max(0, match_start - window)
    end = min(len(text), match_end + window)
    return text[start:end]


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
        # 多個引用
        "依最高法院112年度台抗字第100號及最高法院113年度台上字第200號判決。",
        # 不應匹配（高等法院）
        "臺灣高等法院113年度上字第100號判決。",
    ]

    for text in sample_texts:
        print(f"\n輸入：{text[:50]}...")
        results = extract_citations(text)
        if results:
            for r in results:
                print(f"  ✅ {r['raw_match']} → jyear={r['jyear']}, jcase={r['jcase_norm']}, jno={r['jno']}")
        else:
            print("  （無命中）")
