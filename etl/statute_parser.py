"""
從判決文字抽取法條引用（白名單 + 狀態機版本）

設計：
  1. LAW_NAMES 白名單 → 精確比對法律名稱，完全避免 greedy regex 噪音
  2. 狀態機（同 citation_parser.py 邏輯）：
     ① ABBR_ARTICLE_RE.match()：省略法名的連續條號（繼承 current_law）
     ② LAW_ARTICLE_RE.search()：具名法條（從白名單命中）
  3. PSEUDO_LAWS：虛指詞（本法/同法 等）不更新 current_law，不 append
"""
import re
from typing import List, Tuple, Optional

from law_names import LAW_NAMES, PSEUDO_LAWS

# =========================
# Regex 建構
# =========================

# 按長度降序排列，避免短名遮蔽長名（例如「民法」遮蔽「民事訴訟法」）
# PSEUDO_LAWS 也納入 regex（讓 pos 能正確前進），match 到時才在邏輯層過濾
_ALL_NAMES = sorted(set(LAW_NAMES) | PSEUDO_LAWS, key=len, reverse=True)

# 具名法條：白名單法律（含虛指詞）+ 第 X 條
# group(1) = 法律名稱, group(2) = 條號
LAW_ARTICLE_RE = re.compile(
    r'(' + '|'.join(re.escape(n) for n in _ALL_NAMES) + r')\s*第\s*(\d+)\s*條'
)

# 省略法名的連續條號：「、第X條」「及第X條」等
# group(1) = 條號
ABBR_ARTICLE_RE = re.compile(
    r'[、，及與暨或,]\s*第\s*(\d+)\s*條'
)

# 臺→台 正規化
def _normalize(law: str) -> str:
    return law.replace('臺', '台')


# =========================
# 抽取函式
# =========================
def extract_statutes(text: str) -> List[Tuple[str, str, str]]:
    """
    從文字抽取法條引用（狀態機版）

    演算法：
    1. 在文字上線性掃描
    2. ① ABBR_ARTICLE_RE.match(pos)（只在 current_law 存在時）
          → 成功：繼承 current_law；append；pos 前進
          → 失敗：進 ②
       ② LAW_ARTICLE_RE.search(pos)
          → 命中白名單法律：更新 current_law（或忽略 PSEUDO_LAWS）；append；pos 跳到 end
          → 找不到 → break

    Args:
        text: 判決全文或 snippet

    Returns:
        List of (law, article_raw, raw_match)
        - law:         正規化法律名稱（臺→台）
        - article_raw: 條號字串（對應 DB article_raw TEXT 欄位）
        - raw_match:   原始命中片段
        每個 (law, article_raw) 只回傳第一次命中（去重）
    """
    if not text:
        return []

    results: List[Tuple[str, str, str]] = []
    seen: set = set()
    current_law: Optional[str] = None
    pos = 0

    while pos < len(text):
        # ① 省略法名的連續條號（只在 chain 進行中才嘗試）
        if current_law is not None:
            abbr = ABBR_ARTICLE_RE.match(text, pos)
            if abbr:
                article_raw = abbr.group(1)
                raw = abbr.group(0)[1:].lstrip()   # 跳過分隔符
                key = (current_law, article_raw)
                if key not in seen:
                    seen.add(key)
                    results.append((current_law, article_raw, raw))
                pos = abbr.end()
                continue

        # ② 具名法條（白名單比對）
        full = LAW_ARTICLE_RE.search(text, pos)
        if full is None:
            break

        law_raw = full.group(1)
        law = _normalize(law_raw)
        article_raw = full.group(2)

        if law in PSEUDO_LAWS or law_raw in PSEUDO_LAWS:
            # 虛指詞（同法/本法 等）：繼承 current_law（若有的話）
            # 例：「民法第184條、同法第767條」→ 同法 = 民法
            if current_law is not None:
                key = (current_law, article_raw)
                if key not in seen:
                    seen.add(key)
                    results.append((current_law, article_raw, full.group(0)))
            # current_law 不變
        else:
            current_law = law
            key = (law, article_raw)
            if key not in seen:
                seen.add(key)
                results.append((law, article_raw, full.group(0)))

        pos = full.end()

    return results


# =========================
# 測試
# =========================
if __name__ == '__main__':
    cases = [
        # 兩字法律名稱
        ('民法/刑法', '依民法第184條第1項前段，及刑法第277條第1項。'),
        # 連續條號
        ('連續條號', '民事訴訟法第447條、第449條、第450條。'),
        # 跨法
        ('跨法', '民法第184條及公司法第8條第2項規定。'),
        # 虛指詞（同法 不 append，chain 繼續）
        ('虛指詞', '民法第184條、同法第185條及同法第186條。'),
        # 縮寫
        ('縮寫', '依勞基法第14條及勞退條例第6條。'),
        # 臺→台 正規化
        ('臺→台', '依臺灣地區與大陸地區人民關係條例第2條規定。'),
        # 噪音（白名單抓不到噪音前綴，只命中法名本身）
        ('噪音前綴', '上訴人依民法第184條及被上訴人依民事訴訟法第47條。'),
    ]

    for label, text in cases:
        print(f'\n【{label}】')
        print(f'  {text}')
        for law, art, raw in extract_statutes(text):
            print(f'  {law} 第{art}條  ← {raw!r}')
