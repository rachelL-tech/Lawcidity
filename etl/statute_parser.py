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
# group(1) = 法律名稱, group(2) = 條號, group(3) = 之N 附號（可能為 None）
# 台灣法律條號格式：第29條（無附號）或第29條之1（附號在「條」之後）
LAW_ARTICLE_RE = re.compile(
    r'(' + '|'.join(re.escape(n) for n in _ALL_NAMES) + r')\s*第\s*(\d+)\s*條(?:之\s*(\d+))?'
)

# 省略法名的連續條號（含前置項款修飾詞 & 條之N 附號）
# 支援：「、第X條」「及第X條」（無修飾詞）
#       「第1條第2項及第122條」（後接項號）
#       「第184條第1項前段、第2項前段及第195條」（項號 + 分隔 + 項號 + 分隔 + 條號）
#       「第29條第1項、第29條之1」（條之N 附號）
# 原理：跳過「第N項/款」「前段/後段/但書/本文」及「不接條號的分隔符」，
#       直到遇到「[分隔符]第N條（之M）」才命中。
# group(1) = 條號, group(2) = 之N 附號（可能為 None）
ABBR_ARTICLE_RE = re.compile(
    r'(?:'
    r'(?:第\s*\d+\s*[項款]|前段|後段|但書|本文)\s*'   # 項款修飾詞
    r'|[、，及與暨或,]\s*(?!第\s*\d+\s*條)'            # 分隔符（後方不接條號）
    r')*'
    r'[、，及與暨或,]\s*第\s*(\d+)\s*條(?:之\s*(\d+))?'  # 真正的分隔符 + 條號（含之N）
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
                _suf = f'之{abbr.group(2)}' if abbr.group(2) else ''
                article_raw = abbr.group(1) + _suf
                raw = f'第{abbr.group(1)}條{_suf}'
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
        _suf = f'之{full.group(3)}' if full.group(3) else ''
        article_raw = full.group(2) + _suf

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