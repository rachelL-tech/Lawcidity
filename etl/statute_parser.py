"""
從判決文字抽取法條引用（白名單 + 狀態機版本）

設計：
  1. LAW_NAMES 白名單 → 精確比對法律名稱，完全避免 greedy regex 噪音
  2. 狀態機三層：
     ① ABBR_ITEM_RE.match()：省略法名的縮略項款（繼承 current_law + current_article）
     ② ABBR_ARTICLE_RE.match()：省略法名的連續條號（繼承 current_law）
     ③ LAW_ARTICLE_RE.search()：具名法條（從白名單命中）
  3. PSEUDO_LAWS：虛指詞（本法/同法 等）不更新 current_law，不 append
  4. 每次命中條號後立即 _parse_qualifier()，抽取附隨的項/款/目/前後段
"""
import re
from typing import List, Tuple, Optional

from law_names import LAW_NAMES, PSEUDO_LAWS, normalize_law_name

# =========================
# Regex 建構
# =========================

# 按長度降序排列，避免短名遮蔽長名（例如「民法」遮蔽「民事訴訟法」）
_ALL_NAMES = sorted(set(LAW_NAMES) | PSEUDO_LAWS, key=len, reverse=True)

# 具名法條：白名單法律（含虛指詞）+ 第 X 條（含 之N 附號）
# group(1) = 法律名稱, group(2) = 條號, group(3) = 之N 附號（可能為 None）
# 台灣法律條號格式：第29條（無附號）或 第29條之1（附號在「條」之後）
LAW_ARTICLE_RE = re.compile(
    r'(' + '|'.join(re.escape(n) for n in _ALL_NAMES) + r')\s*(?:（[^）]*）\s*)?第\s*(\d+)\s*條(?:之\s*(\d+))?'
)

# 省略法名的連續條號（含前置項款修飾詞 & 條之N 附號）
# group(1) = 條號, group(2) = 之N 附號（可能為 None）
ABBR_ARTICLE_RE = re.compile(
    r'(?:'
    r'(?:第\s*\d+\s*[項款目]|前段|後段|但書|本文)\s*'   # 項款修飾詞（跳過）
    r'|[、，及與暨或,]\s*(?!第\s*\d+\s*條)'              # 分隔符（後方不接條號）
    r')*'
    r'[、，及與暨或,]\s*第\s*(\d+)\s*條(?:之\s*(\d+))?'  # 分隔符 + 條號（含之N）
)

# ── Qualifier tokens（條號之後）──────────────────────────────────────────────
# 第X項 / 第X款 / 第X目（允許數字前後有空白，如「第1 項」）
_ITEM_TOKEN_RE  = re.compile(r'第\s*(\d+)\s*([項款目])')
# 第X款 / 第X目（Phase A 延伸：項後面的款/目；不含項本身）
_CLAUSE_TOKEN_RE = re.compile(r'第\s*(\d+)\s*([款目])')
# 多號縮略：第X、Y項（qualifier 位置）
_MULTI_TOKEN_RE = re.compile(r'第\s*(\d+)\s*[、，]\s*(\d+)\s*([項款目])')
# 修飾詞（terminal）
_MODIFIER_RE    = re.compile(r'前段|後段|但書|本文')

# ── Abbreviated item/clause（分隔符 + 項/款/目）────────────────────────────
# 縮略單號：「、第X項」「及第X款」
_ABBR_ITEM_RE  = re.compile(r'[、，及與暨或,]\s*第\s*(\d+)\s*([項款目])')
# 縮略多號：「、第X、Y項」
_ABBR_MULTI_RE = re.compile(r'[、，及與暨或,]\s*第\s*(\d+)\s*[、，]\s*(\d+)\s*([項款目])')

# 文內簡稱：例如「勞動基準法（下稱勞基法）」「民事訴訟法（簡稱民訴法）」
ABBR_ALIAS_RE = re.compile(
    r'(' + '|'.join(re.escape(n) for n in sorted(set(LAW_NAMES), key=len, reverse=True)) + r')'
    r'\s*（(?:下稱|簡稱)\s*「?([^」）]{1,20})」?）'
)


def _extract_inline_law_aliases(text: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in ABBR_ALIAS_RE.finditer(text):
        canonical = normalize_law_name(m.group(1))
        alias = normalize_law_name(m.group(2))
        if not alias or alias in PSEUDO_LAWS or alias == canonical:
            continue
        out[alias] = canonical
    return out


def _normalize(law: str, inline_aliases: dict[str, str] | None = None) -> str:
    return normalize_law_name(law, aliases=inline_aliases)


def _parse_qualifier(text: str, pos: int) -> Tuple[List[str], int]:
    """
    條號之後，貪婪解析 qualifier 鏈，回傳 sub_ref 清單與新位置。

    支援：
    - 「第X項第Y款第Z目前段」→ ['第X項第Y款第Z目前段']
    - 「第X、Y項」→ ['第X項', '第Y項']（多號縮略）
    - 無 qualifier → ['']

    Returns: (list_of_sub_refs, new_pos)
    """
    # 先試多號縮略：第X、Y[項款目]
    m = _MULTI_TOKEN_RE.match(text, pos)
    if m:
        unit = m.group(3)
        return [f'第{m.group(1)}{unit}', f'第{m.group(2)}{unit}'], m.end()

    # 逐 token 解析：(第X項)* (第X款)* (第X目)* modifier?
    parts: List[str] = []
    while pos < len(text):
        m = _ITEM_TOKEN_RE.match(text, pos)
        if m:
            parts.append(f'第{m.group(1)}{m.group(2)}')
            pos = m.end()
            continue
        m = _MODIFIER_RE.match(text, pos)
        if m:
            parts.append(m.group(0))
            pos = m.end()
            break
        break

    return [''.join(parts)], pos


def _parse_item_extension(text: str, pos: int) -> Tuple[str, int]:
    """
    Phase A 縮略項後，繼續解析附隨的款/目/修飾詞（不含項，避免跨 item）。
    Returns: (suffix_str, new_pos)
    """
    parts: List[str] = []
    while pos < len(text):
        m = _CLAUSE_TOKEN_RE.match(text, pos)
        if m:
            parts.append(f'第{m.group(1)}{m.group(2)}')
            pos = m.end()
            continue
        m = _MODIFIER_RE.match(text, pos)
        if m:
            parts.append(m.group(0))
            pos = m.end()
            break
        break
    return ''.join(parts), pos


# =========================
# 抽取函式
# =========================
def extract_statutes(text: str) -> List[Tuple[str, str, str, str]]:
    """
    從文字抽取法條引用（含項/款/目，狀態機版）

    演算法：
    1. 在文字上線性掃描
    2. ① ABBR_ITEM_RE.match(pos)（只在 current_article 存在時）
          → 成功：繼承 current_law + current_article；解析延伸款/目；append；pos 前進
          → 失敗：進 ②
       ② ABBR_ARTICLE_RE.match(pos)（只在 current_law 存在時）
          → 成功：更新 current_article；parse_qualifier；append；pos 跳到 qualifier 後
          → 失敗：進 ③
       ③ LAW_ARTICLE_RE.search(pos)
          → 命中：更新 current_law / current_article；parse_qualifier；append；pos 跳到後
          → 找不到 → break

    Args:
        text: 判決全文或 snippet

    Returns:
        List of (law, article_raw, sub_ref, raw_match)
        - law:         正規化法律名稱（臺→台）
        - article_raw: 條號字串（如 '55', '29之1'）
        - sub_ref:     項/款/目 qualifier（如 '第1項第1款', '前段', ''）
        - raw_match:   原始命中片段（法條來源文字）
        每個 (law, article_raw, sub_ref) 只回傳第一次命中（去重）
    """
    if not text:
        return []

    inline_aliases = _extract_inline_law_aliases(text)

    results: List[Tuple[str, str, str, str]] = []
    seen: set = set()
    current_law: Optional[str] = None
    current_article: Optional[str] = None
    pos = 0

    def emit(law: str, article: str, sub: str, raw: str) -> None:
        key = (law, article, sub)
        if key not in seen:
            seen.add(key)
            results.append((law, article, sub, raw))

    while pos < len(text):
        # ① 省略項款（只在 chain 進行中才嘗試）
        if current_article is not None:
            # 多號縮略：[sep]第X、Y[項款目]
            m = _ABBR_MULTI_RE.match(text, pos)
            if m:
                unit = m.group(3)
                for n in [m.group(1), m.group(2)]:
                    emit(current_law, current_article, f'第{n}{unit}', m.group(0))
                pos = m.end()
                continue

            # 單號縮略：[sep]第X[項款目]
            m = _ABBR_ITEM_RE.match(text, pos)
            if m:
                base = f'第{m.group(1)}{m.group(2)}'
                ext, new_pos = _parse_item_extension(text, m.end())
                raw = text[pos: new_pos]  # 含延伸的款/目/修飾詞
                emit(current_law, current_article, base + ext, raw)
                pos = new_pos
                continue

        # ② 省略法名的連續條號
        if current_law is not None:
            m = ABBR_ARTICLE_RE.match(text, pos)
            if m:
                suf = f'之{m.group(2)}' if m.group(2) else ''
                current_article = m.group(1) + suf
                raw_art = f'第{m.group(1)}條{suf}'
                sub_refs, pos = _parse_qualifier(text, m.end())
                for sub in sub_refs:
                    emit(current_law, current_article, sub, raw_art)
                continue

        # ③ 具名法條（白名單比對）
        full = LAW_ARTICLE_RE.search(text, pos)
        if full is None:
            break

        law_raw = full.group(1)
        law = _normalize(law_raw, inline_aliases)
        suf = f'之{full.group(3)}' if full.group(3) else ''
        article = full.group(2) + suf

        if law in PSEUDO_LAWS or law_raw in PSEUDO_LAWS:
            # 虛指詞（同法/本法 等）：繼承 current_law（若有的話）
            if current_law is not None:
                current_article = article
                sub_refs, pos = _parse_qualifier(text, full.end())
                for sub in sub_refs:
                    emit(current_law, current_article, sub, full.group(0))
            else:
                pos = full.end()
        else:
            current_law = law
            current_article = article
            sub_refs, pos = _parse_qualifier(text, full.end())
            for sub in sub_refs:
                emit(current_law, current_article, sub, full.group(0))

    return results


# =========================
# 測試
# =========================
if __name__ == '__main__':
    cases = [
        # 原始測試
        ('民法/刑法',    '依民法第184條第1項前段，及刑法第277條第1項。'),
        ('連續條號',     '民事訴訟法第447條、第449條、第450條。'),
        ('跨法',        '民法第184條及公司法第8條第2項規定。'),
        ('虛指詞',      '民法第184條、同法第185條及同法第186條。'),
        # 新增項款測試
        ('Issue1',      '強制執行法第1條第2項及第122條等規定'),
        ('Issue2',      '民法第184條第1項前段、第2項前段及第195條第1項前段'),
        ('Issue-之1',   '銀行法第29條第1項、第29條之1分別定有明文'),
        ('款',          '勞基法第55條第1項第1款之工作年資'),
        ('多號縮略',    '民事訴訟法第77條之1第1、2項'),
        ('項+新條',     '土地法第104條第1項前段、第2項定有明文'),
        ('之2後段',     '勞基法第84條之2後段及第55條第1項、第2項規定'),
        ('但書',        '民事訴訟法第249條第1項但書、第444條第1項但書亦有'),
        ('空白',        '民法第474 條第1 項'),
        ('多項+之1',    '銀行法第29條第1項、第2項、第29條之1定有明文'),
        ('括號簡稱',    '依勞動基準法（下稱勞基法）第59條、第13條及勞工請假規則第6條'),
    ]

    for label, text in cases:
        print(f'\n【{label}】 {text}')
        for law, art, sub, raw in extract_statutes(text):
            label_sub = sub if sub else '（無）'
            print(f'  {law} 第{art}條 {label_sub} ← {raw!r}')
