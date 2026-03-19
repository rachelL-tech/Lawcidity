"""
將判決原始全文（JFULL）清理成適合前端顯示的 clean_text。

目標：
1. 正規化法院 title 與主文 / 理由類 heading 的字間空白
2. 只在正文區合併 PDF 折行
3. 保留當事人名單區與簽署區的版面空白和換行
"""
import re
import json
from pathlib import Path


# 偵測同行內嵌大標題（某些 PDF 以 \u3000 代替 \r\n），如「起訴。　　理　由一、」
_INLINE_HEADING_RE = re.compile(
    r'([。！？])\u3000{2,}'
    r'(主\u3000*文'
    r'|理\u3000*由(?:\u3000*要\u3000*領)?'
    r'|事\u3000*實(?:\u3000*(?:及|與)\u3000*理\u3000*由(?:\u3000*要\u3000*領)?'
    r'|\u3000*理\u3000*由\u3000*及\u3000*證\u3000*據)?)'
    r'\u3000*'
)

_DATE_LINE_RE = re.compile(r'中[ \t\u3000]*華[ \t\u3000]*民[ \t\u3000]*國')
_DATE_LINE_ONLY_RE = re.compile(
    r'^[ \t\u3000]*'
    r'中[ \t\u3000]*華[ \t\u3000]*民[ \t\u3000]*國'
    r'[ \t\u3000]*\d{2,3}[ \t\u3000]*年'
    r'[ \t\u3000]*\d{1,2}[ \t\u3000]*月'
    r'[ \t\u3000]*\d{1,2}[ \t\u3000]*日'
    r'[ \t\u3000]*$'
)
_SECTION_INDEX_START_RE = re.compile(
    r'[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁'
    r'㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩'
    r'①②③④⑤⑥⑦⑧⑨⑩'
    r'⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽'
    r'⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑'
    r'0-9０-９（(]'
)
_BODY_BLOCK_START_RE = re.compile(
    r'^[ \t\u3000]*(?:'
    r'[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁]+[、：:](?![一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁][、：:])'
    r'|[㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩]'
    r'|[①②③④⑤⑥⑦⑧⑨⑩]'
    r'|[⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽]'
    r'|[⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑]'
    r'|[（(][一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁]+[）)]'
    r'|[0-9０-９]{1,3}[.、](?![0-9０-９])'   # 排除小數（如 4.9%）
    r')'
)
# 附表/附件/附錄 的 inline 引用排除（如「附表一編號」「附件一之」「附表四、五所示」），不視為段落起首
_ATTACHMENT_INLINE_RE = re.compile(
    r'附[錄表件]'
    r'(?:[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁0-9０-９]+'
    r'(?:[、，][一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁0-9０-９]+)*)?'
    r'(?:所示|編號|之|所列|明細)'
)
_KEEP_HEADERS = {"主文", "事實", "理由", "理由要領", "事實及理由", "事實與理由", "事實及理由要領", "事實理由及證據"}
_KEEP_SECTION_LINE_RE = re.compile(
    r'(?:'
    r'主文'
    r'|事實'
    r'|理由(?:要領)?'
    r'|事實(?:及|與)理由(?:要領)?'
    r'|(?:[一二三四五六七八九十壹貳參肆伍陸柒捌玖甲乙丙丁0-9０-９]+[、：:]?)'
    r'(?:原告(?:起訴|之|的)?主張|被告(?:答辯|抗辯)|被告則以'
    r'|本院(?:之|的)?判斷|本院判斷|得心證之理由|茲分敘理由如下'
    r'|程序方面|程序事項|程序部分|實體方面|實體事項|實體部分'
    r'|論罪科刑|沒收(?:部分)?|證據能力|原判決認定|原裁定略以|兩造不爭執事項)'
    r'[:：]?'
    r')$'
)
_COURT_TITLE_RE = re.compile(r'^.+法院.*(?:判決|裁定)$')
_REASON_HEADING_RE = re.compile(
    r'^(?:理由(?:要領)?|事實(?:及|與)?理由(?:要領)?|事實理由及證據|事實)$'
)


def _normalize_spaces(text: str) -> str:
    return re.sub(r'[ \t\u3000]+', '', text)


def _canonical_heading(line: str) -> str | None:
    leading = re.match(r'^[ \t\u3000]*', line).group(0)
    norm = _normalize_spaces(line.strip())
    if norm == "主文":
        return leading + "主文"
    if _REASON_HEADING_RE.fullmatch(norm):
        return leading + norm
    return None


def _canonical_title(line: str) -> str | None:
    norm = _normalize_spaces(line.strip())
    if _COURT_TITLE_RE.fullmatch(norm):
        return norm
    return None


def _normalize_body_line(line: str) -> str:
    heading = _canonical_heading(line)
    if heading is not None:
        return heading

    if line.startswith('\u3000'):
        stripped_full = line.lstrip('\u3000')
        if stripped_full and _SECTION_INDEX_START_RE.match(stripped_full):
            return stripped_full
    return line


def _is_body_keep_line(line: str) -> bool:
    norm = _normalize_spaces(line.strip())
    return norm in _KEEP_HEADERS or bool(_KEEP_SECTION_LINE_RE.fullmatch(norm))


def _starts_new_body_block(line: str) -> bool:
    stripped = line.lstrip(' \t\u3000')
    if not stripped:
        return True
    norm = _normalize_spaces(stripped)
    return (
        norm in _KEEP_HEADERS
        or (stripped.startswith("附錄") and not _ATTACHMENT_INLINE_RE.match(stripped))
        or (stripped.startswith("附表") and not _ATTACHMENT_INLINE_RE.match(stripped))
        or (stripped.startswith("附件") and not _ATTACHMENT_INLINE_RE.match(stripped))
        or norm.startswith("中華民國")
        or norm.startswith("如不服")
        or "正本證明" in norm
        or bool(_KEEP_SECTION_LINE_RE.fullmatch(norm))
        or _BODY_BLOCK_START_RE.match(stripped) is not None
    )


def _split_lines_preserve_trailing(text: str) -> tuple[list[str], bool]:
    trailing_newline = text.endswith('\r\n')
    if trailing_newline:
        text = text[:-2]
    return text.split('\r\n'), trailing_newline


def _join_lines(lines: list[str], trailing_newline: bool) -> str:
    text = '\r\n'.join(lines)
    if trailing_newline:
        text += '\r\n'
    return text


def _normalize_header_lines(lines: list[str], body_start_idx: int | None) -> list[str]:
    limit = len(lines) if body_start_idx is None else body_start_idx
    out = list(lines)
    for idx in range(limit):
        title = _canonical_title(out[idx])
        if title is not None:
            out[idx] = title
    return out


def _find_footer_start(lines: list[str], body_start_idx: int) -> int:
    for idx in range(body_start_idx, len(lines)):
        line = lines[idx]
        if _DATE_LINE_ONLY_RE.fullmatch(line) or "正本證明與原本無異" in line:
            return idx
    return len(lines)


def _find_footer_end(lines: list[str], footer_start_idx: int) -> int:
    """從 footer 起始往後找最後一個含「書記官」的行，傳回該行的下一個 index。
    若找不到，傳回 footer_start_idx（保留整個 footer，不截斷）。"""
    last_secretary = -1
    for idx in range(footer_start_idx, min(footer_start_idx + 40, len(lines))):
        if "書記官" in lines[idx]:
            last_secretary = idx
        # 已找到書記官後遇到日期行才停（避免掃到附錄法條內的「書記官」）
        # 若尚未找到書記官（如宜蘭地院「日期→如不服→日期→書記官」格式），繼續往下掃
        if idx > footer_start_idx and _DATE_LINE_ONLY_RE.fullmatch(lines[idx]):
            if last_secretary >= 0:
                break
    if last_secretary >= 0:
        return last_secretary + 1
    return footer_start_idx


# 圈號項目起首（㈠~㈩ 等），用於移除清單項目間的多餘空行
_CIRCLED_ITEM_START_RE = re.compile(
    r'^[㈠㈡㈢㈣㈤㈥㈦㈧㈨㈩①②③④⑤⑥⑦⑧⑨⑩⑴⑵⑶⑷⑸⑹⑺⑻⑼⑽⒈⒉⒊⒋⒌⒍⒎⒏⒐⒑]'
)

# footer 區「隔行」分隔符（這些行不與其他行合併）
_FOOTER_SPECIAL_RE = re.compile(r'正本(?:係照|係依|證明)|書記官|法\s*官|審判長')

_ZHUJIAN_SENT_END_RE = re.compile(r'。\s*$')

_BODY_SPACE_RE = re.compile(
    r'(?<=[\u4e00-\u9fff\d\u3000-\u303f\u2460-\u24ff\u3200-\u32ff])'
    r'\s+'
    r'(?=[\u4e00-\u9fff\d\u3000-\u303f\u2460-\u24ff\u3200-\u32ff])'
)


def _compress_body_spaces(lines: list[str], body_start_idx: int, footer_start_idx: int) -> list[str]:
    """正文區（body_start_idx ~ footer_start_idx）逐行壓縮 CJK/數字間的殘留空白。"""
    out = list(lines)
    for idx in range(body_start_idx, min(footer_start_idx, len(out))):
        out[idx] = _BODY_SPACE_RE.sub('', out[idx])
    return out


def _merge_footer_lines(lines: list[str], footer_start_idx: int, footer_end_idx: int) -> list[str]:
    """合併 footer 區的折行（如不服本判決/本裁定 等段落）。
    日期行、法官行、書記官行、正本行保持獨立；段落起首行（如不服/告訴人）
    開始一個新段落，其後的折行續接。"""
    if footer_end_idx <= footer_start_idx:
        return list(lines)
    out = list(lines)
    footer = [lines[i] for i in range(footer_start_idx, footer_end_idx)]
    merged: list[str] = []
    in_paragraph = False  # 是否在可續接的段落中
    for line in footer:
        stripped = line.strip()
        is_break = (
            not stripped
            or bool(_DATE_LINE_RE.search(line))
            or bool(_FOOTER_SPECIAL_RE.search(stripped))
        )
        is_para_start = stripped.startswith('如不服') or stripped.startswith('告訴人')
        if is_break:
            merged.append(line)
            in_paragraph = False
        elif is_para_start:
            merged.append(line)
            in_paragraph = True
        elif in_paragraph and merged:
            merged[-1] = merged[-1].rstrip(' \t\u3000') + line.lstrip(' \t\u3000')
        else:
            merged.append(line)
            in_paragraph = False
    out[footer_start_idx:footer_end_idx] = merged
    return out


def _merge_body_lines(lines: list[str], body_start_idx: int, footer_start_idx: int) -> list[str]:
    out = list(lines)
    if body_start_idx >= footer_start_idx:
        return out

    body_lines = [_normalize_body_line(line) for line in lines[body_start_idx:footer_start_idx]]
    merged: list[str] = []
    in_zhujian = False
    for line in body_lines:
        if not merged:
            merged.append(line)
            continue
        prev = merged[-1]
        norm_prev = _normalize_spaces(prev.strip())
        # 進入主文區後一律保留換行；遇到下一個大標題才離開
        if norm_prev == '主文':
            in_zhujian = True
        elif in_zhujian and _is_body_keep_line(prev):
            in_zhujian = False
        keep_break = (
            not prev.strip()
            or not line.strip()
            or _is_body_keep_line(prev)
            or _starts_new_body_block(line)
            or (in_zhujian and bool(_ZHUJIAN_SENT_END_RE.search(prev)))
        )
        if keep_break:
            merged.append(line)
        else:
            merged[-1] = prev.rstrip(' \t\u3000') + line.lstrip(' \t\u3000')

    # 後處理：移除緊接在圈號項目（㈠~㈩ 等）前的多餘空行（PDF 排版殘留）
    cleaned: list[str] = []
    for j, line in enumerate(merged):
        if not line.strip():
            nxt = next((merged[k] for k in range(j + 1, len(merged)) if merged[k].strip()), '')
            nxt_stripped = nxt.lstrip(' \t\u3000')
            if nxt_stripped and _CIRCLED_ITEM_START_RE.match(nxt_stripped):
                continue  # 跳過此空行
        cleaned.append(line)

    out[body_start_idx:footer_start_idx] = cleaned
    return out


def clean_judgment_text(full_text: str) -> str:
    """
    Args:
        full_text: JFULL 原始全文（含 \\r\\n 換行）

    Returns:
        clean_text：段落內換行合併、大標題清理後的文字
    """
    text = re.sub(r'\u3000+(\r\n)', r'\1', full_text)
    # 把「。　　理　由」等內嵌大標題前後補換行（某些 PDF 用 \u3000 代替 \r\n）
    text = _INLINE_HEADING_RE.sub(r'\1\r\n\2\r\n', text)
    lines, trailing_newline = _split_lines_preserve_trailing(text)

    body_start_idx = next(
        (idx for idx, line in enumerate(lines)
         if line.lstrip(' \t\u3000').startswith('上列')   # 只比對行首的「上列」
         or "上開當事人間" in line
         or line.lstrip().startswith("列當事人間")
         or "當事人間" in line                             # 補字 / 無「上列」格式
         or "裁定如下" in line or "判決如下" in line),
        None,
    )
    # 最後備援：若以上觸發條件都沒有（如 補/裁定 案件），
    # 從第 3 行起找第一個段落號行（一、/㈠/① 等）
    if body_start_idx is None:
        body_start_idx = next(
            (idx for idx, line in enumerate(lines)
             if idx >= 3 and _BODY_BLOCK_START_RE.match(line)),
            None,
        )
    lines = _normalize_header_lines(lines, body_start_idx)

    if body_start_idx is None:
        return _join_lines(lines, trailing_newline)

    footer_start_idx = _find_footer_start(lines, body_start_idx)
    lines = _merge_body_lines(lines, body_start_idx, footer_start_idx)
    footer_start_idx = _find_footer_start(lines, body_start_idx)  # recompute: merge changes line count
    footer_end_idx = _find_footer_end(lines, footer_start_idx)
    lines = lines[:footer_end_idx]  # 截斷書記官以下（附錄、附表、起訴書等）
    lines = _merge_footer_lines(lines, footer_start_idx, footer_end_idx)
    lines = _compress_body_spaces(lines, body_start_idx, footer_start_idx)

    return _join_lines(lines, trailing_newline)


# =========================
# 測試
# =========================
if __name__ == '__main__':
    # 用實際檔案驗證
    sample_path = Path('/Users/rachel/Downloads/202511/福建高等法院金門分院民事/KMHV,114,抗,10,20251111,1.json')
    if not sample_path.exists():
        print('找不到測試檔案')
        exit()

    with open(sample_path, encoding='utf-8') as f:
        data = json.load(f)

    full_text = data['JFULL']
    clean = clean_judgment_text(full_text)

    print('=== 原文前 400 字 ===')
    print(repr(full_text[:400]))
    print()
    print('=== clean_text 前 400 字 ===')
    print(repr(clean[:400]))
    print()

    # 確認 citation 在 clean_text 裡是否完整（無換行中斷）
    import re as _re
    citations = _re.findall(r'最高法院\S+?號', clean)
    print('=== clean_text 中找到的 citation（完整性確認）===')
    for c in citations:
        print(f'  {c}')