"""
將判決原始全文（JFULL）清理成適合前端顯示的 clean_text

清理規則（套用順序重要）：
1. 兩字大標題：\u3000\u3000主\u3000文 → 主文
2. 行尾殘留全形空白：「。\u3000\u3000\r\n」→「。\r\n」（讓規則 3 lookbehind 正確判斷）
3. 段落內接行合併：\r\n + 縮排空白 → 移除（PDF 行內折行）
   - Negative lookbehind：前一字是句末標點（。！？；：）時不合併，保留大標題前的 \r\n
4. 大標題殘留前置全形空白：\r\n\u3000\u3000事實及理由 → \r\n事實及理由
"""
import re
import json
from pathlib import Path


def clean_judgment_text(full_text: str) -> str:
    """
    Args:
        full_text: JFULL 原始全文（含 \\r\\n 換行）

    Returns:
        clean_text：段落內換行合併、大標題清理後的文字
    """
    text = full_text

    # 規則 1：兩字大標題全形空白清理
    # \u3000\u3000主\u3000文 → 主文 / \u3000\u3000理\u3000由 → 理由
    text = re.sub(r'\u3000{2}(\S)\u3000(\S)', r'\1\2', text)

    # 規則 2：行尾殘留全形空白（PDF 對齊填充）
    # 「。\u3000\u3000\r\n」→「。\r\n」
    # 目的：讓規則 3 的 lookbehind 能看到正確的句末標點
    text = re.sub(r'\u3000+(\r\n)', r'\1', text)

    # 規則 3：段落內接行合併
    # \r\n 後面接縮排（半形空白、tab、全形空白）→ 直接移除，文字接上一行
    # Negative lookbehind：前一字是句末標點時不合併（保留大標題前的 \r\n）
    text = re.sub(r'(?<![。！？；：])\r\n[ \t\u3000]+', '', text)

    # 規則 4：清除大標題殘留的前置全形空白
    # \r\n\u3000\u3000事實及理由 → \r\n事實及理由
    # （規則 1 只處理「X\u3000Y」型兩字標題；多字標題無內部全形空白，規則 1 不觸及）
    text = re.sub(r'\r\n\u3000+', '\r\n', text)

    return text


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
