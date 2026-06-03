"""
Gemini 爭點/法條提取 + RAG 全文分析。

使用 google-genai SDK，呼叫 Gemini 2.5 Flash。
"""

import json
import os
import time

from google import genai

_client = None


def _get_client():
    global _client
    if _client is None:
        api_key = os.environ.get("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY 未設定")
        _client = genai.Client(api_key=api_key)
    return _client


def _generate_content_with_retry(client, *, error_message: str, **kwargs):
    for attempt in range(2):
        try:
            return client.models.generate_content(**kwargs)
        except Exception as exc:
            if attempt == 1:
                raise RuntimeError(error_message) from exc
            time.sleep(0.5)

    raise RuntimeError(error_message)


# ── 爭點 / 法條提取 ──────────────────────────────────────────────────

EXTRACT_PROMPT = """\
你是台灣法律分析專家。根據以下案例事實，提取：

1. **爭點 (issues)**：列出此案的核心法律爭點，每個爭點用一句話描述。
2. **法條 (statutes)**：列出可能適用的法條，格式為 law（法律名稱）和 article（條號，純數字）。

案例事實：
{text}

請以 JSON 格式回覆，格式如下：
{{
  "issues": ["爭點1", "爭點2", ...],
  "statutes": [{{"law": "民法", "article": "184"}}, ...]
}}

注意：
- 若案例事實非繁體中文，請先在內部翻譯成繁體中文再提取
- issues 與 statutes 一律以**繁體中文**輸出（法律語料為台灣法，需繁中才能正確檢索）
- 爭點要精準、具體，與案例直接相關
- 法條只列最核心的，不要列太多
- article 只填條號數字（如 "184"），不要包含「第」「條」等文字
- 只回傳 JSON，不要加任何其他文字
"""


def extract_issues_and_statutes(text: str) -> dict:
    """
    呼叫 Gemini 提取爭點和法條。

    Returns:
        {"issues": [...], "statutes": [{"law": ..., "article": ...}, ...]}
    """
    client = _get_client()
    response = _generate_content_with_retry(
        client,
        error_message="Gemini 分析 API 呼叫失敗",
        model="gemini-3.1-flash-lite",
        contents=EXTRACT_PROMPT.format(text=text),
        config={
            "temperature": 0.1,
            "response_mime_type": "application/json",
        },
    )

    response_text = (response.text or "").strip()
    if not response_text:
        raise RuntimeError("Gemini 分析回應為空")

    try:
        return json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Gemini 分析回應不是合法 JSON") from exc


# ── RAG 全文分析（Gemini 生成） ───────────────────────────────────────

ANALYZE_PROMPT = """\
你是台灣法律判決分析引擎。根據下方案例事實、確認的爭點、確認的法條與各爭點相關判決段落，針對每個爭點輸出分析。

## 輸出格式

- 直接從第一個爭點開始，**禁止**輸出前言、結語、建議或任何非爭點分析的內容
- 每個爭點以 `<h3>爭點 N：爭點標題</h3>` 開頭，N 從 1 起，標題照抄「確認的爭點」原文
- 引用法條：`<statute law="民法" article="184">民法第184條</statute>`
- `cite` 與 `statute` 的 tag、attribute 名稱、順序、雙引號必須與範例完全一致

## 段落結構與引用規則

下方「各爭點的判決段落」已按爭點分組，分析爭點 N 時主要依據它底下的段落。

每個段落有：
- 一行 `[source decision_id=X]`：這段論述「出自」的判決（召回的判決本身）→ 引用用 `<cite type="source" id="X">案號</cite>`
- 零到多行 `[target id=Y]`：該判決「引用的上級權威」（最高法院判例／決議）→ 引用用 `（參照<cite type="target" id="Y">案號</cite>）`

`[source ...]`、`[target ...]` 這類**方括號標記**只是內部指令、用來告訴你 id，**禁止出現在輸出**；輸出只能用 `<cite>`、`<statute>` 這類 tag。

範例：`<cite type="source" id="100">地院114年訴字第374號</cite>認為...（參照<cite type="target" id="200">最高法院88年台上字第5678號</cite>）`

## 誠實原則（法律工具底線）

- 只能根據提供的段落分析，不得編造案號或引用段落以外的判決
- 若某爭點只找到單一見解，據實呈現，不要捏造對立面

## 案例事實
{query}

## 確認的爭點
{issues}

## 確認的法條
{statutes}

## 各爭點的判決段落
{chunks}
"""


def generate_analysis(
    query: str,
    issues: list[str],
    statutes: list[dict],
    rag_results: list[dict],
) -> str:
    """
    呼叫 Gemini 生成法律分析全文。

    Args:
        query: 案例事實
        issues: 使用者確認的爭點
        statutes: 使用者確認的法條
        rag_results: RAG 搜尋結果（含 chunk text、decision info）

    Returns:
        帶有 citation 標記的分析全文
    """
    chunk_blocks: list[str] = []
    for i, (issue, chunks) in enumerate(rag_results.items(), 1):
        chunk_blocks.append(f"## 爭點 {i}：{issue}")
        for j, r in enumerate(chunks, 1):
            display_title = r.get("display_title", "")
            root_norm = r.get("root_norm", "")
            decision_id = r.get("decision_id", "")
            best_chunk = r.get("best_chunk_text", "")

            source_line = f"[source decision_id={decision_id}] {root_norm} {display_title}"
            target_lines = [
                f"[target id={t.get('id', '')}] {t.get('root_norm', '')} {t.get('display_title', '')}"
                for t in r.get("targets", [])
            ]

            chunk_lines = [
                f"### 段落 {j}",
                source_line,
                *target_lines,
                "內容：",
                best_chunk,
            ]
            chunk_blocks.append("\n".join(chunk_lines))

    chunks_text = "\n\n".join(chunk_blocks)

    issues_text = "\n".join(f"- {issue}" for issue in issues) if issues else "（未指定）"
    statutes_text = "\n".join(
        f"- {s.get('law', '')} 第{s.get('article', '')}條" for s in statutes
    ) if statutes else "（未指定）"

    client = _get_client()
    response = _generate_content_with_retry(
        client,
        error_message="Gemini 生成 API 呼叫失敗",
        model="gemini-3.1-flash-lite",
        contents=ANALYZE_PROMPT.format(
            query=query,
            issues=issues_text,
            statutes=statutes_text,
            chunks=chunks_text,
        ),
        config={
            "temperature": 0.3,
            "max_output_tokens": 8192,
        },
    )

    response_text = (response.text or "").strip()
    if not response_text:
        raise RuntimeError("Gemini 生成回應為空")

    return response_text
