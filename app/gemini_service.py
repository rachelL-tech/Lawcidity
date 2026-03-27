"""
Gemini 爭點/法條提取 + RAG 全文分析。

使用 google-genai SDK，呼叫 Gemini 2.5 Flash。
"""

import json
import os

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


# ── 爭點 / 法條提取 ──────────────────────────────────────────────────

EXTRACT_PROMPT = """\
你是台灣法律分析專家。根據以下案情事實，提取：

1. **爭點 (issues)**：列出此案的核心法律爭點，每個爭點用一句話描述。
2. **法條 (statutes)**：列出可能適用的法條，格式為 law（法律名稱）和 article（條號，純數字）。

案情事實：
{text}

請以 JSON 格式回覆，格式如下：
{{
  "issues": ["爭點1", "爭點2", ...],
  "statutes": [{{"law": "民法", "article": "184"}}, ...]
}}

注意：
- 爭點要精準、具體，與案情直接相關
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
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=EXTRACT_PROMPT.format(text=text),
        config={
            "temperature": 0.1,
            "response_mime_type": "application/json",
        },
    )
    return json.loads(response.text)


# ── RAG 全文分析（Gemini 生成） ───────────────────────────────────────

ANALYZE_PROMPT = """\
你是台灣資深律師，正在為當事人提供法律分析意見。

根據以下案情事實和檢索到的相關判決段落，撰寫一份結構化的法律分析。

## 案情事實
{query}

## 使用者確認的爭點
{issues}

## 使用者確認的法條
{statutes}

## 檢索到的相關判決段落
{chunks}

## 撰寫要求

請根據上述資料撰寫法律分析，必須符合以下格式：

1. 針對每個爭點，撰寫分析段落
2. 在分析中引用判決時，必須使用以下格式標記：
   - 引用高等/地方法院判決（citation_context 來源）：用 `<cite type="source" id="DECISION_ID">案號</cite>` 標記引用方判決，用 `<cite type="target" id="DECISION_ID">案號</cite>` 標記被引用的上級判決
   - 引用最高法院見解（supreme_reasoning 來源）：用 `<cite type="supreme" id="DECISION_ID">案號</cite>` 標記
3. 引用法條時用 `<statute law="法律名稱" article="條號">法律名稱第X條</statute>` 標記
4. 最後給出簡短結論

只回傳分析全文（含標記），不要加 JSON 包裝。
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
        query: 案情事實
        issues: 使用者確認的爭點
        statutes: 使用者確認的法條
        rag_results: RAG 搜尋結果（含 chunk text、decision info）

    Returns:
        帶有 citation 標記的分析全文
    """
    # 組裝 chunks 段落
    chunks_text = ""
    for i, r in enumerate(rag_results, 1):
        chunk_type = r.get("type", "unknown")
        display_title = r.get("display_title", "")
        root_norm = r.get("root_norm", "")
        decision_id = r.get("decision_id", "")
        best_chunk = r.get("best_chunk_text", "")

        targets_info = ""
        for t in r.get("targets", []):
            targets_info += f"\n    - 引用: {t.get('display_title', '')} (id={t.get('id', '')}, 被引用 {t.get('total_citation_count', 0)} 次)"

        chunks_text += f"""
### 段落 {i} [{chunk_type}]
- 來源判決: {root_norm} {display_title} (decision_id={decision_id})
- 類型: {chunk_type}{targets_info}
- 內容:
{best_chunk}

"""

    issues_text = "\n".join(f"- {issue}" for issue in issues) if issues else "（未指定）"
    statutes_text = "\n".join(
        f"- {s.get('law', '')} 第{s.get('article', '')}條" for s in statutes
    ) if statutes else "（未指定）"

    client = _get_client()
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=ANALYZE_PROMPT.format(
            query=query,
            issues=issues_text,
            statutes=statutes_text,
            chunks=chunks_text,
        ),
        config={
            "temperature": 0.3,
            "max_output_tokens": 4096,
        },
    )
    return response.text
