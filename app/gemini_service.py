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
    response = client.models.generate_content(
        model="gemini-3.1-flash-lite-preview",
        contents=EXTRACT_PROMPT.format(text=text),
        config={
            "temperature": 0.1,
            "response_mime_type": "application/json",
        },
    )
    return json.loads(response.text)


# ── RAG 全文分析（Gemini 生成） ───────────────────────────────────────

ANALYZE_PROMPT = """\
你是台灣法律判決分析引擎。根據下方案例與判決資料，針對每個爭點輸出分析。

## 輸出格式（嚴格遵守）

- 直接從第一個爭點開始，**禁止**輸出前言、案例概述、結語、建議、敬語或任何非爭點分析的內容
- 每個爭點以 `<h3>爭點 N：爭點標題</h3>` 開頭，N 從 1 起
- 引用法條：`<statute law="民法" article="184">民法第184條</statute>`

## 引用判決的規則

每個段落有 `[source: ...]`、`[targets: ...]` 等標記行，這些是**內部指令，絕對禁止出現在輸出中**。

- 引用來源判決：`<cite type="source" id="DECISION_ID">案號</cite>`（DECISION_ID 取自 `[source: ...]` 行的 decision_id）
- 若段落中提到 targets 列表內的判決，在 source 見解後括號標記：`（參照<cite type="target" id="ID">案號</cite>）`（ID 取自 `[targets: ...]` 內對應判決的 id）
- 範例：`<cite type="source" id="100">地院114年訴字第374號</cite>認為...（參照<cite type="target" id="200">最高法院88年台上字第5678號</cite>）`

## 案例事實
{query}

## 確認的爭點
{issues}

## 確認的法條
{statutes}

## 相關判決段落
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
    chunks_text = ""
    for i, r in enumerate(rag_results, 1):
        display_title = r.get("display_title", "")
        root_norm = r.get("root_norm", "")
        decision_id = r.get("decision_id", "")
        best_chunk = r.get("best_chunk_text", "")

        targets_lines = "\n".join(
            f"  - {t.get('root_norm', '')} {t.get('display_title', '')}, id={t.get('id', '')}"
            for t in r.get("targets", [])
        )
        targets_block = f"\n[targets:\n{targets_lines}\n]" if targets_lines else ""
        header = (
            f"[source: {root_norm} {display_title}, decision_id={decision_id}]"
            f"{targets_block}"
        )

        chunks_text += f"""
### 段落 {i}
{header}
內容：
{best_chunk}

"""

    issues_text = "\n".join(f"- {issue}" for issue in issues) if issues else "（未指定）"
    statutes_text = "\n".join(
        f"- {s.get('law', '')} 第{s.get('article', '')}條" for s in statutes
    ) if statutes else "（未指定）"

    client = _get_client()
    response = client.models.generate_content(
        model="gemini-3.1-flash-lite-preview",
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
    return response.text
