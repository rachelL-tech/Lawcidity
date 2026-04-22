const BASE = "/api";

// 共用 POST 函式
async function post(path, body) {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    // 丟出 Error 讓頁面層 catch
    // err.detail 可能是字串或陣列（FastAPI validation error），統一轉成字串
    const detail = err.detail;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail) || res.statusText);
  }
  // 成功就回傳 res.json()
  return res.json();
}

// 共用 GET 函式
async function get(path) {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    // err.detail 可能是字串或陣列（FastAPI validation error），統一轉成字串
    const detail = err.detail;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail) || res.statusText);
  }
  return res.json();
}

// 打搜尋 API，req 是搜尋條件物件
export function search(req) {
  return post("/search", req);
}

// 只重跑 target ranking（不重打第一階段 source 召回）
export function rerank(req) {
  return post("/search/rerank", req);
}

// 把 citations 查詢需要的參數轉成 query string，給展開 citations preview 用
function citationParams(keywords, statutes, excludeKeywords, excludeStatutes, caseTypes, cacheId, previewSourceIds) {
  const p = new URLSearchParams();
  if (keywords.length) p.set("keywords", keywords.join(",")); // keywords=a,b,c 
  if (statutes.length) p.set("statutes", JSON.stringify(statutes));
  if (excludeKeywords.length) p.set("exclude_keywords", excludeKeywords.join(","));
  if (excludeStatutes.length) p.set("exclude_statutes", JSON.stringify(excludeStatutes));
  if (caseTypes.length) p.set("case_types", caseTypes.join(","));
  if (cacheId) p.set("search_cache_key", cacheId);
  if (previewSourceIds?.length) p.set("preview_source_ids", previewSourceIds.join(","));
  return p.toString(); // URLSearchParams 是物件，要轉成字串才能放在 URL 後面
}

// 打 GET /api/{targetType}/{targetId}/citations?...
export function fetchCitations(targetType, targetId, keywords, statutes, excludeKeywords, excludeStatutes, caseTypes, cacheId, previewSourceIds = []) {
  const qs = citationParams(keywords, statutes, excludeKeywords, excludeStatutes, caseTypes, cacheId, previewSourceIds);
  return get(`/${targetType}/${targetId}/citations?${qs}`);
}

// 打 GET /api/decisions/{id}
export function fetchDecision(id) {
  return get(`/decisions/${id}`);
}

// GET /api/laws?q=...，給法律 autocomplete 用（後端已按字數小到大排序）
export function fetchLaws(q) {
  return get(`/laws?q=${encodeURIComponent(q)}`);
}

// POST /api/analyze — Gemini 爭點/法條提取
export function analyze(text) {
  return post("/analyze", { text });
}

// POST /api/analyze/generate — RAG + Gemini 全文分析
export function analyzeGenerate(req) {
  return post("/analyze/generate", req);
}
