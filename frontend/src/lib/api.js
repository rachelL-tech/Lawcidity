const BASE = "/api/v1";

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
  return post("/search", {
    keywords: req.keywords,
    statutes: req.statutes,
    exclude_keywords: req.exclude_keywords,
    exclude_statutes: req.exclude_statutes,
    case_types: req.case_types,
    sort: "relevance",
    page: req.page,
    page_size: req.page_size,
  });
}

// 只重跑 target ranking（不重打第一階段 source 召回）
export function rerank(req) {
  return post("/search/rerank", {
    search_cache_key: req.search_cache_key,
    keywords: req.keywords,
    statutes: req.statutes,
    exclude_keywords: req.exclude_keywords,
    exclude_statutes: req.exclude_statutes,
    case_types: req.case_types,
    doc_types: req.doc_types,
    court_levels: req.court_levels,
    sort: req.sort,
    page: req.page,
    page_size: req.page_size,
  });
}

// 把 citations 查詢需要的參數轉成 query string，給展開 citations preview 用
function citationParams(keywords, statutes, excludeKeywords, excludeStatutes, caseTypes, searchCacheKey, rankedSourceIds) {
  const p = new URLSearchParams();
  if (keywords.length) p.set("keywords", keywords.join(",")); // keywords=a,b,c 
  if (statutes.length) p.set("statutes", JSON.stringify(statutes));
  if (excludeKeywords.length) p.set("exclude_keywords", excludeKeywords.join(","));
  if (excludeStatutes.length) p.set("exclude_statutes", JSON.stringify(excludeStatutes));
  if (caseTypes.length) p.set("case_types", caseTypes.join(","));
  if (searchCacheKey) p.set("search_cache_key", searchCacheKey);
  if (rankedSourceIds?.length) p.set("ranked_source_ids", rankedSourceIds.join(","));
  return p.toString(); // URLSearchParams 是物件，要轉成字串才能放在 URL 後面
}

// 打 GET /api/v1/{targetType}/{targetId}/citations?...
export function fetchCitations(targetType, targetId, keywords, statutes, excludeKeywords, excludeStatutes, caseTypes, searchCacheKey, rankedSourceIds = []) {
  const qs = citationParams(keywords, statutes, excludeKeywords, excludeStatutes, caseTypes, searchCacheKey, rankedSourceIds);
  return get(`/${targetType}/${targetId}/citations?${qs}`);
}

// 打 GET /api/v1/decisions/{id}
export function fetchDecision(id) {
  return get(`/decisions/${id}`);
}

// GET /api/v1/laws?q=...，給法律 autocomplete 用（後端已按字數小到大排序）
export function fetchLaws(q) {
  return get(`/laws?q=${encodeURIComponent(q)}`);
}

// POST /api/v1/analyze — Gemini 爭點/法條提取
export function analyze(text) {
  return post("/analyze", { text });
}

// POST /api/v1/analyze/generate — RAG + Gemini 全文分析
export function analyzeGenerate(req) {
  return post("/analyze/generate", req);
}
