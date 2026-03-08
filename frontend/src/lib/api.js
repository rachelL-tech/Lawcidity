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
  return post("/search", req);
}

// 把 citations 查詢需要的參數轉成 query string，給 fetchMatchedCitations 和 fetchOtherCitations 用
function citationParams(keywords, statutes) {
  const p = new URLSearchParams();
  if (keywords.length) p.set("keywords", keywords.join(",")); // keywords=a,b,c 
  if (statutes.length) p.set("statutes", JSON.stringify(statutes));
  return p.toString(); // URLSearchParams 是物件，要轉成字串才能放在 URL 後面
}

// 打 GET /api/v1/{targetType}/{targetId}/citations/matched?...
export function fetchMatchedCitations(targetType, targetId, keywords, statutes) {
  const qs = citationParams(keywords, statutes);
  return get(`/${targetType}/${targetId}/citations/matched?${qs}`);
}

// 打 GET /api/v1/{targetType}/{targetId}/citations/others?...
export function fetchOtherCitations(targetType, targetId, keywords, statutes) {
  const qs = citationParams(keywords, statutes);
  return get(`/${targetType}/${targetId}/citations/others?${qs}`);
}

// 打 GET /api/v1/decisions/{id}
export function fetchDecision(id) {
  return get(`/decisions/${id}`);
}

// GET /api/v1/laws?q=...，給法律 autocomplete 用（後端已按字數小到大排序）
export function fetchLaws(q) {
  return get(`/laws?q=${encodeURIComponent(q)}`);
}
