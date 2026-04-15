// URL query params <-> SearchRequest 物件 轉換：
// 1. searchRequestToParams(req)：讓搜尋狀態能放在 URL 上，重新整理、分享連結、上一頁/下一頁時都能還原同一組搜尋條件
// 2. paramsToSearchRequest(params)：讓 search(searchReq) 直接吃標準化物件，不用每頁重複 parse query

// 把法條陣列壓成字串
function encodeStatutes(statutes) {
  return statutes
    .map((s) => `${s.law}|${s.article || ""}|${s.sub_ref || ""}`)
    .join(","); // law|article|sub_ref,law|article|sub_ref,...
}

// 把上面字串還原回陣列物件
function decodeStatutes(raw) {
  if (!raw) return []; // "" 轉成 []
  return raw.split(",").map((chunk) => {
    const [law, article, sub_ref] = chunk.split("|");
    return { law, article: article || null, sub_ref: sub_ref || null };
  });
}

// 從 URL 讀 query params，還原成 API 要的 request 物件
export function paramsToSearchRequest(params) {
  return {
    keywords: params.get("kw")?.split(",").filter(Boolean) || [],
    statutes: decodeStatutes(params.get("st") || ""),
    exclude_keywords: params.get("xkw")?.split(",").filter(Boolean) || [],
    exclude_statutes: decodeStatutes(params.get("xst") || ""),
    case_types: params.get("ct")?.split(",").filter(Boolean) || [],
    doc_types: params.get("dt")?.split(",").filter(Boolean) || [],
    court_levels: params.get("cl")?.split(",").filter(Boolean).map(Number) || [],
    sort: params.get("sort") || "relevance",
    page: Number(params.get("page")) || 1,
    page_size: 20,
  };
}

// 萃取「搜尋母體」的條件指紋；只要這組值不變，就可重用既有 source recall cache
export function buildSearchSignature(params) {
  const keys = ["kw", "st", "xkw", "xst", "ct"];
  return keys.map((k) => params.get(k) || "").join("|");
}

// 把 request 物件轉成 URL query 字串
export function searchRequestToParams(req) {
  const p = new URLSearchParams();
  if (req.keywords.length) p.set("kw", req.keywords.join(","));
  if (req.statutes.length) p.set("st", encodeStatutes(req.statutes));
  if (req.exclude_keywords.length) p.set("xkw", req.exclude_keywords.join(","));
  if (req.exclude_statutes.length) p.set("xst", encodeStatutes(req.exclude_statutes));
  if (req.case_types.length) p.set("ct", req.case_types.join(","));
  if (req.doc_types?.length) p.set("dt", req.doc_types.join(","));
  if (req.court_levels?.length) p.set("cl", req.court_levels.join(","));
  if (req.sort !== "relevance") p.set("sort", req.sort);
  if (req.page > 1) p.set("page", String(req.page));
  return p.toString();
}
