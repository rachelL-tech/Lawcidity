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
// 把「搜尋狀態」放在 URL，重新整理、分享連結、上一頁/下一頁時都能還原同一組搜尋條件，讓 search(searchReq) 直接吃標準化物件，不用每頁重複 parse query
export function paramsToSearchRequest(params) {
  return {
    keywords: params.get("kw")?.split(",").filter(Boolean) || [],
    statutes: decodeStatutes(params.get("st") || ""),
    exclude_keywords: params.get("xkw")?.split(",").filter(Boolean) || [],
    exclude_statutes: decodeStatutes(params.get("xst") || ""),
    case_types: params.get("ct")?.split(",").filter(Boolean) || [],
    sort: params.get("sort") || "relevance",
    page: Number(params.get("page")) || 1,
    page_size: 20,
  };
}

// 把 request 物件轉成 URL query 字串
// 使用者改排序、分頁、篩選後，要把新狀態寫回網址，讓分享連結、上一頁/下一頁都能保留最新狀態
export function searchRequestToParams(req) {
  const p = new URLSearchParams();
  if (req.keywords.length) p.set("kw", req.keywords.join(","));
  if (req.statutes.length) p.set("st", encodeStatutes(req.statutes));
  if (req.exclude_keywords.length) p.set("xkw", req.exclude_keywords.join(","));
  if (req.exclude_statutes.length) p.set("xst", encodeStatutes(req.exclude_statutes));
  if (req.case_types.length) p.set("ct", req.case_types.join(","));
  if (req.sort !== "relevance") p.set("sort", req.sort);
  if (req.page > 1) p.set("page", String(req.page));
  return p.toString();
}
