// 把法條搜尋條件轉成 snippet 裡可能出現的文字片段
// { law: "民法", article: "242", sub_ref: "第1項" } → "民法第242條第1項"
// { law: "民法", article: "242", sub_ref: null }    → "民法第242條"
// { law: "民法", article: null,  sub_ref: null }    → "民法"
function statutesToTerms(statutes) {
  if (!statutes?.length) return [];
  const terms = [];
  for (const s of statutes) {
    if (s.law && s.article && s.sub_ref) {
      terms.push(`${s.law}第${s.article}條${s.sub_ref}`);
    } else if (s.law && s.article) {
      terms.push(`${s.law}第${s.article}條`);
    } else if (s.law) {
      terms.push(s.law);
    }
  }
  // 去重，長的優先
  return [...new Set(terms)].sort((a, b) => b.length - a.length);
}

function escapeRegex(s) {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

// 雙色 highlight：keyword 用橘色（bg-highlight），statute 用淺綠色（bg-highlight-statute）
// 同時命中時 keyword 優先
export function highlightText(text, keywords, statutes) {
  if (!text) return text;

  const kwTerms = (keywords || []).filter(Boolean);
  const stTerms = statutesToTerms(statutes);

  if (!kwTerms.length && !stTerms.length) return text;

  // 建立一個合併的 regex，用 named group 區分來源
  const parts = [];
  if (kwTerms.length) {
    parts.push(`(?<kw>${kwTerms.map(escapeRegex).join("|")})`);
  }
  if (stTerms.length) {
    parts.push(`(?<st>${stTerms.map(escapeRegex).join("|")})`);
  }
  const pattern = new RegExp(parts.join("|"), "gi");

  // split 不能用 named group，改用 matchAll 手動切片
  const result = [];
  let lastIndex = 0;

  for (const m of text.matchAll(pattern)) {
    // match 前面的純文字
    if (m.index > lastIndex) {
      result.push(text.slice(lastIndex, m.index));
    }
    const matched = m[0];
    const isKeyword = m.groups?.kw != null;
    const className = isKeyword
      ? "bg-highlight text-inherit rounded-sm px-0.5"
      : "bg-highlight-statute text-inherit rounded-sm px-0.5";
    result.push(
      <mark key={m.index} className={className}>
        {matched}
      </mark>
    );
    lastIndex = m.index + matched.length;
  }

  // 尾部純文字
  if (lastIndex < text.length) {
    result.push(text.slice(lastIndex));
  }

  return result.length ? result : text;
}
