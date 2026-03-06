export function highlightText(text, keywords) {
  if (!keywords.length || !text) return text;
  // 先把關鍵字裡的 regex 特殊字元跳脫（例如 +, ?, (）），避免使用者輸入特殊字元把正則搞壞
  const escaped = keywords.map((k) =>
    k.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"),
  );
  // 把多個關鍵字組成一個 regex：(kw1|kw2|kw3)，加上 "g" flag 代表全域搜尋、"i" flag 代表忽略大小寫
  const pattern = new RegExp(`(${escaped.join("|")})`, "gi");
  // 用 regex 把文字切成陣列，關鍵字會被保留在陣列裡，然後 map 出 React 元素，關鍵字部分套上 <mark> 標籤
  const parts = text.split(pattern);
  return parts.map((part, i) =>
    pattern.test(part) ? ( // regex.test() 會檢查這段文字是否符合關鍵字，如果是就套上 <mark> 標籤
      <mark key={i} className="bg-highlight text-inherit rounded-sm px-0.5">
        {part}
      </mark>
    ) : (
      part // 如果不是關鍵字就直接回傳原文字
    ),
  );
}
