// 用來顯示像「判決」、「裁定」這種小標籤

export default function DocTypeBadge({ docType }) {
  // 如果 docType 沒值（例如 undefined、null、空字串），就不要顯示任何東西（return null 在 React 裡代表「這個元件不渲染」）
  if (!docType) return null;
  return (
    <span className="inline-block px-2 py-0.5 text-xs font-medium text-white bg-brand rounded">
      {docType}
    </span>
  );
}
