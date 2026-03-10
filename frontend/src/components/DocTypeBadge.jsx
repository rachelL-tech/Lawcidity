const DOC_TYPE_COLORS = {
  "判決":       "bg-brand text-white",             // 橘紅（主色）
  "裁定":       "bg-emerald-500 text-white",       // 綠色
  "判例":       "bg-orange-700 text-white",        // 深橘
  "憲判字":     "bg-blue-600 text-white",          // 藍色
  "決議":       "bg-purple-500 text-white",         // 紫色
  "釋字":       "bg-blue-800 text-white",          // 深藍
  "法律座談會": "bg-cyan-500 text-white",           // 青色
  "研審小組意見": "bg-sky-300 text-gray-800",       // 淺藍
  "聯席會議決議": "bg-gray-400 text-white",         // 灰色
};

const DEFAULT_COLOR = "bg-gray-400 text-white";

export default function DocTypeBadge({ docType }) {
  if (!docType) return null;
  const color = DOC_TYPE_COLORS[docType] || DEFAULT_COLOR;
  return (
    <span className={`inline-block px-2 py-0.5 text-xs font-medium rounded ${color}`}>
      {docType}
    </span>
  );
}
