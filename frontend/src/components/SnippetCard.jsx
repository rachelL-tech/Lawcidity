import { highlightText } from "../lib/highlight";

// 單一引用來源的卡片：顯示來源判決的裁判字號、引用片段、涉及法條
// Props:
//   citation: {
//     source_id, source_court, jyear, jcase_norm, jno,
//     snippet,                    — 引用片段原文
//     statutes: [{law, article, sub}]  — 此引用涉及的法條
//   }
//   keywords: string[] — 用來在 snippet 裡 highlight 關鍵字
export default function SnippetCard({ citation, keywords }) {
  const caseRef = citation.case_ref || `來源 #${citation.source_id}`;

  return (
    <div className="border border-brand-border rounded-lg p-3 bg-white text-sm space-y-2">
      {/* 來源裁判字號 + 法院 */}
      <div className="flex items-center gap-2 text-xs text-gray-500">
        <span className="font-medium text-gray-700">{caseRef}</span>
        {citation.source_court && (
          <span className="text-gray-400">· {citation.source_court}</span>
        )}
      </div>

      {/* 引用片段，關鍵字 highlight */}
      {citation.snippet && (
        <p className="text-gray-700 leading-relaxed">
          {highlightText(citation.snippet, keywords)}
        </p>
      )}

      {/* 此引用涉及的法條 chip */}
      {citation.statutes?.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {citation.statutes.map((s, i) => {
            const label = [s.law, s.article && `第${s.article}條`, s.sub_ref]
              .filter(Boolean)
              .join(" ");
            return (
              <span
                key={i}
                className="inline-block px-2 py-0.5 text-xs rounded bg-brand-light text-brand border border-brand-border"
              >
                {label}
              </span>
            );
          })}
        </div>
      )}
    </div>
  );
}
