import { Link, useParams } from "react-router-dom";
import { highlightText } from "../lib/highlight";

// 單一引用來源的卡片：顯示來源判決的裁判字號、引用片段、涉及法條
// Props:
//   citation: { source_id, source_court, case_ref, snippet, statutes, doc_type, ... }
//   keywords: string[] — 用來在 snippet 裡 highlight 關鍵字
//   searchStatutes: StatuteFilter[] — 搜尋條件的法條，用來在 snippet 裡 highlight（淺綠色）
export default function SnippetCard({ citation, keywords, searchStatutes }) {
  const { lang = "en" } = useParams();
  const caseRef = citation.case_ref || `來源 #${citation.source_id}`;
  const params = new URLSearchParams();
  if (keywords.length) params.set("kw", keywords.join(","));
  if (citation.snippet) params.set("anchor", citation.snippet);
  const queryStr = params.toString() ? `?${params.toString()}` : "";

  return (
    <div className="border border-brand-border rounded-lg p-4 bg-white text-sm space-y-2">
      {/* 來源裁判字號（連結至判決詳情） + 法院 */}
      <div className="flex items-center gap-2 text-xs text-gray-500">
        <Link
          to={`/${lang}/decisions/${citation.source_id}${queryStr}`}
          className="font-medium text-brand hover:underline"
        >
          {caseRef}{citation.doc_type && citation.doc_type}
        </Link>
        {citation.source_court && (
          <span className="text-gray-400">· {citation.source_court}</span>
        )}
      </div>

      {/* 引用片段，關鍵字 highlight */}
      {citation.snippet && (
        <p className="text-gray-700 leading-relaxed">
          {highlightText(citation.snippet, keywords, searchStatutes)}
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
