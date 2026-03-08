import { useState, useEffect } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { fetchDecision } from "../lib/api";
import { highlightText } from "../lib/highlight";
import DocTypeBadge from "../components/DocTypeBadge";

function parseDecisionSections(text) {
  const mainIdx = text.indexOf("主　　文") !== -1
    ? text.indexOf("主　　文")
    : text.indexOf("主文");
  const reasonIdx = text.indexOf("理　　由") !== -1
    ? text.indexOf("理　　由")
    : text.indexOf("理由");

  if (mainIdx === -1 && reasonIdx === -1) {
    return { header: text, main: "", reason: "" };
  }

  const header = mainIdx !== -1 ? text.slice(0, mainIdx).trim() : text.slice(0, reasonIdx).trim();

  let main = "";
  if (mainIdx !== -1) {
    const mainStart = text.indexOf("\n", mainIdx);
    const mainEnd = reasonIdx !== -1 ? reasonIdx : text.length;
    main = text.slice(mainStart !== -1 ? mainStart : mainIdx + 2, mainEnd).trim();
  }

  let reason = "";
  if (reasonIdx !== -1) {
    const reasonStart = text.indexOf("\n", reasonIdx);
    reason = text.slice(reasonStart !== -1 ? reasonStart : reasonIdx + 2).trim();
  }

  return { header, main, reason };
}

export default function DecisionPage() {
  const { id } = useParams();
  const [urlParams] = useSearchParams();
  const keywords = urlParams.get("kw")?.split(",").filter(Boolean) || [];

  const [decision, setDecision] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (!id) return;
    setLoading(true);
    fetchDecision(Number(id))
      .then(setDecision)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) return <div className="p-8 text-center text-gray-400">載入中…</div>;
  if (error) return <div className="p-8 text-center text-red-500">錯誤：{error}</div>;
  if (!decision) return null;

  const sections = parseDecisionSections(decision.clean_text || "");

  return (
    <main className="max-w-4xl mx-auto px-4 py-6 space-y-6">
      {/* AI 摘要 placeholder */}
      <div className="bg-white rounded-2xl p-6 shadow-sm">
        <h2 className="text-brand font-bold mb-3">AI 摘要</h2>
        <p className="text-sm text-gray-400">即將推出</p>
      </div>

      {/* 跳至關鍵段落 */}
      {sections.reason && (
        <div className="text-center">
          <button
            onClick={() => document.getElementById("section-reason")?.scrollIntoView({ behavior: "smooth" })}
            className="px-6 py-2 bg-gray-800 text-white rounded-full text-sm"
          >
            跳至理由段落
          </button>
        </div>
      )}

      {/* 判決全文 */}
      <div className="bg-white rounded-2xl p-6 shadow-sm">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-bold">判決全文</h2>
          <span className="text-sm text-brand">
            被引用 {decision.total_citation_count} 次
          </span>
        </div>

        {/* Metadata */}
        <div className="flex items-center gap-4 text-sm text-gray-600 mb-6 pb-4 border-b">
          <DocTypeBadge docType={decision.doc_type} />
          <span>{decision.court}</span>
          <span>{decision.case_ref}</span>
          {decision.decision_date && <span>{decision.decision_date}</span>}
        </div>

        {/* Header / 前文 */}
        {sections.header && (
          <div className="mb-6 text-sm leading-7 whitespace-pre-wrap">
            {highlightText(sections.header, keywords)}
          </div>
        )}

        {/* 主文 */}
        {sections.main && (
          <div className="mb-6">
            <h3 className="text-brand font-bold text-lg mb-2">主文</h3>
            <blockquote className="border-l-4 border-brand pl-4 text-sm leading-7 whitespace-pre-wrap bg-brand-light rounded-r-lg py-3">
              {highlightText(sections.main, keywords)}
            </blockquote>
          </div>
        )}

        {/* 理由 */}
        {sections.reason && (
          <div id="section-reason" className="mb-6">
            <h3 className="text-brand font-bold text-lg mb-2">理由</h3>
            <div className="text-sm leading-7 whitespace-pre-wrap">
              {highlightText(sections.reason, keywords)}
            </div>
          </div>
        )}
      </div>

      {/* 法條列表 */}
      {decision.statutes.length > 0 && (
        <div className="bg-white rounded-2xl p-6 shadow-sm">
          <h2 className="font-bold mb-3">此判決提到的法條</h2>
          <div className="flex flex-wrap gap-2">
            {decision.statutes.map((s, i) => (
              <span
                key={i}
                className="px-3 py-1 rounded-full bg-brand-light text-sm border border-brand-border"
              >
                {s.law}第{s.article}條{s.sub_ref && s.sub_ref}
                <span className="text-gray-400 ml-1">x{s.count}</span>
              </span>
            ))}
          </div>
        </div>
      )}
    </main>
  );
}
