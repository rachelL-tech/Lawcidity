import { useState, useEffect } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { fetchDecision } from "../lib/api";
import { parseDecisionSections } from "../lib/decisionSections";
import { highlightText } from "../lib/highlight";
import DocTypeBadge from "../components/DocTypeBadge";

export default function DecisionPage() {
  const { id } = useParams();
  const [urlParams] = useSearchParams();
  const keywords = urlParams.get("kw")?.split(",").filter(Boolean) || [];
  const anchor = (urlParams.get("anchor") || "").replace(/\r\n/g, "\n").replace(/\r/g, "\n").trim();

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

  const highlightTerms = [anchor, ...keywords].filter(Boolean);
  const sections = parseDecisionSections(decision.clean_text || "");

  return (
    <main className="max-w-4xl mx-auto px-4 py-6 space-y-6">
      {/* AI 摘要 placeholder */}
      <div className="bg-white rounded-2xl p-6 shadow-sm">
        <h2 className="text-brand font-bold mb-3">AI 摘要</h2>
        <p className="text-sm text-gray-400">即將推出</p>
      </div>

      {/* 跳至引用段落（使用者從搜尋結果點進來時，跳到 snippet 高亮位置） */}
      {highlightTerms.length > 0 && (
        <div className="text-center">
          <button
            onClick={() => {
              const firstMark = document.querySelector("mark");
              if (firstMark) firstMark.scrollIntoView({ behavior: "smooth", block: "center" });
            }}
            className="px-6 py-2 bg-gray-800 text-white rounded-full text-sm"
          >
            跳至引用段落
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
            {highlightText(sections.header, highlightTerms)}
          </div>
        )}

        {/* 主文 */}
        {sections.main && (
          <div className="mb-6">
            <h3 className="text-brand font-bold text-lg mb-2">主文</h3>
            <blockquote className="border-l-4 border-brand pl-4 text-sm leading-7 whitespace-pre-wrap bg-brand-light rounded-r-lg py-3">
              {highlightText(sections.main, highlightTerms)}
            </blockquote>
          </div>
        )}

        {/* 事實（有些刑事判決事實/理由為獨立段落） */}
        {sections.fact && (
          <div id="section-fact" className="mb-6">
            <h3 className="text-brand font-bold text-lg mb-2">
              {sections.factLabel || "事實"}
            </h3>
            <div className="text-sm leading-7 whitespace-pre-wrap">
              {highlightText(sections.fact, highlightTerms)}
            </div>
          </div>
        )}

        {/* 理由（或事實及理由） */}
        {sections.reason && (
          <div id="section-reason" className="mb-6">
            <h3 className="text-brand font-bold text-lg mb-2">
              {sections.reasonLabel || "理由"}
            </h3>
            <div className="text-sm leading-7 whitespace-pre-wrap">
              {highlightText(sections.reason, highlightTerms)}
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
