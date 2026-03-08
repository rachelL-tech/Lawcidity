import { useState } from "react";
import DocTypeBadge from "./DocTypeBadge";
import SnippetCard from "./SnippetCard";
import { fetchMatchedCitations, fetchOtherCitations } from "../lib/api";

// 搜尋結果卡片：顯示一筆目標判決的摘要，點擊展開引用來源列表
// Props:
//   item: SearchResultItem（從 /search API 回傳）
//   keywords: string[] — 傳給 SnippetCard 做 highlight
//   statutes: StatuteFilter[] — 傳給 citations API 篩選 matched
//   rank: number — 排名序號（從 1 開始）
export default function ResultCard({ item, keywords, statutes, rank }) {
  const [expanded, setExpanded] = useState(false);
  const [matched, setMatched] = useState(null); // null = 尚未載入
  const [others, setOthers] = useState(null);
  const [loadingCitations, setLoadingCitations] = useState(false);

  // 依 target 類型決定 endpoint
  const targetType = item.target_id != null ? "decisions" : "authorities";
  const targetId = item.target_id ?? item.authority_id;

  // 展開時才打 citations API（lazy load）
  async function handleExpand() {
    if (expanded) {
      setExpanded(false);
      return;
    }
    setExpanded(true);
    if (matched !== null) return; // 已載入過，不重打
    setLoadingCitations(true);
    try {
      const [m, o] = await Promise.all([
        fetchMatchedCitations(targetType, targetId, keywords, statutes),
        fetchOtherCitations(targetType, targetId, keywords, statutes),
      ]);
      // API 回傳 CitationsResponse { target, total, sources: [...] }，取 sources 陣列
      setMatched(m.sources ?? []);
      setOthers(o.sources ?? []);
    } finally {
      setLoadingCitations(false);
    }
  }

  return (
    <div className="bg-white rounded-xl border border-brand-border shadow-sm overflow-hidden">
      {/* 卡片標題列 — 整塊可點擊展開/收合 */}
      <div className="p-4 cursor-pointer select-none hover:bg-gray-50 transition-colors" onClick={handleExpand}>
        <div className="flex items-start gap-3">
          {/* 排名序號 */}
          <span className="text-2xl font-bold text-brand-border w-8 text-center flex-shrink-0">
            {rank}
          </span>

          <div className="flex-1 min-w-0">
            {/* 裁判字號 + 文書類型 + 法院 */}
            <div className="flex items-center gap-2 flex-wrap mb-1">
              <span className="font-semibold text-gray-800 text-sm">
                {item.case_ref || `判決 #${item.target_id}`}
              </span>
              <DocTypeBadge docType={item.doc_type} />
              {item.court && (
                <span className="text-xs text-gray-400">{item.court}</span>
              )}
            </div>

            {/* 引用數統計 */}
            <div className="flex items-center gap-4 text-xs text-gray-500">
              <span>
                符合引用{" "}
                <span className="font-semibold text-brand">
                  {item.matched_citation_count}
                </span>{" "}
                次
              </span>
              <span>
                總引用{" "}
                <span className="font-semibold text-gray-700">
                  {item.total_citation_count}
                </span>{" "}
                次
              </span>
            </div>
          </div>

          {/* 展開/收合指示 */}
          <span className="flex-shrink-0 text-sm text-brand-border">
            {expanded ? "▲" : "▼"}
          </span>
        </div>
      </div>

      {/* 展開區：引用來源列表 */}
      {expanded && (
        <div className="border-t border-brand-border px-4 pb-4 pt-3 space-y-4 bg-brand-light/30">
          {loadingCitations && (
            <p className="text-sm text-gray-400 text-center py-4">載入中…</p>
          )}

          {/* 符合搜尋條件的引用 */}
          {matched?.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-brand mb-2">
                符合條件的引用來源（{matched.length}）
              </h3>
              <div className="space-y-2">
                {matched.map((c) => (
                  <SnippetCard key={c.citation_id} citation={c} keywords={keywords} />
                ))}
              </div>
            </section>
          )}

          {/* 其他引用 */}
          {others?.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-gray-500 mb-2">
                其他引用來源（{others.length}）
              </h3>
              <div className="space-y-2">
                {others.map((c) => (
                  <SnippetCard key={c.citation_id} citation={c} keywords={keywords} />
                ))}
              </div>
            </section>
          )}

          {!loadingCitations && matched?.length === 0 && others?.length === 0 && (
            <p className="text-sm text-gray-400 text-center py-4">無引用資料</p>
          )}
        </div>
      )}
    </div>
  );
}
