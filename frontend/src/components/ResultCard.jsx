import { useState } from "react";
import DocTypeBadge from "./DocTypeBadge";
import SnippetCard from "./SnippetCard";
import { fetchCitations } from "../lib/api";

// 搜尋結果卡片：顯示一筆被參照判決，點擊展開引用來源列表
// Props:
//   item: SearchResultItem（從 /search API 回傳）
//   keywords: string[] — 傳給 SnippetCard 做 highlight
//   statutes: StatuteFilter[] — 傳給 citations API 篩選 matched
//   excludeKeywords / excludeStatutes / caseTypes — cache miss 時重建第一階段 source 母體
//   searchCacheId: string | null — /search 回傳的後端 cache UUID
export default function ResultCard({
  item,
  keywords,
  statutes,
  excludeKeywords,
  excludeStatutes,
  caseTypes,
  rank,
  searchCacheId,
}) {
  const [expanded, setExpanded] = useState(false);
  const [matched, setMatched] = useState(null); // null = 尚未載入
  const [matchedTotal, setMatchedTotal] = useState(null);
  const [others, setOthers] = useState(null);
  const [othersTotal, setOthersTotal] = useState(null);
  const [loadingCitations, setLoadingCitations] = useState(false);

  // 依 target 類型決定 endpoint
  const targetType = item.target_id != null ? "decisions" : "authorities";
  const targetId = item.target_id ?? item.authority_id;

  const caseRef = item.case_ref || `判決 #${targetId}`;

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
      const resp = await fetchCitations(
        targetType,
        targetId,
        keywords,
        statutes,
        excludeKeywords,
        excludeStatutes,
        caseTypes,
        searchCacheId,
        item.ranked_source_ids ?? [],
      );
      const matchedSources = resp.matched_sources ?? [];
      const otherSources = resp.others_sources ?? [];
      setMatched(matchedSources);
      setMatchedTotal(resp.matched_total ?? matchedSources.length);
      setOthers(otherSources);
      setOthersTotal(resp.others_total ?? otherSources.length);
    } finally {
      setLoadingCitations(false);
    }
  }

  return (
    <div className="bg-white rounded-xl border border-brand-border shadow-sm overflow-hidden">
      {/* 卡片標題列 — 整塊可點擊展開/收合 */}
      <div
        className="px-5 py-4 cursor-pointer select-none hover:bg-gray-50 transition-colors"
        onClick={handleExpand}
      >
        <div className="flex items-center justify-between gap-4">
          {/* 排名序號 */}
          {rank != null && (
            <span className="text-2xl font-bold text-brand-border w-8 text-center shrink-0">
              {rank}
            </span>
          )}

          <div className="flex-1 min-w-0">
            {/* 裁判字號 + 文書類型 badge */}
            <div className="flex items-center gap-2 flex-wrap">
              <span className="font-semibold text-gray-800 text-sm">
                {caseRef}
              </span>
              <DocTypeBadge docType={item.doc_type} />
              {targetType === "decisions" && item.court && (
                <span className="text-xs text-gray-400">{item.court}</span>
              )}
            </div>
          </div>

          {/* 右側：引用次數 + 展開箭頭 */}
          <div className="flex items-center gap-2 shrink-0">
            <span className="text-sm text-gray-500">
              共 <span className="font-semibold text-gray-700">{item.total_citation_count}</span> 筆引用來源
            </span>
            <span
              className={`text-brand transition-transform duration-200 ${
                expanded ? "rotate-180" : ""
              }`}
            >
              ▾
            </span>
          </div>
        </div>
      </div>

      {/* 展開區：引用來源列表 */}
      {expanded && (
        <div className="border-t border-brand-border px-5 pb-5 pt-4 space-y-5 bg-brand-light/30">
          {loadingCitations && (
            <p className="text-sm text-gray-400 text-center py-4">載入中…</p>
          )}

          {/* 符合搜尋條件的引用來源 */}
          {matched?.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-brand mb-2 flex items-center gap-1">
                <span>📎</span>
                完全命中搜尋條件（{matchedTotal ?? matched.length} 筆）
              </h3>
              <div className="space-y-3">
                {matched.map((c) => (
                  <SnippetCard key={c.citation_id} citation={c} keywords={keywords} searchStatutes={statutes} />
                ))}
              </div>
            </section>
          )}

          {/* 其他引用來源 */}
          {others?.length > 0 && (
            <section>
              <h3 className="text-xs font-semibold text-gray-500 mb-2 flex items-center gap-1">
                <span>📎</span>
                未完全命中，但引用此裁判（{othersTotal ?? others.length} 筆）
              </h3>
              <div className="space-y-3">
                {others.map((c) => (
                  <SnippetCard key={c.citation_id} citation={c} keywords={keywords} searchStatutes={statutes} />
                ))}
              </div>
            </section>
          )}

          {!loadingCitations && matched?.length === 0 && others?.length === 0 && (
            <p className="text-sm text-gray-400 text-center py-4">未能找到實務見解符合您的搜尋</p>
          )}
        </div>
      )}
    </div>
  );
}
