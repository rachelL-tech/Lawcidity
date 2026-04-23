import { useEffect, useState } from "react";
import { useLocation, useNavigate, Link } from "react-router-dom";
import { analyzeGenerate, fetchDecision } from "../lib/api";

/**
 * AI 分析結果頁。
 *
 * 左側：原始 query + 已選爭點 / 法條
 * 右側：Gemini 全文分析（含 citation tags）+ RAG 來源判決列表
 */
const CACHE_KEY = "ai_results_cache";
const EMPTY_LIST = [];
const EMPTY_AI_RESULT = {
  resolvedKey: null,
  analysis: "",
  ragResults: [],
  error: null,
};

function getCacheKey(query, issues, statutes) {
  return JSON.stringify({ query, issues, statutes });
}

function readCachedAiResult(cacheKey) {
  if (!cacheKey) return null;
  try {
    const raw = sessionStorage.getItem(CACHE_KEY);
    if (!raw) return null;
    const cached = JSON.parse(raw);
    return cached.key === cacheKey ? cached : null;
  } catch {
    return null;
  }
}

export default function AiResultsPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const query = location.state?.query;
  const issues = location.state?.issues ?? EMPTY_LIST;
  const statutes = location.state?.statutes ?? EMPTY_LIST;

  const requestKey = query ? getCacheKey(query, issues, statutes) : null;
  const cachedResult = readCachedAiResult(requestKey);
  const [requestState, setRequestState] = useState(EMPTY_AI_RESULT);
  const hasActiveResult = requestState.resolvedKey === requestKey;
  const activeResult = hasActiveResult ? requestState : EMPTY_AI_RESULT;
  const loading = Boolean(query) && !cachedResult && !hasActiveResult;
  const error = activeResult.error;
  const analysis = hasActiveResult
    ? activeResult.analysis
    : (cachedResult?.analysis ?? "");
  const ragResults = hasActiveResult
    ? activeResult.ragResults
    : (cachedResult?.rag_results ?? EMPTY_LIST);

  useEffect(() => {
    if (!query || !requestKey || cachedResult || hasActiveResult) return;

    let cancelled = false;

    async function loadAnalysis() {
      try {
        const data = await analyzeGenerate({
          query,
          issues,
          statutes,
          top: 10,
        });
        if (cancelled) return;
        setRequestState({
          resolvedKey: requestKey,
          analysis: data.analysis,
          ragResults: data.rag_results,
          error: null,
        });
        // 寫入 sessionStorage
        try {
          sessionStorage.setItem(CACHE_KEY, JSON.stringify({
            key: requestKey,
            analysis: data.analysis,
            rag_results: data.rag_results,
          }));
        } catch { /* storage full, ignore */ }
      } catch (e) {
        if (cancelled) return;
        setRequestState({
          resolvedKey: requestKey,
          analysis: "",
          ragResults: [],
          error: e.message,
        });
      }
    }

    loadAnalysis();

    return () => {
      cancelled = true;
    };
  }, [cachedResult, hasActiveResult, issues, query, requestKey, statutes]);

  if (!query) {
    return (
      <div className="max-w-2xl mx-auto px-4 py-20 text-center">
        <p className="text-gray-500 mb-4">尚無分析資料，請先從首頁輸入案情</p>
        <button
          onClick={() => navigate("/")}
          className="px-6 py-2 bg-brand text-white rounded-lg text-sm"
        >
          回首頁
        </button>
      </div>
    );
  }

  return (
    <div className="max-w-7xl mx-auto px-4 py-8 flex gap-8">
      {/* ── 左側：搜尋條件 ── */}
      <aside className="w-72 shrink-0">
        <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-5 sticky top-6 space-y-4">
          <h2 className="text-sm font-semibold text-gray-700">搜尋條件</h2>

          {/* 原始 query */}
          <div>
            <h3 className="text-xs text-gray-400 mb-1">案例事實</h3>
            <p className="text-sm text-gray-700 leading-relaxed line-clamp-6">
              {query}
            </p>
          </div>

          {/* 爭點 */}
          {issues?.length > 0 && (
            <div>
              <h3 className="text-xs text-gray-400 mb-1">爭點</h3>
              <ul className="space-y-1">
                {issues.map((issue, i) => (
                  <li key={i} className="text-sm text-gray-700 flex gap-1.5">
                    <span className="text-brand font-medium shrink-0">
                      {i + 1}.
                    </span>
                    <span>{issue}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* 法條 */}
          {statutes?.length > 0 && (
            <div>
              <h3 className="text-xs text-gray-400 mb-1">法條</h3>
              <div className="flex flex-wrap gap-1.5">
                {statutes.map((s, i) => (
                  <span
                    key={i}
                    className="px-2 py-0.5 rounded-full bg-brand-light text-sm border border-brand-border text-gray-700"
                  >
                    {s.law} 第{s.article}條
                  </span>
                ))}
              </div>
            </div>
          )}

          <button
            onClick={() => navigate("/")}
            className="w-full text-sm text-brand hover:underline pt-2"
          >
            重新搜尋
          </button>
        </div>
      </aside>

      {/* ── 右側：分析結果 ── */}
      <main className="flex-1 min-w-0 space-y-6">
        {/* Loading */}
        {loading && (
          <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-8 text-center">
            <div className="inline-block w-6 h-6 border-2 border-brand/30 border-t-brand rounded-full animate-spin mb-3" />
            <p className="text-sm text-gray-500">AI 正在分析相關判決...</p>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="bg-white rounded-2xl border border-red-200 shadow-sm p-6">
            <p className="text-sm text-red-500">{error}</p>
          </div>
        )}

        {/* 分析全文 */}
        {!loading && !error && analysis && (
          <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-6">
            <h2 className="text-lg font-bold text-brand mb-4 flex items-center gap-2">
              <svg className="w-5 h-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="12" cy="12" r="10"/>
                <path d="M12 16v-4M12 8h.01"/>
              </svg>
              法律分析
            </h2>
            <div className="prose-analysis">
              <AnalysisContent text={analysis} />
            </div>
          </div>
        )}

        {/* RAG 來源判決 */}
        {!loading && !error && ragResults.length > 0 && (
          <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-6">
            <h2 className="text-sm font-semibold text-gray-700 mb-3">
              參考來源（{ragResults.length} 筆判決）
            </h2>
            <div className="space-y-3">
              {ragResults.map((r) => (
                <RagSourceCard key={r.decision_id} item={r} />
              ))}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

/* ── 分析內文渲染 ── */

/**
 * 解析 Gemini 回傳的 HTML-like 標記，轉成 React 元素。
 *
 * 標記格式：
 *  <cite type="source|target|supreme" id="123">案號</cite>
 *  <statute law="民法" article="294">民法第294條</statute>
 */
function AnalysisContent({ text }) {
  const parts = parseAnalysis(text);
  return <div className="text-sm leading-8 text-gray-800">{parts}</div>;
}

function parseAnalysis(text) {
  // 統一處理 <h3>、<cite> 和 <statute> 標記
  const regex =
    /<h3>(.*?)<\/h3>|<cite\s+type="(source|target|supreme)"\s+id="(\d+)">(.*?)<\/cite>|<statute\s+law="([^"]+)"\s+article="([^"]+)">(.*?)<\/statute>/g;

  const result = [];
  let lastIndex = 0;

  for (const match of text.matchAll(regex)) {
    // 先把前面的純文字推入
    if (match.index > lastIndex) {
      result.push(
        ...renderPlainText(text.slice(lastIndex, match.index), lastIndex)
      );
    }

    if (match[1] !== undefined && match[2] === undefined) {
      // <h3>
      result.push(
        <h3 key={`h3-${match.index}`} className="text-base font-semibold text-gray-900 mt-6 mb-2 pb-1 border-b border-brand-border">
          {match[1]}
        </h3>
      );
    } else if (match[2]) {
      // <cite>
      const type = match[2];
      const id = match[3];
      const label = match[4];
      result.push(
        <CiteTag key={`cite-${match.index}`} type={type} id={id}>
          {label}
        </CiteTag>
      );
    } else {
      // <statute>
      const label = match[7];
      result.push(
        <StatuteTag key={`stat-${match.index}`}>
          {label}
        </StatuteTag>
      );
    }

    lastIndex = match.index + match[0].length;
  }

  // 剩餘文字
  if (lastIndex < text.length) {
    result.push(...renderPlainText(text.slice(lastIndex), lastIndex));
  }

  return result;
}

/**
 * 純文字中的 **粗體**、數字序號等基本 Markdown。
 */
function renderPlainText(str, baseKey) {
  const parts = [];
  // 處理 **bold**
  const boldRegex = /\*\*(.+?)\*\*/g;
  let last = 0;
  for (const m of str.matchAll(boldRegex)) {
    if (m.index > last) {
      parts.push(
        <span key={`t-${baseKey}-${last}`}>{str.slice(last, m.index)}</span>
      );
    }
    parts.push(
      <strong key={`b-${baseKey}-${m.index}`} className="font-semibold text-gray-900">
        {m[1]}
      </strong>
    );
    last = m.index + m[0].length;
  }
  if (last < str.length) {
    parts.push(<span key={`t-${baseKey}-${last}`}>{str.slice(last)}</span>);
  }
  return parts;
}

/* ── Citation Tag ── */

function CiteTag({ type, id, children }) {
  const isSupreme = type === "supreme";
  const isTarget = type === "target";

  // source / supreme → 點擊跳轉 /decisions/{id}
  // target → 點擊顯示 tooltip（被引用總次數等）
  if (isTarget) {
    return <TargetCiteTag id={id}>{children}</TargetCiteTag>;
  }

  return (
    <Link
      to={`/decisions/${id}`}
      className={`inline-flex items-center gap-1 px-2 py-0.5 mx-0.5 rounded text-sm border cursor-pointer transition-colors ${
        isSupreme
          ? "bg-blue-50 border-blue-200 text-blue-700 hover:bg-blue-100"
          : "bg-brand-light border-brand-border text-brand hover:bg-highlight"
      }`}
    >
      <svg className="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
        <polyline points="14 2 14 8 20 8" />
      </svg>
      {children}
    </Link>
  );
}

function TargetCiteTag({ id, children }) {
  const [open, setOpen] = useState(false);
  const [info, setInfo] = useState(null);
  const [fetching, setFetching] = useState(false);

  async function handleClick() {
    setOpen((prev) => !prev);
    if (!info && !fetching) {
      setFetching(true);
      try {
        const data = await fetchDecision(Number(id));
        setInfo(data);
      } catch {
        setInfo({ error: true });
      } finally {
        setFetching(false);
      }
    }
  }

  return (
    <span className="relative inline-block">
      <button
        onClick={handleClick}
        className="inline-flex items-center gap-1 px-2 py-0.5 mx-0.5 rounded text-sm border bg-gray-50 border-gray-300 text-gray-700 hover:bg-gray-100 cursor-pointer transition-colors"
      >
        <svg className="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z" />
          <polyline points="14 2 14 8 20 8" />
        </svg>
        {children}
        <svg className={`w-3 h-3 transition-transform ${open ? "rotate-180" : ""}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <polyline points="6 9 12 15 18 9" />
        </svg>
      </button>
      {open && (
        <div className="absolute top-full left-0 mt-1 z-50 w-64 bg-white border border-gray-200 rounded-lg shadow-lg p-3 text-xs">
          {fetching ? (
            <p className="text-gray-400">載入中…</p>
          ) : info?.error ? (
            <p className="text-red-500">載入失敗</p>
          ) : info ? (
            <>
              {/* <p className="font-medium mb-1">{info.case_ref}</p> */}
              <p className="text-gray-500 mb-2">被引用 {info.total_citation_count} 次</p>
              {info.statutes?.length > 0 && (
                <div className="flex flex-wrap gap-1">
                  {info.statutes.slice(0, 8).map((s, i) => (
                    <span
                      key={i}
                      className="px-1.5 py-0.5 rounded bg-green-50 text-green-700 border border-green-200"
                    >
                      {s.law}§{s.article}
                    </span>
                  ))}
                </div>
              )}
              {/* <Link
                to={`/decisions/${id}`}
                className="block mt-2 text-brand hover:underline"
              >
                查看判決全文
              </Link> */}
            </>
          ) : null}
        </div>
      )}
    </span>
  );
}

/* ── Statute Tag ── */

function StatuteTag({ children }) {
  return (
    <span className="inline-flex items-center px-1.5 py-0.5 mx-0.5 rounded text-sm bg-highlight-statute text-green-800 border border-green-200 font-medium">
      {children}
    </span>
  );
}

/* ── RAG 來源卡片 ── */

function RagSourceCard({ item }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="border border-brand-border rounded-lg p-3">
      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <span
              className={`px-2 py-0.5 rounded text-xs font-medium ${
                item.type.includes("supreme")
                  ? "bg-blue-50 text-blue-700"
                  : "bg-brand-light text-brand"
              }`}
            >
              {item.root_norm}
            </span>
            <Link
              to={`/decisions/${item.decision_id}`}
              className="text-sm font-medium text-gray-800 hover:text-brand truncate"
            >
              {item.display_title.startsWith(item.root_norm)
                ? item.display_title.slice(item.root_norm.length).trim()
                : item.display_title}
            </Link>
          </div>
          <div className="text-xs text-gray-400">
            相似度: {(item.sim * 100).toFixed(1)}%
            {item.statute_hit && " · 法條命中"}
          </div>
        </div>
        <button
          onClick={() => setExpanded(!expanded)}
          className="text-xs text-brand hover:underline shrink-0 ml-2"
        >
          {expanded ? "收合" : "展開"}
        </button>
      </div>
      {expanded && (
        <div className="mt-2 pt-2 border-t border-gray-100">
          <p className="text-xs text-gray-600 leading-6 whitespace-pre-wrap line-clamp-10">
            {item.best_chunk_text}
          </p>
          {item.targets?.length > 0 && (
            <div className="mt-2 flex flex-wrap gap-1">
              {item.targets.map((t) => (
                <span
                  key={t.id}
                  className="text-xs px-2 py-0.5 rounded bg-gray-50 border border-gray-200 text-gray-600"
                >
                  {t.display_title} ({t.total_citation_count}次)
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
