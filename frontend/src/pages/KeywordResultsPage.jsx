import { useState, useEffect, useRef } from "react";
import { useSearchParams, useNavigate } from "react-router-dom";
import SearchForm from "../components/SearchForm";
import Pagination from "../components/Pagination";
import ResultCard from "../components/ResultCard";
import TargetFilterBar from "../components/TargetFilterBar";
import { search, rerank } from "../lib/api";
import { paramsToSearchRequest, searchRequestToParams, buildSearchSignature } from "../lib/url";

// 搜尋結果頁
// URL 是 state 的唯一來源
// 搜尋條件改變 → 重跑第一階段 source 召回
// doc_types / court_levels / sort / page 改變 → 只重跑 target rerank
export default function SearchResultsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const navigate = useNavigate();

  const [results, setResults] = useState(null); // null = 未完成，[] = 空結果
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // 目前頁面手上正在使用的後端 cache UUID，供 rerank / citations 重用
  const activeCacheIdRef = useRef(null);
  // 目前頁面已對齊的搜尋母體指紋，用來判斷是否需要重跑 full search
  const handledSearchSignatureRef = useRef("");

  // 從 URL 還原目前的搜尋條件
  const req = paramsToSearchRequest(searchParams);
  const searchSignature = buildSearchSignature(searchParams);
  const sessionLookupKey = searchSignature
    ? `search-cache-id:${searchSignature}`
    : null;
  const needsTargetRerank =
    req.doc_types.length > 0 ||
    req.court_levels.length > 0 ||
    req.sort !== "relevance";

  function buildSearchRequest() {
    return {
      keywords: req.keywords,
      statutes: req.statutes,
      exclude_keywords: req.exclude_keywords,
      exclude_statutes: req.exclude_statutes,
      case_types: req.case_types,
      sort: "relevance",
      page: req.page,
      page_size: req.page_size,
    };
  }

  function buildRerankRequest(cacheId) {
    return {
      search_cache_key: cacheId,
      keywords: req.keywords,
      statutes: req.statutes,
      exclude_keywords: req.exclude_keywords,
      exclude_statutes: req.exclude_statutes,
      case_types: req.case_types,
      doc_types: req.doc_types,
      court_levels: req.court_levels,
      sort: req.sort,
      page: req.page,
      page_size: req.page_size,
    };
  }

  // URL 改變時決定要打全量搜尋還是 rerank
  useEffect(() => {
    const storedCacheId = sessionLookupKey
      ? window.sessionStorage.getItem(sessionLookupKey)
      : null;
    if (storedCacheId) {
      activeCacheIdRef.current = storedCacheId;
      handledSearchSignatureRef.current = searchSignature;
    }

    const needFullSearch =
      searchSignature !== handledSearchSignatureRef.current && !storedCacheId;

    async function fetchResults() {
      setLoading(true);
      setError(null);
      try {
        let data;
        if (needFullSearch) {
          const searchData = await search(buildSearchRequest());
          activeCacheIdRef.current = searchData.search_cache_key ?? null;
          handledSearchSignatureRef.current = searchSignature;
          data = needsTargetRerank
            ? await rerank(buildRerankRequest(activeCacheIdRef.current))
            : searchData;
        } else {
          data = await rerank(buildRerankRequest(activeCacheIdRef.current));
        }
        activeCacheIdRef.current = data.search_cache_key ?? activeCacheIdRef.current;
        handledSearchSignatureRef.current = searchSignature;
        if (sessionLookupKey) {
          if (activeCacheIdRef.current) {
            window.sessionStorage.setItem(sessionLookupKey, activeCacheIdRef.current);
          } else {
            window.sessionStorage.removeItem(sessionLookupKey);
          }
        }
        setResults(data.results);
        setTotal(data.total);
      } catch (e) {
        setError(e.message);
        setResults([]);
      } finally {
        setLoading(false);
      }
    }
    fetchResults();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchParams.toString()]);

  // SearchForm 送出新搜尋條件時，更新 URL（會觸發上面的 useEffect）
  function handleSearch(newReq) {
    navigate(`/search?${searchRequestToParams(newReq)}`);
  }

  // 切換排序，page 重置為 1
  function handleSort(sort) {
    const newReq = { ...req, sort, page: 1 };
    setSearchParams(searchRequestToParams(newReq));
  }

  // 切換頁碼
  function handlePage(page) {
    const newReq = { ...req, page };
    setSearchParams(searchRequestToParams(newReq));
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  const totalPages = Math.ceil(total / req.page_size);

  return (
    <div className="max-w-6xl mx-auto px-4 py-8 flex gap-8">
      {/* 左側 Sidebar：搜尋條件 */}
      <aside className="w-72 shrink-0">
        <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-5 sticky top-6">
          <h2 className="text-sm font-semibold text-gray-700 mb-4">更改搜尋條件</h2>
          <SearchForm initialReq={req} onSearch={handleSearch} />
        </div>
      </aside>

      {/* 右側：結果列表 */}
      <main className="flex-1 min-w-0">
        {/* Target 篩選：文書類型 + 法院層級 */}
        <TargetFilterBar
          req={req}
          onChange={(newReq) => setSearchParams(searchRequestToParams(newReq))}
        />

        {/* 結果摘要 + 排序 */}
        <div className="flex items-center justify-between mb-4">
          <div className="text-sm text-gray-500">
            {loading ? (
              "搜尋中…"
            ) : error ? (
              <span className="text-red-500">{error}</span>
            ) : results !== null ? (
              <>
                為您找到 <span className="font-medium text-gray-800">{total}</span> 筆重要實務裁判，點擊可看詳細見解
              </>
            ) : null}
          </div>

          {/* 排序切換 */}
          {!loading && results?.length > 0 && (
            <div className="flex items-center gap-2 text-sm">
              <span className="text-gray-400">排序依據：</span>
              {[
                { value: "relevance", label: "命中率" },
                { value: "total_citation_count", label: "引用次數" },
              ].map(({ value, label }) => (
                <button
                  key={value}
                  onClick={() => handleSort(value)}
                  className={`px-3 py-1 rounded-full border text-sm transition-colors ${
                    req.sort === value
                      ? "bg-brand text-white border-brand"
                      : "border-brand-border text-gray-600 hover:border-brand"
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* 結果列表 */}
        {loading && (
          <div className="text-center py-20 text-gray-400 text-sm">搜尋中…</div>
        )}

        {!loading && error && (
          <div className="text-center py-20 text-red-400 text-sm">{error}</div>
        )}

        {!loading && !error && results?.length === 0 && (
          <div className="text-center py-20 text-gray-400 text-sm">
            沒有符合條件的結果
          </div>
        )}

        {!loading && !error && results?.length > 0 && (
          <div className="space-y-4">
            {results.map((item, i) => (
              <ResultCard
                key={item.target_id != null ? `d-${item.target_id}` : `a-${item.authority_id}`}
                item={item}
                rank={(req.page - 1) * req.page_size + i + 1}
                keywords={req.keywords}
                statutes={req.statutes}
                excludeKeywords={req.exclude_keywords}
                excludeStatutes={req.exclude_statutes}
                caseTypes={req.case_types}
                searchCacheId={activeCacheIdRef.current}
              />
            ))}
          </div>
        )}

        {/* 分頁 */}
        {!loading && totalPages > 1 && (
          <Pagination page={req.page} totalPages={totalPages} onChange={handlePage} />
        )}
      </main>
    </div>
  );
}
