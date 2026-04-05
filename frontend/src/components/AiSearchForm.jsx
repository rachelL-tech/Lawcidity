import { useState } from "react";
import { analyze } from "../lib/api";

/**
 * AI 長文模式搜尋表單。
 * 使用者輸入案情 → 點「AI 分析」→ Gemini 提取爭點/法條 → 顯示勾選 → 送出搜尋。
 *
 * Props:
 *   onSubmit({ query, issues, statutes, case_type }) — 確認後觸發
 */
export default function AiSearchForm({ onSubmit }) {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Gemini 回傳的結果
  const [analysisResult, setAnalysisResult] = useState(null);
  // 勾選狀態
  const [selectedIssues, setSelectedIssues] = useState([]);
  const [selectedStatutes, setSelectedStatutes] = useState([]);

  async function handleAnalyze() {
    if (!text.trim()) return;
    setLoading(true);
    setError(null);
    setAnalysisResult(null);
    try {
      const result = await analyze(text);
      setAnalysisResult(result);
      // 預設全選
      setSelectedIssues(result.issues.map((_, i) => i));
      setSelectedStatutes(result.statutes.map((_, i) => i));
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }

  function toggleIssue(idx) {
    setSelectedIssues((prev) =>
      prev.includes(idx) ? prev.filter((i) => i !== idx) : [...prev, idx]
    );
  }

  function toggleStatute(idx) {
    setSelectedStatutes((prev) =>
      prev.includes(idx) ? prev.filter((i) => i !== idx) : [...prev, idx]
    );
  }

  function handleSubmit() {
    if (!analysisResult) return;
    const issues = selectedIssues.map((i) => analysisResult.issues[i]);
    const statutes = selectedStatutes.map((i) => analysisResult.statutes[i]);
    onSubmit({ query: text, issues, statutes });
  }

  return (
    <div className="space-y-4">
      {/* 輸入區 */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">
          案例事實描述 Describe your case
        </label>
        <textarea
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="e.g. 如果我騎車，對方碰瓷，但沒有行車記錄器，該怎麼主張無過失？&#10;Describe the facts of your case in Chinese for best results."
          rows={5}
          className="w-full border border-brand-border rounded-lg px-3 py-2 text-sm focus:outline-none focus:border-brand resize-y"
        />
      </div>

      {/* AI 分析按鈕 */}
      <button
        type="button"
        onClick={handleAnalyze}
        disabled={loading || !text.trim()}
        className="inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium bg-brand text-white hover:opacity-90 transition-opacity disabled:opacity-50"
      >
        {loading ? (
          <>
            <span className="inline-block w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
            分析中 Analyzing...
          </>
        ) : (
          <>
            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M12 2a4 4 0 0 1 4 4c0 1.5-.8 2.8-2 3.5V11h3a4 4 0 0 1 4 4v1a2 2 0 0 1-2 2h-1v2a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2v-2H5a2 2 0 0 1-2-2v-1a4 4 0 0 1 4-4h3V9.5A4 4 0 0 1 8 6a4 4 0 0 1 4-4z"/>
            </svg>
            AI 分析 Analyze
          </>
        )}
      </button>

      {error && (
        <div className="text-sm text-red-500">{error}</div>
      )}

      {/* 分析結果 — 爭點勾選 */}
      {analysisResult && (
        <div className="border border-brand-border rounded-lg p-4 space-y-4 bg-brand-light/30">
          {/* 爭點 */}
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-2">
              爭點 Issues (select to search)
            </h3>
            <div className="space-y-2">
              {analysisResult.issues.map((issue, i) => (
                <label
                  key={i}
                  className="flex items-start gap-2 text-sm cursor-pointer"
                >
                  <input
                    type="checkbox"
                    checked={selectedIssues.includes(i)}
                    onChange={() => toggleIssue(i)}
                    className="mt-0.5 accent-brand"
                  />
                  <span className="text-gray-700">{issue}</span>
                </label>
              ))}
            </div>
          </div>

          {/* 法條 */}
          <div>
            <h3 className="text-sm font-semibold text-gray-700 mb-2">
              相關法條 Related statutes
            </h3>
            <div className="flex flex-wrap gap-2">
              {analysisResult.statutes.map((s, i) => (
                <button
                  key={i}
                  type="button"
                  onClick={() => toggleStatute(i)}
                  className={`px-3 py-1 rounded-full text-sm border transition-colors ${
                    selectedStatutes.includes(i)
                      ? "bg-brand text-white border-brand"
                      : "bg-white text-gray-600 border-brand-border hover:border-brand"
                  }`}
                >
                  {s.law} 第{s.article}條
                </button>
              ))}
            </div>
          </div>

          {/* 送出 */}
          <button
            type="button"
            onClick={handleSubmit}
            disabled={selectedIssues.length === 0 && selectedStatutes.length === 0}
            className="w-full bg-brand text-white py-2.5 rounded-lg text-sm font-medium hover:opacity-90 transition-opacity disabled:opacity-50"
          >
            搜尋相關判決 Search decisions
          </button>
        </div>
      )}
    </div>
  );
}
