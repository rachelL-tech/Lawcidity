import { useState, useRef } from "react";
import FilterChip from "./FilterChip";
import LawCombobox from "./LawCombobox";

const CASE_TYPES = ["民事", "刑事", "行政", "憲法"];

// 主要搜尋表單元件
// Props:
//   initialReq: SearchRequest 初始值（來自 URL params）
//   onSearch(req): 提交搜尋時呼叫，傳入完整 SearchRequest 物件
export default function SearchForm({ initialReq, onSearch }) {
  const [kwInput, setKwInput] = useState("");
  const [keywords, setKeywords] = useState(initialReq.keywords || []);
  // macOS IME：compositionend 在 keydown 之前觸發，isComposing 已是 false，
  // 所以用 ref 記住「剛剛才結束組字」，在 setTimeout(0) 後清掉
  const kwComposingRef = useRef(false);

  const [statutes, setStatutes] = useState(
    // URL 帶來的法條視為已確認（之前搜尋過，必然在白名單內）
    initialReq.statutes.length > 0
      ? initialReq.statutes.map((s) => ({ ...s, confirmed: true }))
      : []
  );

  const [xkwInput, setXkwInput] = useState("");
  const [excludeKeywords, setExcludeKeywords] = useState(initialReq.exclude_keywords || []);
  const xkwComposingRef = useRef(false);

  const [excludeStatutes, setExcludeStatutes] = useState(
    initialReq.exclude_statutes.length > 0
      ? initialReq.exclude_statutes.map((s) => ({ ...s, confirmed: true }))
      : []
  );

  const [caseTypes, setCaseTypes] = useState(initialReq.case_types || []);

  // 新增一個空白法條輸入列
  function addStatute(setter) {
    setter((prev) => [...prev, { law: "", article: null, sub_ref: null }]);
  }

  // 更新指定 index 的法條
  function updateStatute(setter, index, newVal) {
    setter((prev) => prev.map((s, i) => (i === index ? newVal : s)));
  }

  // 刪除指定 index 的法條
  function removeStatute(setter, index) {
    setter((prev) => prev.filter((_, i) => i !== index));
  }

  // 切換案件類型
  function toggleCaseType(ct) {
    setCaseTypes((prev) =>
      prev.includes(ct) ? prev.filter((c) => c !== ct) : [...prev, ct]
    );
  }

  // 提交
  function handleSubmit(e) {
    e.preventDefault();
    // 過濾掉沒填法律名稱、或尚未從白名單確認的法條
    const validStatutes = statutes.filter((s) => s.law.trim() && s.confirmed);
    const validExcludeStatutes = excludeStatutes.filter((s) => s.law.trim() && s.confirmed);
    // 關鍵字和法條都是空的，不送出
    if (keywords.length === 0 && validStatutes.length === 0) return;
    onSearch({
      keywords,
      statutes: validStatutes,
      exclude_keywords: excludeKeywords,
      exclude_statutes: validExcludeStatutes,
      case_types: caseTypes,
      doc_types: initialReq.doc_types || [],
      court_levels: initialReq.court_levels || [],
      sort: initialReq.sort,
      page: 1,
      page_size: 20,
    });
  }

  return (
    <form onSubmit={handleSubmit} className="space-y-4">
      {/* 關鍵字 */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">關鍵字 Keywords</label>
        <div className="flex gap-2">
          <input
            type="text"
            value={kwInput}
            onChange={(e) => setKwInput(e.target.value)}
            onCompositionStart={() => { kwComposingRef.current = true; }}
            onCompositionEnd={() => {
              setTimeout(() => { kwComposingRef.current = false; }, 0);
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !kwComposingRef.current) {
                e.preventDefault();
                const kw = kwInput.trim();
                if (kw && !keywords.includes(kw)) setKeywords((prev) => [...prev, kw]);
                setKwInput("");
              }
            }}
            placeholder="e.g. 行車記錄器, 車禍 — press Enter to add"
            className="border border-brand-border rounded px-3 py-1.5 text-sm flex-1 focus:outline-none focus:border-brand"
          />
        </div>
        {keywords.length > 0 && (
          <div className="flex flex-wrap gap-2 mt-2">
            {keywords.map((kw) => (
              <FilterChip
                key={kw}
                label={kw}
                onRemove={() => setKeywords((prev) => prev.filter((k) => k !== kw))}
              />
            ))}
          </div>
        )}
      </div>

      {/* 法條 */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">法條 Statutes</label>
        <div className="space-y-2">
          {statutes.map((s, i) => (
            <LawCombobox
              key={i}
              value={s}
              onChange={(val) => updateStatute(setStatutes, i, val)}
              onRemove={() => removeStatute(setStatutes, i)}
            />
          ))}
        </div>
        <button
          type="button"
          onClick={() => addStatute(setStatutes)}
          className="mt-2 text-sm text-brand hover:underline"
        >
          + 新增法條 Add statute
        </button>
      </div>

      {/* 排除關鍵字 */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">排除關鍵字 Exclude keywords</label>
        <div className="flex gap-2">
          <input
            type="text"
            value={xkwInput}
            onChange={(e) => setXkwInput(e.target.value)}
            onCompositionStart={() => { xkwComposingRef.current = true; }}
            onCompositionEnd={() => {
              setTimeout(() => { xkwComposingRef.current = false; }, 0);
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !xkwComposingRef.current) {
                e.preventDefault();
                const kw = xkwInput.trim();
                if (kw && !excludeKeywords.includes(kw))
                  setExcludeKeywords((prev) => [...prev, kw]);
                setXkwInput("");
              }
            }}
            placeholder="e.g.民事責任 press Enter to add"
            className="border border-brand-border rounded px-3 py-1.5 text-sm flex-1 focus:outline-none focus:border-brand"
          />
        </div>
        {excludeKeywords.length > 0 && (
          <div className="flex flex-wrap gap-2 mt-2">
            {excludeKeywords.map((kw) => (
              <FilterChip
                key={kw}
                label={kw}
                onRemove={() => setExcludeKeywords((prev) => prev.filter((k) => k !== kw))}
              />
            ))}
          </div>
        )}
      </div>

      {/* 排除法條 */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">排除法條 Exclude statutes</label>
        <div className="space-y-2">
          {excludeStatutes.map((s, i) => (
            <LawCombobox
              key={i}
              value={s}
              onChange={(val) => updateStatute(setExcludeStatutes, i, val)}
              onRemove={() => removeStatute(setExcludeStatutes, i)}
            />
          ))}
        </div>
        <button
          type="button"
          onClick={() => addStatute(setExcludeStatutes)}
          className="mt-2 text-sm text-brand hover:underline"
        >
          + 新增排除法條 Add exclude statute
        </button>
      </div>

      {/* 案件類型 */}
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-2">案件類型 Case type</label>
        <div className="flex flex-wrap gap-2">
          {CASE_TYPES.map((ct) => (
            <button
              key={ct}
              type="button"
              onClick={() => toggleCaseType(ct)}
              className={`px-3 py-1 rounded-full text-sm border transition-colors ${
                caseTypes.includes(ct)
                  ? "bg-brand text-white border-brand"
                  : "bg-white text-gray-600 border-brand-border hover:border-brand"
              }`}
            >
              {ct}
            </button>
          ))}
        </div>
      </div>

      {/* 送出 */}
      <button
        type="submit"
        className="w-full bg-brand text-white py-2.5 rounded-lg text-sm font-medium hover:opacity-90 transition-opacity"
      >
        搜尋 Search
      </button>
    </form>
  );
}
