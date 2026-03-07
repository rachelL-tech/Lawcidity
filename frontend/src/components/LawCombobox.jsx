import { useState, useEffect, useRef } from "react";
import { fetchLaws } from "../lib/api";

// 單一法條輸入元件：讓使用者輸入「法律名稱 autocomplete + 條號＋ 項款目」
// Props:
//   value: { law: "民法", article: "184", sub_ref: "第1項", confirmed: true } — 目前這筆法條的值，父元件SearchForm 持有真正資料的 state，這裡只是用 props 拿來顯示在輸入框裡
//     confirmed: 法律名稱是否已從白名單確認（從下拉選取，或離開欄位時完全符合白名單某項）
//   onChange(newValue): 只要法律名稱、條號、項款目有改，就呼叫它，把新資料傳回父元件
//   onRemove(): 點刪除按鈕時呼叫，讓父元件把這筆法條移掉

export default function LawCombobox({ value, onChange, onRemove }) {
  // state
  const [query, setQuery] = useState(value.law || ""); // 法律名稱輸入框目前顯示的文字
  const [suggestions, setSuggestions] = useState([]); //  autocomplete 下拉選單的候選法律名稱陣列
  const [open, setOpen] = useState(false); // 下拉選單目前是否打開
  const [selectedIndex, setSelectedIndex] = useState(-1); // 鍵盤上下選時，目前 highlight 的候選項 index（-1 表示沒有選）
  const [hasError, setHasError] = useState(false); // 離開欄位後法律名稱仍不在白名單，顯示錯誤提示
  const timerRef = useRef(null); // 存 debounce 的 setTimeout id，避免每打一個字都立刻打 API
  const wrapperRef = useRef(null); // 指向整個元件外層 DOM，用來判斷「使用者是不是點到元件外面」
  const listRef = useRef(null); // 指向下拉選單 <ul>，用來捲動到 highlight 的項目

  // 法律名稱輸入框的 handler
  function handleLawInput(e) {
    // 每次輸入時，讀使用者現在打到哪裡：e 是輸入事件， e.target 是那個 input DOM，e.target.value 就是使用者最新輸入的文字
    const q = e.target.value;
    // 把新文字更新到 query state，React 會 re-render 把新的 query 再餵回 input 的 value
    setQuery(q);
    // 打字就重置確認狀態與錯誤
    setHasError(false);
    // 同步通知父元件，confirmed 設 false（使用者正在修改中，尚未確認）
    onChange({ ...value, law: q, confirmed: false });
    // 清掉前一個 debounce timer
    clearTimeout(timerRef.current);
    // 如果輸入是空白
    if (!q.trim()) {
      // 清掉建議
      setSuggestions([]);
      // 關掉下拉
      setOpen(false);
      return;
    }
    // 否則輸入法律名稱時等 debounce 300ms 再打 API
    timerRef.current = setTimeout(async () => {
      const res = await fetchLaws(q);
      setSuggestions(res.laws);
      setSelectedIndex(-1); // 新建議出來時重置鍵盤選取位置
      setOpen(res.laws.length > 0);
    }, 300);
  }

  // 鍵盤操作：上下選候選項、Enter 確認、Escape 關閉
  function handleLawKeyDown(e) {
    if (e.key === "Enter") {
      e.preventDefault(); // 一定要攔，否則會送出表單
      if (open && selectedIndex >= 0) {
        // 有下拉且使用者已用方向鍵選到某項：選那一項
        // 注意：不自動選第一項，避免中文 IME 按 Enter 確認字時誤觸發
        selectLaw(suggestions[selectedIndex]);
        setSelectedIndex(-1);
      }
      // 沒選到任何項（selectedIndex === -1）或下拉沒開：什麼都不做
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.min(prev + 1, suggestions.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIndex((prev) => Math.max(prev - 1, -1));
    } else if (e.key === "Escape") {
      setOpen(false);
      setSelectedIndex(-1);
    }
  }

  // 使用者點選下拉選單某個法律名稱時的 handler（也被 handleBlur 的自動 match 呼叫）
  function selectLaw(law) {
    // 把輸入框文字改成選到的法律名稱
    setQuery(law);
    setHasError(false);
    // 同步通知父元件，confirmed 設 true 表示已從白名單確認
    onChange({ ...value, law, confirmed: true });
    // 清掉建議
    setSuggestions([]);
    // 關掉下拉
    setOpen(false);
  }

  // 離開輸入框（blur）時：若尚未確認，檢查輸入文字是否可解析成白名單法律
  // 1. 完全符合白名單某項 → 自動確認
  // 2. 剛好只有一筆結果且不是 prefix match → alias 解析成功，自動選取正規全名
  // 3. 其他 → 顯示錯誤提示
  async function handleBlur() {
    if (value.confirmed || !query.trim()) return;
    clearTimeout(timerRef.current); // 取消還在等待的 debounce
    const res = await fetchLaws(query);
    const exact = res.laws.find((l) => l === query);
    if (exact) {
      selectLaw(exact); // 完全符合，自動確認
    } else if (res.laws.length === 1 && !res.laws[0].startsWith(query)) {
      // 只有一筆結果且不是 prefix match → 這筆是 alias 解析出來的正規全名
      selectLaw(res.laws[0]);
    } else {
      setHasError(true);
    }
  }

  // selectedIndex 改變時，把 highlight 的項目捲進可視範圍
  useEffect(() => {
    if (selectedIndex >= 0 && listRef.current) {
      const items = listRef.current.querySelectorAll("li");
      items[selectedIndex]?.scrollIntoView({ block: "nearest" });
    }
  }, [selectedIndex]);

  // 點元件外部時關閉下拉
  // 依賴是 []，代表這個 useEffect 只會在首次元件掛載（mount）和卸載（unmount）時執行一次，不是每次 render 都執行
  useEffect(() => {
    function handleClickOutside(e) {
      // 如果 wrapperRef.current 存在（React 把 DOM 元素存到 current 之後），且點的地方不在這個元件裡，就關掉下拉
      if (wrapperRef.current && !wrapperRef.current.contains(e.target)) {
        setOpen(false);
      }
    }
    // 使用者之後每次點畫面都會觸發 handleClickOutside，檢查是不是點到元件外面
    document.addEventListener("mousedown", handleClickOutside);
    // 在元件卸載時清掉事件監聽器
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  return (
    // 整個法條輸入區，ref={wrapperRef} 告訴React，這個 <div> 綁了一個 ref：wrapperRef，等<div>掛到 DOM 後，React 會把 <div> DOM 節點放進 wrapperRef.current
    <div className="space-y-1" ref={wrapperRef}>
      <div className="flex items-start gap-2 flex-wrap">
      {/* 法律名稱輸入框 + 下拉選單 */}
      <div className="relative">
        {/* value={query} 代表使用者打字，DOM 不會自己直接更新，瀏覽器會先觸發 onChange */}
        <input
          type="text"
          value={query}
          onChange={handleLawInput}
          onKeyDown={handleLawKeyDown}
          onBlur={handleBlur}
          placeholder="法律名稱（必填）"
          className={`border rounded px-3 py-1.5 text-sm w-48 focus:outline-none ${
            hasError ? "border-red-400 focus:border-red-500" : "border-brand-border focus:border-brand"
          }`}
        />
        {/* 下拉選單只有 open === true 時顯示 */}
        {open && (
          <ul ref={listRef} className="absolute z-10 top-full left-0 mt-1 w-64 bg-white border border-brand-border rounded shadow-md max-h-48 overflow-y-auto">
            {suggestions.map((law, i) => (
              // 每個 li 點下去會 selectLaw(law)；鍵盤選到時用 bg-brand-light highlight
              <li
                key={law}
                onMouseDown={() => selectLaw(law)}
                className={`px-3 py-2 text-sm cursor-pointer ${
                  i === selectedIndex ? "bg-brand text-white" : "hover:bg-brand-light"
                }`}
              >
                {law}
              </li>
            ))}
          </ul>
        )}
      </div>

      {/* 條號輸入框：直接把 article 更新回父元件 */}
      <input
        type="text"
        value={value.article || ""}
        onChange={(e) => onChange({ ...value, article: e.target.value || null })}
        placeholder="條（選填）"
        className="border border-brand-border rounded px-3 py-1.5 text-sm w-28 focus:outline-none focus:border-brand"
      />

      {/* 項/款/目輸入框：直接把 sub_ref 更新回父元件 */}
      <input
        type="text"
        value={value.sub_ref || ""}
        onChange={(e) => onChange({ ...value, sub_ref: e.target.value || null })}
        placeholder="項款目（選填）"
        className="border border-brand-border rounded px-3 py-1.5 text-sm w-32 focus:outline-none focus:border-brand"
      />

      {/* 刪除按鈕：子元件要刪除自己時，通知父元件把這筆移掉 */}
      <button
        onClick={onRemove}
        className="text-gray-400 hover:text-red-500 text-lg leading-none mt-1"
        title="移除"
      >
        ×
      </button>
    </div>
    {/* 錯誤提示：法律名稱不在白名單時顯示 */}
    {hasError && (
      <p className="text-xs text-red-500 ml-1">
        此法律未納入索引，建議以關鍵字搜尋
      </p>
    )}
  </div>
  );
}
