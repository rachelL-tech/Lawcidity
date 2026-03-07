// 「兩段式切換按鈕」元件：根據父元件傳來的 mode 決定 button 背景、字的顏色（狀態在父層），並且點擊時會呼叫 onChange callback 通知父元件切換模式
// Props:
//   mode: 當前模式，值為 "keyword" 或 "ai"
//   onChange: 切換模式時要呼叫的callback 函式

export default function ModeToggle({ mode, onChange }) {
  return (
    // 最底層：大膠囊底座 bg-brand-light
    <div className="relative inline-flex p-1 rounded-full bg-brand-light border border-brand-border">
      {/* - 中間層：會滑動的主色滑塊 bg-brand，transition-all duration-300 讓它看起來像「滑過去」 */}
      <span
        className={`absolute top-1 bottom-1 w-1/2 rounded-full bg-brand transition-all duration-300 ${
          mode === "keyword" ? "left-1" : "left-1/2"
        }`}
      />
      {/* - 最上層：兩個可點擊文字按鈕，relative z-10 讓按鈕文字蓋在滑塊上面 */}
      <button
        onClick={() => onChange("keyword")}
        className={`relative z-10 px-6 py-2.5 text-sm font-medium transition-colors ${
          mode === "keyword" ? "text-white" : "text-brand"
        }`}
      >
        關鍵字模式
      </button>

      <button
        onClick={() => onChange("ai")}
        className={`relative z-10 px-6 py-2.5 text-sm font-medium transition-colors ${
          mode === "ai" ? "text-white" : "text-brand"
        }`}
      >
        AI 長文模式
      </button>
    </div>
  );
}