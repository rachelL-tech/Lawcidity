export default function ModeToggle({ mode, onChange }) {
  return (
    <div className="inline-flex rounded-full overflow-hidden border border-brand-border">
      <button
        onClick={() => onChange("keyword")}
        className={`px-6 py-2.5 text-sm font-medium flex items-center gap-2 transition-colors ${
          mode === "keyword"
            ? "bg-brand text-white"
            : "bg-brand-light text-brand"
        }`}
      >
        🔍 關鍵字模式
      </button>
      <button
        onClick={() => onChange("ai")}
        className={`px-6 py-2.5 text-sm font-medium flex items-center gap-2 transition-colors ${
          mode === "ai"
            ? "bg-gray-800 text-white"
            : "bg-brand-light text-gray-600"
        }`}
      >
        💡 AI 長文模式
      </button>
    </div>
  );
}
