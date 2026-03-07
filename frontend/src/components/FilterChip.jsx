// 接兩個 props：
// - label：chip 上顯示的文字
// - onRemove：刪除時要執行的函式

export default function FilterChip({ label, onRemove }) {
  return (
    <span className="inline-flex items-center gap-1 px-3 py-1 rounded-full border border-brand-border bg-brand-light text-sm">
      {label}
      {/* 條件渲染：只有 onRemove 存在時，才顯示後面的按鈕，點下去執行 onRemove。hover:text-brand 表示滑過去變主色 */}
      {onRemove && (
        <button onClick={onRemove} className="text-gray-500 hover:text-brand ml-1">
          ×
        </button>
      )}
    </span>
  );
}
