// 分頁元件
// Props:
//   page: 目前頁碼（從 1 開始）
//   totalPages: 總頁數
//   onChange(newPage): 切換頁碼時呼叫
export default function Pagination({ page, totalPages, onChange }) {
  if (totalPages <= 1) return null;

  // 產生要顯示的頁碼陣列：最多顯示 5 頁，超過時在兩端以「...」省略
  function getPages() {
    if (totalPages <= 7) {
      return Array.from({ length: totalPages }, (_, i) => i + 1);
    }
    const pages = [];
    if (page <= 4) {
      // 靠近開頭
      for (let i = 1; i <= 5; i++) pages.push(i);
      pages.push("...");
      pages.push(totalPages);
    } else if (page >= totalPages - 3) {
      // 靠近結尾
      pages.push(1);
      pages.push("...");
      for (let i = totalPages - 4; i <= totalPages; i++) pages.push(i);
    } else {
      // 中間
      pages.push(1);
      pages.push("...");
      for (let i = page - 1; i <= page + 1; i++) pages.push(i);
      pages.push("...");
      pages.push(totalPages);
    }
    return pages;
  }

  const pages = getPages();

  return (
    <div className="flex items-center justify-center gap-1 mt-8">
      {/* 上一頁 */}
      <button
        onClick={() => onChange(page - 1)}
        disabled={page === 1}
        className="px-3 py-1.5 text-sm rounded border border-brand-border disabled:opacity-30 hover:bg-brand-light disabled:hover:bg-transparent"
      >
        ‹
      </button>

      {pages.map((p, i) =>
        p === "..." ? (
          <span key={`ellipsis-${i}`} className="px-2 text-gray-400 text-sm">
            …
          </span>
        ) : (
          <button
            key={p}
            onClick={() => onChange(p)}
            className={`px-3 py-1.5 text-sm rounded border transition-colors ${
              p === page
                ? "bg-brand text-white border-brand"
                : "border-brand-border hover:bg-brand-light"
            }`}
          >
            {p}
          </button>
        )
      )}

      {/* 下一頁 */}
      <button
        onClick={() => onChange(page + 1)}
        disabled={page === totalPages}
        className="px-3 py-1.5 text-sm rounded border border-brand-border disabled:opacity-30 hover:bg-brand-light disabled:hover:bg-transparent"
      >
        ›
      </button>
    </div>
  );
}
