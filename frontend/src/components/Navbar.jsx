import { Link } from "react-router-dom"; // 給 React Router 用的 <a>，點了會切頁但不整頁重載（SPA）

export default function Navbar() {
  return (
    <nav className="flex items-center justify-between px-6 py-3 bg-white rounded-full shadow-sm mx-4 mt-4">
      {/* 左邊是 Logo 和網站名稱（點了會回首頁），中間是搜尋框，右邊是使用者圖示（暫時用灰色圓代替） */}
      <Link to="/" className="flex items-center gap-2 text-brand font-bold text-lg">
        <span className="w-8 h-8 bg-brand-light rounded-full flex items-center justify-center text-brand">
          ⚖
        </span>
        Lawcidity 法澄
      </Link>
      <div className="flex-1 max-w-md mx-8">
        <input
          type="text"
          placeholder="直接查詢裁判字號"
          className="w-full px-4 py-2 border border-gray-200 rounded-full text-sm focus:outline-none focus:border-brand"
        />
      </div>
      <div className="w-8 h-8 bg-gray-200 rounded-full" />
    </nav>
  );
}
