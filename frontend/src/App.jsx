import { BrowserRouter, Routes, Route } from "react-router-dom";
import Navbar from "./components/Navbar";
import HomePage from "./pages/HomePage";
import SearchResultsPage from "./pages/SearchResultsPage";
import DecisionPage from "./pages/DecisionPage";
import AiResultsPage from "./pages/AiResultsPage";

export default function App() {
  return (
    <BrowserRouter>
      <div className="min-h-screen bg-page-bg">
        <Navbar />
        {/* Routes 會根據 BrowserRouter 提供的 router context ，判斷符合目前 pathname 的 route component，執行 component 函式，算出這個元件現在需要的資料、邏輯、事件和畫面（讀 props、執行 hooks、宣告一般變數與計算值、宣告事件處理函式、條件判斷、回傳 JSX） ，React 再更新畫面 commit 到 DOM */}
        <Routes> 
          <Route path="/" element={<HomePage />} />
          <Route path="/search" element={<SearchResultsPage />} />
          <Route path="/decisions/:id" element={<DecisionPage />} />
          <Route path="/ai-results" element={<AiResultsPage />} />
        </Routes>
        <footer className="text-center text-sm text-gray-400 py-6">
          © 2026 Lawcidity 法澄. All Rights Reserved.
        </footer>
      </div>
    </BrowserRouter>
  );
}
