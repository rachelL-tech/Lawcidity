import { useState } from "react";
import { useNavigate } from "react-router-dom";
import ModeToggle from "../components/ModeToggle";
import SearchForm from "../components/SearchForm";
import { searchRequestToParams } from "../lib/url";

const EMPTY_REQ = {
  keywords: [],
  statutes: [],
  exclude_keywords: [],
  exclude_statutes: [],
  case_types: [],
  sort: "relevance",
  page: 1,
  page_size: 20,
};

export default function HomePage() {
  const [mode, setMode] = useState("keyword");
  const navigate = useNavigate();

  function handleSearch(req) {
    const qs = searchRequestToParams(req);
    navigate(`/search?${qs}`);
  }

  return (
    <div className="max-w-2xl mx-auto px-4 py-20">
      {/* Hero */}
      <div className="text-center mb-10">
        <h1 className="text-4xl font-bold text-brand mb-3">今天您想探索什麼案件類型？</h1>
        <p className="text-gray-500 text-base">台灣熱門實務見解搜尋</p>
      </div>

      {/* 模式切換 */}
      <div className="flex justify-center mb-8">
        <ModeToggle mode={mode} onChange={setMode} />
      </div>

      {/* 搜尋表單 */}
      {mode === "keyword" ? (
        <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-6">
          <SearchForm initialReq={EMPTY_REQ} onSearch={handleSearch} />
        </div>
      ) : (
        <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-6 text-center text-gray-400 text-sm py-16">
          AI 長文模式（開發中）
        </div>
      )}
    </div>
  );
}
