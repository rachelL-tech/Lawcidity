import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import ModeToggle from "../components/ModeToggle";
import SearchForm from "../components/SearchForm";
import AiSearchForm from "../components/AiSearchForm";
import { searchRequestToParams } from "../lib/url";

function useTypingEffect(text, speed = 90) {
  const [charIndex, setCharIndex] = useState(0);
  const done = charIndex >= text.length;

  useEffect(() => {
    if (done) return;
    const timer = setTimeout(() => setCharIndex((i) => i + 1), speed + Math.random() * 40);
    return () => clearTimeout(timer);
  }, [charIndex, done, speed]);

  const [cursorVisible, setCursorVisible] = useState(true);

  useEffect(() => {
    if (!done) return;
    // 閃爍兩下（0.8s × 2 = 1.6s）後消失
    const timer = setTimeout(() => setCursorVisible(false), 1600);
    return () => clearTimeout(timer);
  }, [done]);

  return { displayed: text.substring(0, charIndex), done, cursorVisible };
}

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
  const { displayed, done, cursorVisible } = useTypingEffect("您今天想探索什麼案件類型？");

  function handleSearch(req) {
    const qs = searchRequestToParams(req);
    navigate(`/search?${qs}`);
  }

  function handleAiSubmit({ query, issues, statutes }) {
    // 用 state 傳到 AI 結果頁，避免 URL 太長
    navigate("/ai-results", { state: { query, issues, statutes } });
  }

  return (
    <div className="max-w-2xl mx-auto px-4 py-20">
      {/* Hero */}
      <div className="text-center mb-10">
        <h1 className="text-4xl font-bold text-brand mb-3">
          {displayed}
          {cursorVisible && <span className={`inline-block w-[3px] h-[1.1em] bg-brand ml-1 align-bottom ${done ? "animate-blink" : ""}`} />}
        </h1>
        <p className="text-gray-500 text-base">台灣熱門實務見解搜尋</p>
      </div>

      {/* 模式切換 */}
      <div className="flex flex-col items-center gap-2 mb-8">
        <ModeToggle mode={mode} onChange={setMode} />
        <p className="text-gray-500 text-sm">
          {mode === "keyword"
            ? "輸入關鍵字與法條，搜尋相關實務見解"
            : "輸入完整案情事實，AI 自動分析爭點與法條"}
        </p>
      </div>

      {/* 搜尋表單 */}
      {mode === "keyword" ? (
        <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-6">
          <SearchForm initialReq={EMPTY_REQ} onSearch={handleSearch} />
        </div>
      ) : (
        <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-6">
          <AiSearchForm onSubmit={handleAiSubmit} />
        </div>
      )}
    </div>
  );
}
