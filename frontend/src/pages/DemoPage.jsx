import { useState, useEffect } from "react";
import { useNavigate, useParams } from "react-router-dom";
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

export default function DemoPage() {
  const [mode, setMode] = useState("keyword");
  const navigate = useNavigate();
  const { lang = "en" } = useParams();
  const TYPING_TEXT = {
    en: "What type of cases are you looking for?",
    ja: "今日はどんな裁判例をお探しですか？",
    "zh-TW": "您今天想探索什麼案件類型？",
  };
  const SUBTITLE_TEXT = {
    en: "Search popular Taiwanese court holdings",
    ja: "台湾の代表的な裁判例を検索",
    "zh-TW": "台灣熱門實務見解搜尋",
  };
  const text = TYPING_TEXT[lang] || TYPING_TEXT.en;
  const subtitle = SUBTITLE_TEXT[lang] || SUBTITLE_TEXT.en;
  const speed = lang === "en" ? 50 : 90;
  const { displayed, done, cursorVisible } = useTypingEffect(text, speed);

  function handleSearch(req) {
    const qs = searchRequestToParams(req);
    navigate(`/${lang}/search?${qs}`);
  }

  function handleAiSubmit({ query, issues, statutes }) {
    navigate(`/${lang}/ai-results`, { state: { query, issues, statutes } });
  }

  return (
    <div className="max-w-2xl mx-auto px-4 py-20 font-body">
      {/* Hero */}
      <div className="text-center mb-10">
        <h1 className={`font-display text-brand mb-3 ${lang === "en" ? "text-4xl" : "text-5xl"}`}>
          {displayed}
          {cursorVisible && <span className={`inline-block w-[3px] h-[1.1em] bg-brand ml-1 align-bottom ${done ? "animate-blink" : ""}`} />}
        </h1>
        <p className="text-text-secondary text-base">{subtitle}</p>
      </div>

      {/* Mode toggle */}
      <div className="flex justify-center mb-8">
        <ModeToggle mode={mode} onChange={setMode} />
      </div>

      {/* Search form */}
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
