import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import ModeToggle from "../components/ModeToggle";
import SearchForm from "../components/SearchForm";
import AiSearchForm from "../components/AiSearchForm";
import { DEFAULT_SEARCH_REQ, searchRequestToParams } from "../lib/url";
import { warmup } from "../lib/api";

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

// 首頁的範例搜尋：線上資料是六個主題的引用子集，用範例引導訪客搜得到結果的條件
const KEYWORD_EXAMPLES = [
  { label: "Murder", zh: "殺人", keywords: ["殺人", "無罪"], statutes: [{ law: "刑法", article: "271", sub_ref: null }] },
  { label: "Self-defense", zh: "正當防衛", keywords: ["正當防衛"], statutes: [{ law: "刑法", article: "23", sub_ref: null }] },
  { label: "DUI", zh: "酒駕", keywords: ["酒駕", "吐氣酒精濃度"], statutes: [{ law: "刑法", article: "185之3", sub_ref: null }] },
  { label: "Car accident", zh: "車禍", keywords: ["車禍", "與有過失"], statutes: [{ law: "民法", article: "217", sub_ref: null }] },
  { label: "Defamation", zh: "誹謗", keywords: ["誹謗", "言論自由"], statutes: [{ law: "刑法", article: "310", sub_ref: null }] },
  { label: "Wrongful termination", zh: "資遣", keywords: ["資遣", "不能勝任"], statutes: [{ law: "勞動基準法", article: "11", sub_ref: null }] },
];

const AI_EXAMPLES = [
  {
    label: "Staged accident",
    text: "如果我騎車，對方碰瓷，但沒有行車記錄器，該怎麼主張無過失？",
    translation: "I was riding my scooter when someone staged an accident, and I have no dashcam. How can I argue I was not at fault?",
  },
  {
    label: "DUI stop",
    text: "我喝了兩罐啤酒後騎機車回家，被警察攔檢，酒測值每公升 0.27 毫克，但沒有發生任何事故，可以主張什麼來減輕刑責？",
    translation: "I rode my scooter home after two beers and was stopped by police. My breath test was 0.27 mg/L, but there was no accident. What can I argue to reduce the penalty?",
  },
  {
    label: "Self-defense",
    text: "對方先拿棍子打我，我搶下棍子反擊，把他打成骨折，我可以主張正當防衛嗎？",
    translation: "Someone attacked me with a stick. I grabbed it and struck back, breaking his bone. Can I claim self-defense?",
  },
  {
    label: "Online review",
    text: "我在網路評論寫某間餐廳的食物害我食物中毒，店家告我誹謗，我該怎麼主張？",
    translation: "I posted an online review saying a restaurant's food gave me food poisoning, and the owner sued me for defamation. How can I defend myself?",
  },
  {
    label: "Fired for performance",
    text: "公司以我不能勝任工作為由資遣我，但從來沒有給過改善的機會，這樣的資遣合法嗎？",
    translation: "My employer laid me off for poor performance but never gave me a chance to improve. Is the dismissal lawful?",
  },
  {
    label: "Divorce",
    text: "配偶和我分居五年，而且拒絕溝通，我可以訴請離婚嗎？",
    translation: "My spouse and I have lived apart for five years and they refuse to communicate. Can I file for divorce?",
  },
];

export default function DemoPage() {
  const [mode, setMode] = useState("keyword");
  const navigate = useNavigate();
  const text = "What type of cases are you looking for?";
  const subtitle = "Search popular Taiwanese court holdings";
  const { displayed, done, cursorVisible } = useTypingEffect(text, 50);

  // 直接開 /demo 的訪客也先暖機
  useEffect(() => {
    warmup();
  }, []);

  function handleSearch(req) {
    const qs = searchRequestToParams(req);
    navigate(`/search?${qs}`);
  }

  function handleAiSubmit({ query, issues, statutes }) {
    navigate("/ai-results", { state: { query, issues, statutes } });
  }

  return (
    <div className="max-w-2xl mx-auto px-4 py-20 font-body">
      {/* Hero */}
      <div className="text-center mb-10">
        <h1 className="font-display text-brand mb-3 text-4xl">
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
          <SearchForm initialReq={DEFAULT_SEARCH_REQ} onSearch={handleSearch} examples={KEYWORD_EXAMPLES} />
        </div>
      ) : (
        <div className="bg-white rounded-2xl border border-brand-border shadow-sm p-6">
          <AiSearchForm onSubmit={handleAiSubmit} examples={AI_EXAMPLES} />
        </div>
      )}
    </div>
  );
}
