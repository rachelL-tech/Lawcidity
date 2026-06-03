import { useEffect, useState } from "react";

import { fetchTraces } from "../lib/api";

const DEFAULT_LIMIT = 20;

// dark dashboard 配色
const C = {
  bg: "bg-[#0f1117]",
  text: "text-[#e6e6e6]",
  card: "border border-[#2c3242]",
  head: "bg-[#161a24]",
  qid: "text-[#7aa2f7]",
  lbl: "text-[#9ece6a]",
  muted: "text-[#8b93a7]",
  pill: "bg-[#1f2430] border border-[#2c3242]",
  rowLine: "border-b border-[#232838]",
};

function fmtMs(v) {
  return v == null ? "—" : `${v} ms`;
}

// created_at 是 UTC，顯示時轉成台灣時間
function fmtTime(iso) {
  return new Date(iso).toLocaleString("zh-TW", { timeZone: "Asia/Taipei" });
}

function sumChunks(targets) {
  return targets.reduce((acc, t) => acc + t.chunk_count, 0);
}

function Pill({ children }) {
  return (
    <span className={`${C.pill} rounded-full px-2 py-0.5 text-xs`}>{children}</span>
  );
}

function AnnTable({ ann }) {
  if (!ann?.length) return <div className={`${C.muted} text-sm`}>（なし）</div>;
  return (
    <table className="w-full text-[13px]">
      <thead>
        <tr className={C.muted}>
          <th className="text-left py-1 px-2 font-semibold">issue</th>
          <th className="text-right py-1 px-2 font-semibold">top_sim</th>
          <th className="text-right py-1 px-2 font-semibold">median_sim</th>
          <th className="text-right py-1 px-2 font-semibold">min_sim</th>
        </tr>
      </thead>
      <tbody>
        {ann.map((k, i) => (
          <tr key={`${k.issue}-${i}`} className={C.rowLine}>
            <td className="py-1 px-2">{k.issue}</td>
            <td className="py-1 px-2 text-right tabular-nums">{k.top_sim}</td>
            <td className="py-1 px-2 text-right tabular-nums">{k.median_sim}</td>
            <td className="py-1 px-2 text-right tabular-nums">{k.min_sim}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function TargetRows({ targets, total }) {
  return (
    <table className="w-full text-[13px]">
      <thead>
        <tr className={C.muted}>
          <th className="text-left py-1 px-2 font-semibold">type</th>
          <th className="text-left py-1 px-2 font-semibold">target</th>
          <th className="text-right py-1 px-2 font-semibold">chunk_count</th>
          <th className="text-right py-1 px-2 font-semibold">share</th>
          <th className="text-right py-1 px-2 font-semibold">citation</th>
        </tr>
      </thead>
      <tbody>
        {targets.map((t, i) => (
          <tr key={`${t.type}-${t.display}-${i}`} className={C.rowLine}>
            <td className="py-1 px-2 align-top">
              <Pill>{t.type}</Pill>
            </td>
            <td className="py-1 px-2">
              <div>{t.display}</div>
              <details className="mt-1">
                <summary className={`${C.muted} cursor-pointer text-xs`}>
                  {t.chunk_texts.length} 段 chunk_text
                </summary>
                <div className="mt-1 space-y-1">
                  {t.chunk_texts.map((text, j) => (
                    <p key={j} className="text-xs leading-relaxed whitespace-pre-wrap">
                      {text}
                    </p>
                  ))}
                </div>
              </details>
            </td>
            <td className="py-1 px-2 text-right tabular-nums align-top">{t.chunk_count}</td>
            <td className="py-1 px-2 text-right tabular-nums align-top">
              {(t.chunk_count / total).toFixed(2)}
            </td>
            <td className="py-1 px-2 text-right tabular-nums align-top">
              {t.total_citation_count}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function AggBlocks({ aggregation }) {
  if (!aggregation?.length) return <div className={`${C.muted} text-sm`}>（なし）</div>;
  return aggregation.map((a, i) => {
    const total = sumChunks(a.targets);
    return (
      <div key={`${a.issue}-${i}`} className="mt-3">
        <div className={`${C.lbl} font-bold mb-1 flex flex-wrap items-center gap-2`}>
          <span>issue: {a.issue}</span>
          <Pill>concentration {a.concentration}</Pill>
        </div>
        {a.targets.length ? (
          <TargetRows targets={a.targets} total={total} />
        ) : (
          <div className={`${C.muted} text-sm`}>no targets</div>
        )}
      </div>
    );
  });
}

function TraceCard({ trace }) {
  const issues = trace.embed ? trace.embed.selected_issues.join(" / ") : "—";
  const voyage = trace.embed ? fmtMs(trace.embed.voyage_latency) : "—";
  return (
    <div className={`${C.card} rounded-[10px] mb-4 overflow-hidden`}>
      <div className={`${C.head} px-3.5 py-2.5 flex flex-wrap gap-3.5 items-baseline`}>
        <span className={`${C.qid} font-bold`}>#{trace.id}</span>
        <span className="font-bold">{trace.raw_query}</span>
        <Pill>{fmtTime(trace.created_at)}</Pill>
        <Pill>gemini {fmtMs(trace.gemini_latency)}</Pill>
      </div>
      <div className="px-3.5 pb-3.5 pt-1.5">
        <div className="mt-3">
          <div className={`${C.lbl} font-bold mb-1`}>embed</div>
          <div className="text-sm">issues: {issues}</div>
          <div className="text-sm">voyage_latency: {voyage}</div>
        </div>
        <div className="mt-3">
          <div className={`${C.lbl} font-bold mb-1`}>ann（検索された chunk の sim 分布）</div>
          <AnnTable ann={trace.ann} />
        </div>
        <div className="mt-3">
          <div className={`${C.lbl} font-bold mb-1`}>aggregation（target 集中度）</div>
          <AggBlocks aggregation={trace.aggregation} />
        </div>
        <details className="mt-2">
          <summary className={`${C.muted} cursor-pointer`}>raw JSON</summary>
          <pre className="bg-[#0b0d13] border border-[#232838] rounded-lg p-2.5 mt-1 overflow-auto text-xs">
            {JSON.stringify(trace, null, 2)}
          </pre>
        </details>
      </div>
    </div>
  );
}

export default function TracesPage() {
  const [traces, setTraces] = useState([]);
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [status, setStatus] = useState("");
  const [error, setError] = useState("");

  async function load() {
    setStatus("読み込み中…");
    setError("");
    try {
      const data = await fetchTraces(limit);
      setTraces(data);
      setStatus(`${data.length} 件`);
    } catch (e) {
      setStatus("");
      setError(e.message);
    }
  }

  useEffect(() => {
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <div className={`${C.bg} ${C.text} font-mono min-h-screen`}>
      <div className="max-w-5xl mx-auto p-6">
      <h1 className="text-lg font-bold mb-1">
        Retrieval Traces <span className={C.muted}>/api/debug/traces</span>
      </h1>
      <div className="flex items-center gap-2 my-4">
        <button
          type="button"
          onClick={load}
          className={`${C.pill} rounded-md px-2.5 py-1.5 cursor-pointer hover:bg-[#262c3a]`}
        >
          ↻ 再読み込み
        </button>
        <label className={`${C.muted} text-sm`}>
          件数{" "}
          <input
            type="number"
            min="1"
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value) || DEFAULT_LIMIT)}
            onKeyDown={(e) => e.key === "Enter" && load()}
            className={`${C.pill} rounded-md px-2 py-1 w-[70px]`}
          />
        </label>
        <span className={`${C.muted} text-sm`}>{status}</span>
      </div>

      {error ? (
        <div className={`${C.muted} text-center py-10`}>取得失敗: {error}</div>
      ) : traces.length ? (
        traces
          .slice()
          .reverse()
          .map((t) => <TraceCard key={t.id} trace={t} />)
      ) : (
        <div className={`${C.muted} text-center py-10`}>
          まだ trace がありません。先に generate を実行してください。
        </div>
      )}
      </div>
    </div>
  );
}
