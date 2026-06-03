"""
観測用デバッグ endpoint。

- GET /debug/traces — 直近の retrieval trace（in-memory、再起動で消える）の JSON
- GET /debug/traces/ui — 上記を閲覧する自己完結 HTML ビュー

ローカルで埋め込み / KNN / 集約の各 span を即時確認。
"""
from dataclasses import asdict

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

from app.retrieval_trace import get_recent

router = APIRouter(tags=["debug"])

DEFAULT_TRACE_LIMIT = 20


@router.get("/debug/traces")
def list_traces(n: int = DEFAULT_TRACE_LIMIT):
    """直近 n 件の retrieval trace を返す。"""
    return [asdict(t) for t in get_recent(n)]


@router.get("/debug/traces/ui", response_class=HTMLResponse)
def traces_ui():
    """retrieval trace を閲覧する自己完結 HTML（CORS 不要・同一オリジン）。"""
    return _TRACES_HTML


_TRACES_HTML = """<!doctype html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Retrieval Traces</title>
<style>
  :root { color-scheme: dark; }
  body { margin:0; font:14px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;
         background:#0f1117; color:#e6e6e6; padding:24px; }
  h1 { font-size:18px; margin:0 0 4px; }
  .bar { display:flex; gap:8px; align-items:center; margin:12px 0 20px; }
  button, input { font:inherit; background:#1b1f2a; color:#e6e6e6;
         border:1px solid #2c3242; border-radius:6px; padding:6px 10px; }
  button { cursor:pointer; }
  button:hover { background:#262c3a; }
  .muted { color:#8b93a7; }
  .trace { border:1px solid #2c3242; border-radius:10px; margin-bottom:16px; overflow:hidden; }
  .thead { background:#161a24; padding:10px 14px; display:flex; flex-wrap:wrap;
           gap:14px; align-items:baseline; }
  .qid { color:#7aa2f7; font-weight:700; }
  .query { font-weight:700; }
  .pill { background:#1f2430; border:1px solid #2c3242; border-radius:999px;
          padding:1px 8px; font-size:12px; }
  .body { padding:6px 14px 14px; }
  .sec { margin-top:12px; }
  .sec > .lbl { color:#9ece6a; font-weight:700; margin-bottom:4px; }
  table { border-collapse:collapse; width:100%; font-size:13px; }
  th,td { text-align:left; padding:4px 8px; border-bottom:1px solid #232838; }
  th { color:#8b93a7; font-weight:600; }
  td.num { text-align:right; font-variant-numeric:tabular-nums; }
  .empty { color:#8b93a7; padding:40px; text-align:center; }
  details summary { cursor:pointer; color:#8b93a7; margin-top:8px; }
  pre { background:#0b0d13; border:1px solid #232838; border-radius:8px;
        padding:10px; overflow:auto; font-size:12px; }
</style>
</head>
<body>
  <h1>Retrieval Traces <span class="muted">/api/debug/traces</span></h1>
  <div class="bar">
    <button id="reload">↻ 再読み込み</button>
    <label class="muted">件数 <input id="n" type="number" value="20" min="1" style="width:70px"></label>
    <span id="status" class="muted"></span>
  </div>
  <div id="out"></div>

<script>
const esc = s => String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
const ms = v => v == null ? '<span class="muted">—</span>' : esc(v) + ' ms';

function knnTable(knn) {
  if (!knn || !knn.length) return '<div class="muted">（なし）</div>';
  const rows = knn.map(k => `<tr>
    <td>${esc(k.issue)}</td>
    <td class="num">${esc(k.top_sim)}</td>
    <td class="num">${esc(k.median_sim)}</td>
    <td class="num">${esc(k.min_sim)}</td></tr>`).join('');
  return `<table><thead><tr><th>issue</th><th>top_sim</th><th>median_sim</th><th>min_sim</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function aggBlocks(agg) {
  if (!agg || !agg.length) return '<div class="muted">（なし）</div>';
  return agg.map(a => {
    const tg = (a.targets || []).map(t => `<tr>
      <td><span class="pill">${esc(t.type)}</span></td>
      <td>${esc(t.display)}</td>
      <td class="num">${esc(t.chunk_count)}</td>
      <td class="num">${esc(t.total_citation_count)}</td></tr>`).join('');
    return `<div class="sec">
      <div class="lbl">issue: ${esc(a.issue)} <span class="pill">concentration ${esc(a.concentration)}</span></div>
      <table><thead><tr><th>type</th><th>target</th><th>chunk_count</th><th>citation</th></tr></thead>
      <tbody>${tg || '<tr><td colspan=4 class="muted">no targets</td></tr>'}</tbody></table>
    </div>`;
  }).join('');
}

function traceCard(t) {
  const issues = t.embed ? (t.embed.selected_issues || []).map(esc).join(' / ') : '<span class="muted">—</span>';
  const voyage = t.embed ? ms(t.embed.voyage_latency) : '<span class="muted">—</span>';
  return `<div class="trace">
    <div class="thead">
      <span class="qid">#${esc(t.id)}</span>
      <span class="query">${esc(t.raw_query)}</span>
      <span class="pill">${esc(t.created_at)}</span>
      <span class="pill">gemini ${ms(t.gemini_latency)}</span>
    </div>
    <div class="body">
      <div class="sec"><div class="lbl">embed</div>
        <div>issues: ${issues}</div><div>voyage_latency: ${voyage}</div></div>
      <div class="sec"><div class="lbl">knn（召回 sim 分布）</div>${knnTable(t.knn)}</div>
      <div class="sec"><div class="lbl">aggregation（target 集中度）</div>${aggBlocks(t.aggregation)}</div>
      <details><summary>raw JSON</summary><pre>${esc(JSON.stringify(t, null, 2))}</pre></details>
    </div>
  </div>`;
}

async function load() {
  const n = document.getElementById('n').value || 20;
  const status = document.getElementById('status');
  const out = document.getElementById('out');
  status.textContent = '読み込み中…';
  try {
    const res = await fetch(`/api/debug/traces?n=${n}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    status.textContent = `${data.length} 件`;
    out.innerHTML = data.length
      ? data.slice().reverse().map(traceCard).join('')
      : '<div class="empty">まだ trace がありません。先に /api/analyze/generate を叩いてください。</div>';
  } catch (e) {
    status.textContent = '';
    out.innerHTML = `<div class="empty">取得失敗: ${esc(e.message)}</div>`;
  }
}
document.getElementById('reload').addEventListener('click', load);
document.getElementById('n').addEventListener('keydown', e => { if (e.key === 'Enter') load(); });
load();
</script>
</body>
</html>"""
