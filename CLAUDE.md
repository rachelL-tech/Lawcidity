# CLAUDE.md — 台灣判決引用關係排行榜（MVP 規格 & Claude Code 工作指令）

> 目標：用「司法院開放判決 JSON（RAR 解壓後）」建立可查詢的引用圖（citation graph），提供：
> 1) 依 filters 篩選來源判決（keyword / 法條 / 法院層級 / 年限 / 地圖所在地），
> 2) 計算「被引用最多」的判決排行（rankings），
> 3) 點進某個被引用判決（target），看到哪些判決引用它與引用 snippet，
> 4)（未來）點進來源判決頁，螢光筆標示 snippet；視覺化（引用網路圖、趨勢圖）；多地圖篩選。

---

## 0) 核心決策（不要偏離）

### MVP 的關鍵字（keyword）
- **不做**自動 keyword tagging、主題分類。
- keyword = 使用者輸入任意字串/片語 → 篩選「來源判決」集合，再用該集合重算被引用排行。
- 技術（建議路線）：OpenSearch（AWS）做混合式搜尋（一般欄位 + ngram），回 `source_id` 清單後由 PostgreSQL 聚合 citations → rankings。
- 備用/對照：PostgreSQL `ILIKE '%keyword%'` + `pg_trgm`（對 `decisions.clean_text` 已有 GIN trgm index）。

### 引用 snippet（可重現、不追求完美段落）
- snippet = 以 citation match 為中心的 window（預設 ±200 字）+ 句子/換行邊界擴張。
- **必存** `raw_match`、`match_start`、`match_end`（方便重切、也能做螢光筆定位）。
- `match_start` / `match_end` 對應 `decisions.clean_text` 的 index（PDF 折行無法定位時可為 NULL），snippet 也從 `clean_text` 取。

### Snippet 邊界擴張策略（混合式）
- 向前優先順序：① 子條款關鍵字（再按/復按/按等）→ ② 編號段落起點（一、㈠、①等）→ ③ 任意 `\r\n` → ④ 固定距離 fallback
- 向後：找 match_end 之後最近的 `）`（citation 收尾括號），fallback 找 `。` 或 `\r\n`
- 引用邊界後處理：actual_start ~ match_start 之間若有其他法院具名引用，推進 actual_start 到最後一個引用收尾之後
- **注意**：snippet 從 `clean_text`（保留換行與段落結構的清理版本）取，才能做換行邊界擴張。

### Week 1 範圍（非常重要）
- **來源判決（source / 引用判決）**：先做「高等法院層級」。
- **目標判決（target / 被引用判決）**：先做「最高法院層級」。
- **時間**：先做近 **3 年**（可配置，預設以 decision_date 或資料月份篩）。
- 抽取到的 citation 只保留 target 是「最高法院」者（其他法院可先忽略或記 log）。

### Week 2+ 擴張方向（依時間）
- 近 5 年（理想近 10 年），或「民事 + 智財（或行政/刑事/家事）各 5 年」。
- 來源判決可逐步納入最高法院（或其他層級），但先守住 Week1 可 demo。

### 引用抽取 regex 策略（已確認）
- **預處理**：先將 `\r\n`、`\n` 及多餘空白移除，再跑 regex（`preprocess_text()`）。
- **核心 regex**（`etl/citation_parser.py`）：
  - `ANY_COURT_CITATION`：比對具名法院 + 年度 + 字別 + 案號
  - `ABBR_CITATION`：省略法院名的連續引用（繼承 `current_court`）
- **需處理的變體格式**：
  | 格式 | 範例 | 備註 |
  |---|---|---|
  | 標準格式 | `最高法院113年度台上字第3527號` | 最常見 |
  | 無「度」字（舊案） | `最高法院40年台上字第86號` | 年度 < 80 常見 |
  | 大法庭 | `最高法院110年度台上大字第5660號` | |
  | 判例後綴 | `最高法院75年台上字第7033號判例` | 擷取時忽略「判例」二字 |
- **jcase_norm 長度限制**：字別 regex 用 `[台臺][^字]{1,20}?`（有上限），防止 `preprocess_text()` 移除換行後跨段誤吃超長字串。
- **ingest guard**：`upsert_target_placeholder` 檢查 `len(jcase_norm) > 50`，超過直接跳過並 log。
- **Week 1 不處理**：「本院」自引（需額外推斷法院）、司法院釋字、憲法法庭裁判。

### 法條拆解策略
- `article_raw TEXT` 儲存條號（如 `184`、`29之1`），`sub_ref TEXT` 儲存項/款/目修飾詞（如 `第1項第1款`、`前段`，無修飾詞為空字串）。
- 法律名稱白名單：`etl/law_names.py`（`LAW_NAMES` + `PSEUDO_LAWS` 虛指詞）。
- 解析器：`etl/statute_parser.py`，3-phase 狀態機（① 省略項款 → ② 省略條號 → ③ 具名法條）。
- 抽取腳本：`etl/extract_statutes.py --decisions`（填 `decision_reason_statutes`）、`--citations`（填 `citation_snippet_statutes`）、`--all`（兩者都跑）。

### 資料來源（已確認 2025-02-12）
- **本機資料**：`/Users/rachel/Downloads/202511/`（2025年11月，已解壓 JSON）
- **JSON 結構**：每份判決固定 8 欄位 — `JID`, `JYEAR`, `JCASE`, `JNO`, `JDATE`, `JTITLE`, `JFULL`, `JPDF`
- **資料夾結構**：以「法院名稱+案件類別」分資料夾（如 `臺灣高等法院刑事/`），檔名格式 `JID欄位.json`
- **資料量**（單月）：134 個法院資料夾，高院層級約 5,000+ 份，最高法院約 1,700 份
- **其他月份**：需另寫腳本從司法院批次下載 RAR 並解壓

### 簡易庭對應表
- **檔案位置**：`etl/simple_court_mapping.py`
- **資料來源**：資料夾名稱（如「三重簡易庭民事」）
- **解析目標**：`parent_court`（所屬地方法院）、`county`（縣市）、`district`（區/鎮/市）
- **維護方式**：共 35 個簡易庭，手動維護 Python dict

### API（裁判書 JDoc）定位
- 可用 jid 打 JDoc 拿全文（不限七日內），但有時段限制，不適合即時 request。
- **MVP 不把 JDoc 當同步依賴**：只做離線/批次補齊（例如 nightly job 或手動補 top targets）。

---

## 現況 snapshot（2025-02-12）

### 資料量
| 表格 | 筆數 |
|------|------|
| decisions | 2,381（最高法院 1,217 + 臺灣高等法院 1,159 + 福建高等法院 5） |
| citations | 1,426（來源：高等法院民事，目標：最高法院） |
| decision_reason_statutes | 7,249（含 sub_ref） |
| citation_snippet_statutes | 1,220（含 sub_ref） |

### 已 ingest 資料夾（`/Users/rachel/Downloads/202511/`）
- 臺灣高等法院民事 / 臺中分院民事 / 臺南分院民事 / 花蓮分院民事 / 高雄分院民事
- 福建高等法院金門分院民事

### Docker / DB
- container: `casemap-db`（postgres:15-alpine, port 5432）
- DB: `citations` / user: `postgres` / password: `postgres`
- DATABASE_URL: `postgresql://postgres:postgres@localhost:5432/citations`

---

## 1) Non-goals（MVP 不做，避免爆炸）
- 不做全量 1996–2025 回溯。
- Week 1 以前不把搜尋服務當成必要依賴；若 keyword 搜尋效能/體驗需要，可在 Week 1–3 引入 OpenSearch（AWS 路線）。
- 不做向量 RAG、相似判決推薦（可以是 Week 4–6 加分，但不能卡住核心）。
- 不做複雜前端：Week 1 以 API + 最簡頁/Swagger demo 為主。

---

## 2) 技術棧 & Repo 結構
- Backend：Python 3.11+，FastAPI
- DB：PostgreSQL 15+（含 `pg_trgm` extension）
- 部署/啟動：Docker Compose（app + db）

Repo 結構：
- `sql/001_schema.sql` — DB schema（確定版，含 sub_ref）
- `etl/ingest_decisions.py` — 匯入/Upsert 判決 JSON（含 citation 抽取）
- `etl/citation_parser.py` — citation 抽取 + snippet 擷取
- `etl/statute_parser.py` — 法條抽取（3-phase 狀態機）
- `etl/extract_statutes.py` — 法條 backfill 腳本
- `etl/law_names.py` — 法律名稱白名單
- `etl/text_cleaner.py` — clean_text 預處理
- `etl/simple_court_mapping.py` — 簡易庭對應表
- `app/main.py` — FastAPI app（rankings + citations API）
- `app/static/index.html` — 前端排行頁

---

## 3) DB Schema
> 完整版見 `sql/001_schema.sql`。以下為各表摘要。

```sql
-- 啟用 pg_trgm（中文 substring 搜尋）
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- court_units：法院/審判單位（地圖/所在地/篩選用）
-- decisions：判決節點，自然鍵 (court_root_norm, jyear, jcase_norm, jno)
-- citations：引用邊 source_id → target_id，含 raw_match / match_start / match_end / snippet
-- decision_reason_statutes：判決理由段法條，含 article_raw + sub_ref
-- citation_snippet_statutes：snippet 內法條，含 article_raw + sub_ref
```

### 法條表欄位（兩張表結構相同）
| 欄位 | 型別 | 說明 |
|------|------|------|
| law | TEXT | 法律名稱，如 `民法` |
| article_raw | TEXT | 條號，如 `184`、`29之1` |
| sub_ref | TEXT | 項/款/目，如 `第1項第1款`、`前段`、`''` |
| raw_match | TEXT | 命中原文片段 |

unique index：`(decision_id/citation_id, law, article_raw, sub_ref)`

---

## 待辦事項

## 里程碑（時程非常短，請嚴格守住）

> 日期參考：期初報告 `2026-02-23`、期中報告 `2026-03-09`、期末報告 `2026-03-30`。

### Phase 1（Week 1）：核心功能可 demo（現在 → 2026-02-23）
- **核心交付**：能以 filters 篩來源判決，重算「被引用最多」排行；點擊可看 citations/snippets。
- **Keyword 搜尋（source filter → rankings）**：定義 `/api/search?q=...` 規格與前端互動。
- **Snippet 全文高亮（展示效果強，建議納入）**：點擊 snippet card 顯示來源判決全文，標示 `<mark>` 並自動捲動（參考 `docs/plans/2026-02-16-snippet-fulltext-highlight.md`）。
- **文件同步**：把本週的 demo 範圍、已知限制、下一步寫回本檔（避免方向散亂）。

### Phase 2（Week 2–3）：週邊功能 + 流程（2026-02-24 → 2026-03-09）
- **時間趨勢**：`/api/trends`（按月引用次數、top targets trend）+ 前端基本圖表。
- **地圖視覺化**：`/api/map`（court_units 座標 + 統計）+ 前端地圖展示與篩選。
- **擴增案件類型**：納入刑事/行政/家事（資料夾解析、匯入策略、統計呈現）。
- **批次下載資料**：新增「從網站批次下載壓縮 JSON + 解壓 + 校驗」腳本，讓資料取得可重現。

### Phase 3（Week 4–6）：除錯、效能、UX 收斂（2026-03-10 → 2026-03-30）
- **正確性與效能**：ETL 可重跑一致性、搜尋/聚合查詢超時保護、索引調校、資料品質檢查。
- **UI/UX 重設計（加分）**：若要學 TypeScript + React，建議放在此 Phase，避免 Week 1–3 變更前端技術棧拖慢交付。

## 2/23 Demo 目標（已決定）
- **必做**：keyword 搜尋（篩來源判決 → 重算 rankings）。優先走 OpenSearch；Postgres `ILIKE + pg_trgm` 可做 fallback/對照。
- **必做（demo 版）**：snippet 全文高亮（點擊 snippet → 新分頁顯示 `clean_text` + `<mark>` 高亮並自動捲動）。
- **延後決策（Week 2–3）**：全文呈現 UI/UX（modal vs 跳轉、版面風格、React/TS 重設計）。

### 功能擴充

- **keyword 搜尋 API + 前端搜尋框**：`GET /api/search?q=關鍵字` → OpenSearch 取回命中的 `source_id` → PostgreSQL 聚合 `citations` 產出 rankings；必要時保留 `ILIKE + pg_trgm` 作 fallback/對照。
- **法條篩選 API**：`GET /api/rankings?law=民法&article=184` → JOIN `decision_reason_statutes`，只顯示引用含指定法條的來源判決的目標排行；`decision_reason_statutes` 已有 191K 筆資料。
- **來源判決頁螢光筆標示**：`GET /api/citations/{citation_id}` 回傳 source decision + highlight range（match_start/end）+ snippet，前端用 `<mark>` 標示。
- **時間趨勢**：`GET /api/trends` 每月引用次數、top targets trend。
- **地圖視覺化**：`GET /api/map` 回傳法院單位座標與統計。

### 維護性

- **前端 snippet 截斷提示**：snippet 若截在句中，末尾補 `…` 並提示使用者點擊查看全文（`app/static/index.html`）。
- **CLAUDE.md schema 區塊更新**：仍是舊版，缺 `unit_norm`、`clean_text`、`resolutions` 表描述。
