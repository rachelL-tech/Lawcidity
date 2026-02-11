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
- keyword = 使用者輸入任意字串/片語 → 在來源判決的 `full_text` 做 substring 搜尋。
- 技術：PostgreSQL `ILIKE '%keyword%'` + `pg_trgm` GIN index（中文 substring 友善）。

### 引用 snippet（可重現、不追求完美段落）
- snippet = 以 citation match 為中心的 window（預設 ±200 字）+ 句子/換行邊界擴張。
- **必存** `raw_match`、`match_start`、`match_end`（方便重切、也能做螢光筆定位）。

### Week 1 範圍（非常重要）
- **來源判決（source / 引用判決）**：先做「高等法院層級」。
- **目標判決（target / 被引用判決）**：先做「最高法院層級」。
- **時間**：先做近 **3 年**（可配置，預設以 decision_date 或資料月份篩）。
- 抽取到的 citation 只保留 target 是「最高法院」者（其他法院可先忽略或記 log）。

### Week 2+ 擴張方向（依時間）
- 近 5 年（理想近 10 年），或「民事 + 智財（或行政/刑事/家事）各 5 年」。
- 來源判決可逐步納入最高法院（或其他層級），但先守住 Week1 可 demo。

### 引用抽取 regex 策略（已確認）
- **預處理**：先將 `\r\n`、`\n` 及多餘空白移除，再跑 regex。
- **核心 regex**（Week 1 只抓最高法院）：
  ```python
  r'最高法院\s*\d{2,3}\s*年\s*度?\s*[台臺]\s*\w+\s*字\s*第\s*\d+\s*號'
  ```
- **需處理的變體格式**：
  | 格式 | 範例 | 備註 |
  |---|---|---|
  | 標準格式 | `最高法院113年度台上字第3527號` | 最常見 |
  | 無「度」字（舊案） | `最高法院40年台上字第86號` | 年度 < 80 常見 |
  | 大法庭 | `最高法院110年度台上大字第5660號` | regex 的 `\w+` 已涵蓋 |
  | 判例後綴 | `最高法院75年台上字第7033號判例` | 擷取時可忽略「判例」二字 |
- **Week 1 不處理**：「本院」自引（需額外推斷法院）、司法院釋字、憲法法庭裁判。

### 資料來源（已確認 2025-02-12）
- **本機資料**：`/Users/rachel/Downloads/202511/`（2025年11月，已解壓 JSON）
- **JSON 結構**：每份判決固定 8 欄位 — `JID`, `JYEAR`, `JCASE`, `JNO`, `JDATE`, `JTITLE`, `JFULL`, `JPDF`
- **資料夾結構**：以「法院名稱+案件類別」分資料夾（如 `臺灣高等法院刑事/`），檔名格式 `JID欄位.json`
- **資料量**（單月）：134 個法院資料夾，高院層級約 5,000+ 份，最高法院約 1,700 份
- **其他月份**：需另寫腳本從司法院批次下載 RAR 並解壓

### API（裁判書 JDoc）定位
- 可用 jid 打 JDoc 拿全文（不限七日內），但有時段限制，不適合即時 request。
- **MVP 不把 JDoc 當同步依賴**：只做離線/批次補齊（例如 nightly job 或手動補 top targets）。

---

## 1) Non-goals（MVP 不做，避免爆炸）
- 不做全量 1996–2025 回溯。
- 不引入 Elasticsearch / OpenSearch 作為必要依賴（先用 Postgres pg_trgm）。
- 不做向量 RAG、相似判決推薦（可以是 Week 4–6 加分，但不能卡住核心）。
- 不做複雜前端：Week 1 以 API + 最簡頁/Swagger demo 為主。

---

## 2) 技術棧 & Repo 結構
- Backend：Python 3.11+，FastAPI
- DB：PostgreSQL 15+（含 `pg_trgm` extension）
- 部署/啟動：Docker Compose（app + db）

建議 repo 結構：
- `sql/001_schema.sql`  — DB schema（照下方「確定版 schema」寫入）
- `sql/002_optional_statute_paragraphs.sql` — （可選）法條項款欄位擴充
- `etl/ingest_decisions.py` — 匯入/Upsert 判決 JSON
- `etl/extract_citations.py` — 從 `decisions.full_text` 抽 citations → upsert target placeholder → insert citations
- `etl/extract_statutes.py` — 抽法條（理由段 & snippet）
- `app/main.py` — FastAPI app
- `app/db.py` — DB 連線/簡單查詢層（可用 psycopg）
- `tests/test_citation_parser.py`
- `tests/test_statute_parser.py`
- `tests/test_snippet.py`
- `data/samples/` — 放 3 份高院判決全文（你提供的），作為抽取測試基準

---

## 3) DB Schema
> 直接把以下內容存成 `sql/001_schema.sql`，用 docker init 或手動 psql 執行。

```sql
-- 中文 substring 搜尋（ILIKE '%關鍵字%'）的效能核心
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =========================
-- 1) 法院 / 審判單位（篩選用）
--    Week 1 精簡版：只保留 level, root_norm, unit_norm
--    Week 2+ 擴到地院時再 ALTER TABLE ADD county, lat, lon 等地理欄位
-- =========================
CREATE TABLE court_units (
  id         BIGSERIAL PRIMARY KEY,

  unit_norm  TEXT NOT NULL,  -- 詳細到：臺灣新北地方法院三重簡易庭
  root_norm  TEXT NOT NULL,  -- 聚合層級：臺灣新北地方法院 / 臺灣高等法院 / 最高法院
  level      SMALLINT,       -- 1=最高 2=高院 3=地院 4=簡易庭/分院...

  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX court_units_unit_uniq ON court_units(unit_norm);
CREATE INDEX court_units_root_idx ON court_units(root_norm);


-- =========================
-- 2) 判決節點（唯一真相：placeholder 與完整判決都在這張）
--    placeholder 唯一鍵： (court_root_norm, jyear, jcase_norm, jno)
--    ref_key 由上述四欄位自動生成（不用你手動算）
-- =========================
CREATE TABLE decisions (
  id             BIGSERIAL PRIMARY KEY,

  -- ★ placeholder / 去重的自然鍵（最穩）
  court_root_norm TEXT NOT NULL,     -- 例：臺灣新北地方法院 / 臺灣高等法院 / 最高法院
  jyear           SMALLINT NOT NULL, -- JYEAR（案號年度）
  jcase_norm      TEXT NOT NULL,     -- JCASE（字別正規化）
  jno             INT NOT NULL,      -- JNO（號次）

  -- ★ ref_key：穩定可讀的節點 ID（從自然鍵生成）
  ref_key         TEXT GENERATED ALWAYS AS (
                    court_root_norm || '|' ||
                    jyear::TEXT || '|' ||
                    jcase_norm || '|' ||
                    jno::TEXT
                  ) STORED,

  -- 官方唯一碼（有就存；允許 NULL）
  jid             TEXT,

  -- 地圖/所在地：只存 FK（乾淨）
  court_unit_id   BIGINT REFERENCES court_units(id),

  -- 顯示/搜尋/抽取
  decision_date   DATE,        -- JDATE（裁判日期）
  title           TEXT,        -- JTITLE
  full_text       TEXT,        -- JFULL（全文；抽引用/keyword/法條都靠它）
  pdf_url         TEXT,        -- JPDF（查看原文）
  raw             JSONB,       -- 原始 JSON（除錯/回補欄位）

  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now(),

  -- 自然鍵唯一：placeholder 釘死在這裡
  CONSTRAINT decisions_natural_key_uniq UNIQUE (court_root_norm, jyear, jcase_norm, jno)
);

-- ref_key 也加 unique（雖然自然鍵已 unique，但 ref_key 常用來查詢）
CREATE UNIQUE INDEX decisions_ref_key_uniq ON decisions(ref_key);

-- jid 若有也必須唯一（避免同一官方文件重複匯入）
CREATE UNIQUE INDEX decisions_jid_uniq ON decisions(jid) WHERE jid IS NOT NULL;

-- 常用索引
CREATE INDEX decisions_court_year_idx ON decisions(court_root_norm, jyear);
CREATE INDEX decisions_unit_idx ON decisions(court_unit_id);
CREATE INDEX decisions_date_idx ON decisions(decision_date);

-- keyword 搜尋索引
CREATE INDEX decisions_fulltext_trgm ON decisions USING GIN (full_text gin_trgm_ops);
CREATE INDEX decisions_title_trgm    ON decisions USING GIN (title gin_trgm_ops);


-- =========================
-- 3) 引用邊（source -> target）
-- =========================
CREATE TABLE citations (
  id          BIGSERIAL PRIMARY KEY,

  source_id   BIGINT NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
  target_id   BIGINT NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,

  raw_match   TEXT NOT NULL,   -- 原始命中的引用字串
  match_start INT,             -- 在 source.full_text 的起點 index
  match_end   INT,             -- 在 source.full_text 的終點 index
  snippet     TEXT,            -- 以 match 為中心切出的上下文（展示/除錯）

  created_at  TIMESTAMPTZ DEFAULT now()
);

-- 去重：同來源同目標同一位置不要重複插
CREATE UNIQUE INDEX citations_uniq ON citations(source_id, target_id, match_start);

-- 查「誰引用它」/「它引用誰」都會用到
CREATE INDEX citations_target_idx ON citations(target_id);
CREATE INDEX citations_source_idx ON citations(source_id);


-- =========================
-- 4) 理由段法條（去重）
--    用來做法條 filter（MVP 強建議）
-- =========================
CREATE TABLE decision_reason_statutes (
  id          BIGSERIAL PRIMARY KEY,
  decision_id BIGINT NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,

  law         TEXT NOT NULL,  -- 例：民法 / 刑法 / 民事訴訟法...
  article     INT  NOT NULL,  -- 條號
  raw_match   TEXT,           -- 命中片段（可選）

  created_at  TIMESTAMPTZ DEFAULT now()
);

-- 去重：同判決理由段，同一法條只留一次
CREATE UNIQUE INDEX drs_uniq ON decision_reason_statutes(decision_id, law, article);

CREATE INDEX drs_law_article_idx ON decision_reason_statutes(law, article);
CREATE INDEX drs_decision_idx    ON decision_reason_statutes(decision_id);


-- =========================
-- 5) 引用 snippet 內法條（回答「這次引用在講哪條法」）
-- =========================
CREATE TABLE citation_snippet_statutes (
  id          BIGSERIAL PRIMARY KEY,
  citation_id BIGINT NOT NULL REFERENCES citations(id) ON DELETE CASCADE,

  law         TEXT NOT NULL,
  article     INT  NOT NULL,
  raw_match   TEXT,           -- snippet 內命中片段（可選）

  created_at  TIMESTAMPTZ DEFAULT now()
);

-- 去重：同引用邊 snippet 內，同一法條只記一次
CREATE UNIQUE INDEX css_uniq ON citation_snippet_statutes(citation_id, law, article);

CREATE INDEX css_law_article_idx ON citation_snippet_statutes(law, article);
CREATE INDEX css_citation_idx    ON citation_snippet_statutes(citation_id);
