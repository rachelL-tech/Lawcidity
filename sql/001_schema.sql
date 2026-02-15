-- 中文 substring 搜尋（ILIKE '%關鍵字%'）的效能核心
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- =========================
-- 1) 法院 / 審判單位（地圖/所在地/篩選用）
-- =========================
CREATE TABLE court_units (
  id         BIGSERIAL PRIMARY KEY,

  unit_norm  TEXT NOT NULL,  -- 詳細到：臺灣新北地方法院三重簡易庭
  root_norm  TEXT NOT NULL,  -- 聚合層級：臺灣新北地方法院 / 臺灣高等法院 / 最高法院

  level      SMALLINT,       -- 1=最高 2=高院 3=地院 4=簡易庭/分院...
  county     TEXT,           -- 縣市（例：新北市）
  district   TEXT,           -- 區（例：三重區）
  address    TEXT,           -- 地址（可後補）
  lat        DOUBLE PRECISION,
  lon        DOUBLE PRECISION,

  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX court_units_unit_uniq ON court_units(unit_norm);
CREATE INDEX court_units_root_idx ON court_units(root_norm);
CREATE INDEX court_units_county_district_idx ON court_units(county, district);
CREATE INDEX court_units_geo_idx ON court_units(lat, lon);


-- =========================
-- 2) 判決節點（唯一真相：placeholder 與完整判決都在這張）
--    placeholder 唯一鍵： (court_root_norm, jyear, jcase_norm, jno)
--    ref_key 由上述四欄位自動生成（不用你手動算）
-- =========================
CREATE TABLE decisions (
  id             BIGSERIAL PRIMARY KEY,

  -- ★ placeholder / 去重的自然鍵
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

  law         TEXT NOT NULL,              -- 例：民法 / 刑法 / 民事訴訟法...
  article_raw TEXT NOT NULL,             -- 條號（TEXT 格式，如「184」「29之1」）
  sub_ref     TEXT NOT NULL DEFAULT '',  -- 項/款/目 qualifier（如「第1項第1款」「前段」）
  raw_match   TEXT,                      -- 命中片段（可選）

  created_at  TIMESTAMPTZ DEFAULT now()
);

-- 去重：同判決理由段，同一法條＋項款只留一次
CREATE UNIQUE INDEX drs_uniq ON decision_reason_statutes(decision_id, law, article_raw, sub_ref);

CREATE INDEX drs_law_article_idx ON decision_reason_statutes(law, article_raw);
CREATE INDEX drs_decision_idx    ON decision_reason_statutes(decision_id);


-- =========================
-- 5) 引用 snippet 內法條（回答「這次引用在講哪條法」）
-- =========================
CREATE TABLE citation_snippet_statutes (
  id          BIGSERIAL PRIMARY KEY,
  citation_id BIGINT NOT NULL REFERENCES citations(id) ON DELETE CASCADE,

  law         TEXT NOT NULL,
  article_raw TEXT NOT NULL,             -- 條號（TEXT 格式，如「184」「29之1」）
  sub_ref     TEXT NOT NULL DEFAULT '',  -- 項/款/目 qualifier（如「第1項第1款」「前段」）
  raw_match   TEXT,                      -- snippet 內命中片段（可選）

  created_at  TIMESTAMPTZ DEFAULT now()
);

-- 去重：同引用邊 snippet 內，同一法條＋項款只記一次
CREATE UNIQUE INDEX css_uniq ON citation_snippet_statutes(citation_id, law, article_raw, sub_ref);

CREATE INDEX css_law_article_idx ON citation_snippet_statutes(law, article_raw);
CREATE INDEX css_citation_idx    ON citation_snippet_statutes(citation_id);


-- =========================
-- 6) 匯入紀錄（追蹤哪些資料夾已處理）
-- =========================
CREATE TABLE ingest_log (
  folder_name    TEXT PRIMARY KEY,
  ingested_at    TIMESTAMPTZ DEFAULT now(),
  decision_count INT,
  citation_count INT,
  notes          TEXT
);
