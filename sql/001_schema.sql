-- 台灣判決引用關係排行榜 — 完整 Schema
-- 最後更新：2026-02
-- 執行順序：只需跑這一個檔案（全新安裝）

-- 中文 substring 搜尋（ILIKE '%關鍵字%'）的效能核心
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- =========================
-- 1) 法院 / 審判單位（地圖/所在地/篩選用）
-- =========================
CREATE TABLE court_units (
  id         BIGSERIAL PRIMARY KEY,

  unit_norm  TEXT NOT NULL,  -- 詳細到：臺灣新北地方法院三重簡易庭
  root_norm  TEXT NOT NULL,  -- 聚合層級：臺灣新北地方法院 / 臺灣高等法院 / 最高法院

  level      SMALLINT,       -- 1=最高 2=高院 3=地院 4=簡易庭
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
-- 2) 判決節點
--    placeholder 與完整判決都在這張表
--    自然鍵：(unit_norm, jyear, jcase_norm, jno)
-- =========================
CREATE TABLE decisions (
  id             BIGSERIAL PRIMARY KEY,

  -- ★ 自然鍵：unit_norm 精確到分院/簡易庭，避免不同分院同號判決互蓋
  --   最高法院 placeholder：unit_norm = '最高法院'
  unit_norm       TEXT NOT NULL,
  court_root_norm TEXT NOT NULL,     -- 聚合層級（顯示/篩選）：臺灣高等法院 / 最高法院
  jyear           SMALLINT NOT NULL,
  jcase_norm      TEXT NOT NULL,     -- JCASE 正規化（臺→台）
  jno             INT NOT NULL,

  -- ref_key：穩定可讀的節點 ID（從自然鍵生成）
  ref_key         TEXT GENERATED ALWAYS AS (
                    unit_norm || '|' ||
                    jyear::TEXT || '|' ||
                    jcase_norm || '|' ||
                    jno::TEXT
                  ) STORED,

  jid             TEXT,              -- 官方唯一碼（有就存；允許 NULL）
  court_unit_id   BIGINT REFERENCES court_units(id),

  decision_date   DATE,
  title           TEXT,
  full_text       TEXT,              -- JFULL（原始全文）
  clean_text      TEXT,              -- clean_judgment_text() 處理後（citation/snippet 用）
  pdf_url         TEXT,
  raw             JSONB,             -- 原始 JSON

  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now(),

  CONSTRAINT decisions_natural_key_uniq UNIQUE (unit_norm, jyear, jcase_norm, jno)
);

CREATE UNIQUE INDEX decisions_ref_key_uniq ON decisions(ref_key);
CREATE UNIQUE INDEX decisions_jid_uniq ON decisions(jid) WHERE jid IS NOT NULL;
CREATE INDEX decisions_court_year_idx  ON decisions(court_root_norm, jyear);
CREATE INDEX decisions_unit_idx        ON decisions(court_unit_id);
CREATE INDEX decisions_date_idx        ON decisions(decision_date);
CREATE INDEX decisions_cleantext_trgm  ON decisions USING GIN (clean_text gin_trgm_ops);
CREATE INDEX decisions_title_trgm      ON decisions USING GIN (title gin_trgm_ops);


-- =========================
-- 3) 裁判外權威資料（會議決議、釋字、法律座談會等）
--    auth_type 慣用值（開放文字，不用 ENUM，未來可直接新增）：
--      'resolution'     最高法院民事/刑事庭會議決議
--      'grand_interp'   司法院大法官釋字
--      'conference'     法律座談會（高等法院/高等行政法院/司法院）
--      'agency_opinion' 研審小組意見
--    auth_key 自然鍵範例：'民事庭|77|9'、'釋字|144'、'高等法院|111|21'
-- =========================
CREATE TABLE authorities (
  id         BIGSERIAL PRIMARY KEY,
  auth_type  TEXT NOT NULL,
  auth_key   TEXT NOT NULL,   -- 自然鍵
  display    TEXT,            -- 顯示用完整名稱
  meta       JSONB,           -- 備用結構化欄位
  created_at TIMESTAMPTZ DEFAULT now(),

  UNIQUE (auth_type, auth_key)
);

CREATE INDEX authorities_type_idx ON authorities(auth_type);


-- =========================
-- 4) 引用邊（source -> target）
--    target 為判決（target_id）或裁判外權威（target_authority_id），擇一非 NULL
-- =========================
CREATE TABLE citations (
  id          BIGSERIAL PRIMARY KEY,

  source_id          BIGINT NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
  target_id          BIGINT          REFERENCES decisions(id) ON DELETE CASCADE,
  target_authority_id BIGINT         REFERENCES authorities(id) ON DELETE CASCADE,

  raw_match   TEXT NOT NULL,
  match_start INT,    -- 在 source.clean_text 的起點（PDF 折行無法定位時為 NULL）
  match_end   INT,
  snippet     TEXT,

  created_at  TIMESTAMPTZ DEFAULT now(),

  CONSTRAINT citations_target_check CHECK (
    num_nonnulls(target_id, target_authority_id) = 1
  )
);

-- 判決引用去重（match_start IS NOT NULL）
CREATE UNIQUE INDEX citations_uniq
  ON citations(source_id, target_id, match_start);

-- authority 引用去重（match_start IS NOT NULL）
CREATE UNIQUE INDEX citations_authority_uniq
  ON citations(source_id, target_authority_id, match_start);

-- match_start IS NULL 去重用 partial unique index
-- （避免同一 source + target + raw_match 的 NULL 列重複堆疊）
CREATE UNIQUE INDEX citations_null_match_decision_uniq
  ON citations(source_id, target_id, raw_match)
  WHERE match_start IS NULL;

CREATE UNIQUE INDEX citations_null_match_authority_uniq
  ON citations(source_id, target_authority_id, raw_match)
  WHERE match_start IS NULL;

CREATE INDEX citations_target_idx    ON citations(target_id);
CREATE INDEX citations_source_idx    ON citations(source_id);
CREATE INDEX citations_authority_idx ON citations(target_authority_id);


-- =========================
-- 5) 判決理由段法條（去重）
-- =========================
CREATE TABLE decision_reason_statutes (
  id          BIGSERIAL PRIMARY KEY,
  decision_id BIGINT NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,

  law         TEXT NOT NULL,
  article_raw TEXT NOT NULL,             -- 條號（如「184」「29之1」）
  sub_ref     TEXT NOT NULL DEFAULT '',  -- 項/款/目（如「第1項第1款」「前段」）
  raw_match   TEXT,

  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX drs_uniq ON decision_reason_statutes(decision_id, law, article_raw, sub_ref);
CREATE INDEX drs_law_article_idx ON decision_reason_statutes(law, article_raw);
CREATE INDEX drs_decision_idx    ON decision_reason_statutes(decision_id);


-- =========================
-- 6) 引用 snippet 內法條
-- =========================
CREATE TABLE citation_snippet_statutes (
  id          BIGSERIAL PRIMARY KEY,
  citation_id BIGINT NOT NULL REFERENCES citations(id) ON DELETE CASCADE,

  law         TEXT NOT NULL,
  article_raw TEXT NOT NULL,
  sub_ref     TEXT NOT NULL DEFAULT '',
  raw_match   TEXT,

  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX css_uniq ON citation_snippet_statutes(citation_id, law, article_raw, sub_ref);
CREATE INDEX css_law_article_idx ON citation_snippet_statutes(law, article_raw);
CREATE INDEX css_citation_idx    ON citation_snippet_statutes(citation_id);


-- =========================
-- 7) 匯入紀錄
-- =========================
CREATE TABLE ingest_log (
  folder_name    TEXT PRIMARY KEY,
  ingested_at    TIMESTAMPTZ DEFAULT now(),
  decision_count INT,
  citation_count INT,
  notes          TEXT
);


-- =========================
-- 8) 匯入錯誤紀錄
--    A = JSON 讀取失敗
--    B = 欄位缺失 / 資料異常
--    D = Citation 抽取 / 寫入失敗
-- =========================
CREATE TABLE ingest_error_log (
  id           BIGSERIAL PRIMARY KEY,
  logged_at    TIMESTAMPTZ DEFAULT now(),

  folder_name  TEXT NOT NULL,
  file_name    TEXT NOT NULL,
  error_type   TEXT NOT NULL,  -- 'A' | 'B' | 'D'
  error_msg    TEXT,

  resolved     BOOLEAN DEFAULT false,
  resolved_at  TIMESTAMPTZ
);

CREATE INDEX ingest_error_log_folder_idx   ON ingest_error_log(folder_name);
CREATE INDEX ingest_error_log_resolved_idx ON ingest_error_log(resolved);
