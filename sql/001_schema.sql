-- 台灣判決引用關係排行榜 — 完整 Schema v3
-- 最後更新：2026-02
-- 執行順序：只需跑這一個檔案（全新安裝）

-- 台灣標準時間（Asia/Taipei = UTC+8）
ALTER DATABASE citations SET timezone = 'Asia/Taipei';

-- 中文 substring 搜尋（ILIKE '%關鍵字%'）的效能核心
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- =========================
-- 重置（DROP 舊表，方便重建）
-- =========================
-- DROP TABLE IF EXISTS citation_snippet_statutes CASCADE;
-- DROP TABLE IF EXISTS decision_reason_statutes    CASCADE;
-- DROP TABLE IF EXISTS citations                   CASCADE;
-- DROP TABLE IF EXISTS authorities                 CASCADE;
-- DROP TABLE IF EXISTS decisions                   CASCADE;
-- DROP TABLE IF EXISTS cases                       CASCADE;
-- DROP TABLE IF EXISTS court_units                 CASCADE;
-- DROP TABLE IF EXISTS ingest_error_log            CASCADE;
-- DROP TABLE IF EXISTS ingest_log                  CASCADE;


-- =========================
-- 1) 法院 / 審判單位（地圖/所在地/篩選用）
-- =========================
CREATE TABLE court_units (
  id         BIGSERIAL PRIMARY KEY,

  unit_norm  TEXT NOT NULL,  -- 精確名稱：臺灣新北地方法院三重簡易庭
  root_norm  TEXT NOT NULL,  -- 7 種聚合層級（見下）：
                             --   最高法院 / 最高行政法院
                             --   高等法院 / 高等行政法院 / 高等行政法院地方庭
                             --   地方法院 / 地方法院簡易庭

  level      SMALLINT,       -- 1=最高  2=高院  3=地院/地方庭  4=簡易庭
  county     TEXT,           -- 縣市（例：新北市）
  district   TEXT,           -- 區（例：三重區）
  address    TEXT,           -- 地址（可後補）
  lat        DOUBLE PRECISION,
  lon        DOUBLE PRECISION,

  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX court_units_unit_uniq          ON court_units(unit_norm);
CREATE INDEX        court_units_root_idx            ON court_units(root_norm);
CREATE INDEX        court_units_county_district_idx ON court_units(county, district);
CREATE INDEX        court_units_geo_idx             ON court_units(lat, lon);


-- =========================
-- 2) 案件節點（case 實體）
--
--    ★ cases / decisions 分工設計：
--       cases     = 案件身份識別（citation graph 節點）
--                   儲存 unit_norm, root_norm, jyear, jcase_norm, jno, case_type 等識別欄位。
--                   citations.source_id / target_id 都指向 cases.id。
--                   「placeholder」（被引用但尚未匯入的目標案件）只需一筆 cases，
--                   不需要 decisions，待之後匯入時再補文書。
--       decisions = 文書內容（per-JID，一案可有複數文書，例如判決＋補充裁定）
--                   儲存 jid, doc_type, full_text, clean_text 等文書欄位；jid NOT NULL。
--
--    ★ 自然鍵：(unit_norm, jyear, jcase_norm, jno, COALESCE(case_type, ''))
--       - case_type = NULL 表示「被引用但案件類型未知」的 placeholder（citation 抽取時建立）
--       - 同號但案件類型不同（民事/刑事）= 不同 cases
--       - COALESCE 讓 NULL 與 '' 等值，確保 placeholder 不會重複建立
--       - 最高法院 placeholder：unit_norm = '最高法院'，root_norm = '最高法院'
-- =========================
CREATE TABLE cases (
  id            BIGSERIAL PRIMARY KEY,

  -- ★ 識別欄位（citation graph 節點、placeholder 共用）
  unit_norm     TEXT NOT NULL,
  root_norm     TEXT NOT NULL,     -- 7 種聚合層級（同 court_units.root_norm）
  case_type     TEXT,              -- 民事/刑事/行政；NULL = placeholder（案件類型未知）
  jyear         SMALLINT NOT NULL,
  jcase_norm    TEXT NOT NULL,     -- JCASE 正規化（臺→台）
  jno           INT NOT NULL,

  -- ref_key：穩定可讀的節點 ID（供 citation parser 比對用；不含 case_type）
  ref_key       TEXT GENERATED ALWAYS AS (
                  unit_norm || '|' ||
                  jyear::TEXT || '|' ||
                  jcase_norm || '|' ||
                  jno::TEXT
                ) STORED,

  court_unit_id BIGINT REFERENCES court_units(id),

  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);

-- 自然鍵唯一性：COALESCE 將 NULL case_type 等同 '' 處理，確保 placeholder 不重複建立
CREATE UNIQUE INDEX cases_natural_key_uniq
  ON cases(unit_norm, jyear, jcase_norm, jno, COALESCE(case_type, ''));
CREATE INDEX cases_ref_key_idx    ON cases(ref_key);      -- 非唯一（同 ref_key 可有不同 case_type）
CREATE INDEX cases_root_year_idx  ON cases(root_norm, jyear);
CREATE INDEX cases_unit_idx       ON cases(court_unit_id);


-- =========================
-- 3) 文書節點（decisions = per-JID 文書）
--    ★ 一個 case 可有複數文書（如判決＋補充裁定，各有獨立 JID）
--    ★ jid NOT NULL，由官方 JID 唯一識別文書；識別欄位改由 cases 管理
-- =========================
CREATE TABLE decisions (
  id            BIGSERIAL PRIMARY KEY,

  case_id       BIGINT NOT NULL REFERENCES cases(id) ON DELETE CASCADE,
  jid           TEXT NOT NULL,     -- 官方唯一文書碼（JID），不允許 NULL
  doc_type      TEXT,              -- 判決 / 裁定 / 憲判字 / 宣判筆錄
  decision_date DATE,
  title         TEXT,
  full_text     TEXT,              -- JFULL（原始全文）
  clean_text    TEXT,              -- clean_judgment_text() 處理後（citation/snippet 用）
  pdf_url       TEXT,
  raw           JSONB,             -- 原始 JSON

  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX decisions_jid_uniq       ON decisions(jid);
CREATE INDEX        decisions_case_idx       ON decisions(case_id);
CREATE INDEX        decisions_date_idx       ON decisions(decision_date);
CREATE INDEX        decisions_cleantext_trgm ON decisions USING GIN (clean_text gin_trgm_ops);
CREATE INDEX        decisions_title_trgm     ON decisions USING GIN (title gin_trgm_ops);


-- =========================
-- 4) 裁判外權威資料（會議決議、釋字、法律座談會等）
--    doc_type 值：決議 / 釋字 / 法律座談會 / 研審小組意見
--    ref_key 自然鍵範例：'民事庭|77|9'、'144'、'高等法院|111|21'
-- =========================
CREATE TABLE authorities (
  id         BIGSERIAL PRIMARY KEY,
  doc_type   TEXT NOT NULL,    -- 決議 / 釋字 / 法律座談會 / 研審小組意見
  root_norm  TEXT NOT NULL,    -- 來源機關聚合（如：最高法院、司法院、高等法院）
  ref_key    TEXT NOT NULL,    -- 自然鍵
  display    TEXT,             -- 顯示用完整名稱

  created_at TIMESTAMPTZ DEFAULT now(),

  UNIQUE (doc_type, ref_key)
);

CREATE INDEX authorities_doctype_idx ON authorities(doc_type);
CREATE INDEX authorities_root_idx    ON authorities(root_norm);


-- =========================
-- 5) 引用邊（source -> target）
--    source_id / target_id → cases（非 decisions），在案件層級形成 citation graph
--    target 為判決（target_id）或裁判外權威（target_authority_id），擇一非 NULL
--    match_start / match_end 對應來源文書 decisions.clean_text 的字元位置
-- =========================
CREATE TABLE citations (
  id          BIGSERIAL PRIMARY KEY,

  source_id           BIGINT NOT NULL REFERENCES cases(id)       ON DELETE CASCADE,
  target_id           BIGINT          REFERENCES cases(id)       ON DELETE CASCADE,
  target_authority_id BIGINT          REFERENCES authorities(id) ON DELETE CASCADE,

  raw_match   TEXT NOT NULL,
  match_start INT,    -- 在 source decision.clean_text 的起點（PDF 折行無法定位時為 NULL）
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
-- 6) 判決理由段法條（去重）
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

CREATE UNIQUE INDEX drs_uniq            ON decision_reason_statutes(decision_id, law, article_raw, sub_ref);
CREATE INDEX        drs_law_article_idx ON decision_reason_statutes(law, article_raw);
CREATE INDEX        drs_decision_idx    ON decision_reason_statutes(decision_id);


-- =========================
-- 7) 引用 snippet 內法條
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

CREATE UNIQUE INDEX css_uniq            ON citation_snippet_statutes(citation_id, law, article_raw, sub_ref);
CREATE INDEX        css_law_article_idx ON citation_snippet_statutes(law, article_raw);
CREATE INDEX        css_citation_idx    ON citation_snippet_statutes(citation_id);


-- =========================
-- 8) 匯入紀錄
-- =========================
CREATE TABLE ingest_log (
  folder_name    TEXT PRIMARY KEY,
  ingested_at    TIMESTAMPTZ DEFAULT now(),
  decision_count INT,
  citation_count INT,
  notes          TEXT
);


-- =========================
-- 9) 匯入錯誤紀錄
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
