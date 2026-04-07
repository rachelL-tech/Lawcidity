-- 台灣判決引用關係排行榜 — Schema v4
-- decisions 為 citation graph 唯一節點（刪除 cases 表）
-- 最後更新：2026-02

-- 台灣標準時間（Asia/Taipei = UTC+8）
ALTER DATABASE citations SET timezone = 'Asia/Taipei';

-- pgvector 擴充（RAG 語意搜尋用）
CREATE EXTENSION IF NOT EXISTS vector;

-- =========================
-- 全 Reset（DROP 舊表 + 重建）
-- 執行前請確認已連到正確 DB：\c citations
-- =========================
DROP TABLE IF EXISTS chunks                      CASCADE;
DROP TABLE IF EXISTS citation_snippet_statutes   CASCADE;
DROP TABLE IF EXISTS decision_reason_statutes    CASCADE;
DROP TABLE IF EXISTS citations                   CASCADE;
DROP TABLE IF EXISTS authorities                 CASCADE;
DROP TABLE IF EXISTS decisions                   CASCADE;
DROP TABLE IF EXISTS court_units                 CASCADE;
DROP TABLE IF EXISTS ingest_error_log            CASCADE;
DROP TABLE IF EXISTS ingest_log                  CASCADE;


-- =========================
-- 1) 法院 / 審判單位（地圖/所在地/篩選用）
-- =========================
CREATE TABLE court_units (
  id         BIGSERIAL PRIMARY KEY,

  unit_norm  TEXT NOT NULL,  -- 精確名稱：臺灣新北地方法院三重簡易庭
  root_norm  TEXT NOT NULL,  -- 9 種聚合層級（見下）：
                             --   最高法院 / 最高行政法院
                             --   高等法院 / 高等行政法院
                             --   智財商業法院 / 少家法院
                             --   地方法院 / 高等行政法院地方庭
                             --   地方法院簡易庭

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

-- 可選索引（地圖 / 行政區篩選；目前 MVP 預設不建立）
-- CREATE INDEX        court_units_root_idx            ON court_units(root_norm);
-- CREATE INDEX        court_units_county_district_idx ON court_units(county, district);
-- CREATE INDEX        court_units_geo_idx             ON court_units(lat, lon);


-- =========================
-- 2) 判決節點（decisions = citation graph 唯一節點）
--
--    ★ Schema v4：刪除 cases 表，decisions 同時承擔案件識別 + 文書內容
--       - jid 改為 nullable：placeholder（被引用但未匯入的目標）無 JID
--       - 新增 case_type（民事/刑事/行政/憲法）
--       - 兩個 partial unique index 確保去重
--       - citations.source_id / target_id 直接指向 decisions.id
-- =========================
CREATE TABLE decisions (
  id            BIGSERIAL PRIMARY KEY,

  -- ★ 識別欄位（合併原 cases 表）
  unit_norm     TEXT NOT NULL,
  root_norm     TEXT NOT NULL,     -- 9 種聚合層級（同 court_units.root_norm）
  case_type     TEXT,              -- 民事/刑事/行政/憲法；NULL = placeholder（案件類型未知）
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

  -- ★ 文書內容欄位（placeholder 時全部為 NULL）
  jid           TEXT,              -- 官方唯一文書碼；placeholder 為 NULL
  doc_type      TEXT,              -- 本體類型：判決 / 裁定 / 憲判字 / 宣判筆錄
                                   --           調解筆錄 / 和解筆錄 / 補償決定書
                                   -- 支付命令 / 保護令 歸入「裁定」
                                   -- 判例不是本體 doc_type，placeholder 一律為 NULL
  decision_date DATE,
  title         TEXT,
  clean_text    TEXT,              -- clean_judgment_text() 處理後（citation/snippet 用）
  pdf_url       TEXT,

  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now(),

  -- ★ 同字號分組用：第一筆插入時 canonical_id = 自身 id，後續同字號不同 doc_type 指向第一筆
  canonical_id        BIGINT REFERENCES decisions(id),
  -- ★ 預計算顯示字串，省去各查詢重複 string concat
  display_title       TEXT GENERATED ALWAYS AS (
                        jyear::TEXT || '年度' || jcase_norm || '字第' || jno::TEXT || '號'
                      ) STORED,
  -- ★ denormalized 被引來源數（按 canonical 群計的 distinct source），ingest 結束後批次更新
  total_citation_count INT NOT NULL DEFAULT 0,
  -- ★ canonical 群層級 doc_type 衍生欄位：多種本體時為「裁判」，否則為唯一 doc_type
  canonical_doc_type  TEXT
);

-- jid 非空時唯一（正式文書）
CREATE UNIQUE INDEX decisions_jid_uniq ON decisions(jid) WHERE jid IS NOT NULL;

-- placeholder 唯一：同一案號 + case_type + doc_type 只能有一筆 placeholder
-- 同字號可合法並存多種本體（例如「判決」與「裁定」各一筆，doc_type IS NULL 也獨立一筆）
CREATE UNIQUE INDEX decisions_placeholder_uniq
  ON decisions(unit_norm, jyear, jcase_norm, jno, COALESCE(case_type, ''), COALESCE(doc_type, ''))
  WHERE jid IS NULL;

CREATE INDEX decisions_unit_idx       ON decisions(court_unit_id);
CREATE INDEX decisions_canonical_idx  ON decisions(canonical_id);
-- ingest 熱路徑：_set_canonical_id subquery + upsert_target_placeholder jid IS NOT NULL lookup
CREATE INDEX decisions_case_lookup_idx ON decisions(unit_norm, jyear, jcase_norm, jno);

-- 現階段不必要的查詢索引（可日後補建）
-- CREATE INDEX decisions_ref_key_idx    ON decisions(ref_key);
-- CREATE INDEX decisions_root_year_idx  ON decisions(root_norm, jyear);
-- CREATE INDEX decisions_date_idx       ON decisions(decision_date);
-- CREATE INDEX decisions_title_trgm     ON decisions USING GIN (title gin_trgm_ops);
-- CREATE INDEX decisions_cleantext_trgm ON decisions USING GIN (clean_text gin_trgm_ops);

-- =========================
-- 3) 裁判外權威資料（會議決議、釋字、法律座談會等）
--    doc_type 值：決議 / 釋字 / 法律座談會 / 研審小組意見 / 聯席會議決議
--    ref_key 自然鍵範例：'民事庭|77|9'、'144'、'高等法院|111|21'
-- =========================
CREATE TABLE authorities (
  id         BIGSERIAL PRIMARY KEY,
  doc_type   TEXT NOT NULL,    -- 決議 / 釋字 / 法律座談會 / 研審小組意見 / 聯席會議決議
  root_norm  TEXT NOT NULL,    -- 來源機關聚合（如：最高法院、司法院、高等法院）
  ref_key    TEXT NOT NULL,    -- 自然鍵
  display    TEXT,             -- 顯示用完整名稱
  total_citation_count INTEGER NOT NULL DEFAULT 0, -- 歷史上引用此 authority 的 distinct source 數

  created_at TIMESTAMPTZ DEFAULT now(),

  UNIQUE (doc_type, ref_key)
);

-- 可選索引（authority 多維篩選；目前 MVP 預設不建立）
-- CREATE INDEX authorities_doctype_idx ON authorities(doc_type);
-- CREATE INDEX authorities_root_idx    ON authorities(root_norm);


-- =========================
-- 4) 引用邊（source -> target）
--    source_id / target_id → decisions（citation graph 唯一節點）
--    target 為判決（target_id）或裁判外權威（target_authority_id），擇一非 NULL
--    match_start / match_end 對應來源文書 decisions.clean_text 的字元位置
--    target_case_type / target_doc_type：快取 target 的案件類型和文書類型
-- =========================
CREATE TABLE citations (
  id          BIGSERIAL PRIMARY KEY,

  source_id           BIGINT NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
  target_id           BIGINT          REFERENCES decisions(id) ON DELETE CASCADE,
  target_canonical_id BIGINT          REFERENCES decisions(id),
  target_authority_id BIGINT          REFERENCES authorities(id) ON DELETE CASCADE,

  raw_match   TEXT NOT NULL,
  match_start INT,    -- 在 source decision.clean_text 的起點（PDF 折行無法定位時為 NULL）
  match_end   INT,
  snippet     TEXT,

  target_case_type TEXT,  -- 快取：目標的案件類型（民事/刑事/行政）
  target_doc_type  TEXT,  -- citation raw metadata：判決/裁定/判例/裁判/NULL
                          -- 不代表 target 本體 doc_type；判例/裁判 僅表示引用文字所寫

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
CREATE UNIQUE INDEX citations_null_match_decision_uniq
  ON citations(source_id, target_id, raw_match)
  WHERE match_start IS NULL;

CREATE UNIQUE INDEX citations_null_match_authority_uniq
  ON citations(source_id, target_authority_id, raw_match)
  WHERE match_start IS NULL;

CREATE INDEX citations_target_idx       ON citations(target_id);
CREATE INDEX citations_target_canonical_source_idx
  ON citations(target_canonical_id, source_id)
  WHERE target_canonical_id IS NOT NULL;
CREATE INDEX citations_source_match_idx ON citations(source_id, match_start);  -- Phase 2 snippet bridging
CREATE INDEX citations_authority_idx    ON citations(target_authority_id);


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

CREATE UNIQUE INDEX drs_uniq            ON decision_reason_statutes(decision_id, law, article_raw, sub_ref);
CREATE INDEX        drs_law_article_idx ON decision_reason_statutes(law, article_raw);
CREATE INDEX        drs_decision_idx    ON decision_reason_statutes(decision_id);


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

CREATE UNIQUE INDEX css_uniq            ON citation_snippet_statutes(citation_id, law, article_raw, sub_ref);
CREATE INDEX        css_law_article_idx ON citation_snippet_statutes(law, article_raw);  -- RAG statute filter
CREATE INDEX        css_citation_idx    ON citation_snippet_statutes(citation_id);


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
-- 可選索引（resolved 狀態看板；目前 MVP 預設不建立）
-- CREATE INDEX ingest_error_log_resolved_idx ON ingest_error_log(resolved);


-- =========================
-- 9) 語意搜尋 chunks
--    chunk_type = 'citation_context'  : 引用前後文脈絡（由 etl/build_citation_chunks.py 填充）
--    chunk_type = 'supreme_reasoning' : 最高法院理由段（由 etl/build_supreme_chunks.py 填充）
-- =========================
CREATE TABLE chunks (
  id                  BIGSERIAL PRIMARY KEY,
  decision_id         BIGINT NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
  citation_id         BIGINT          REFERENCES citations(id) ON DELETE CASCADE,
  target_id           BIGINT          REFERENCES decisions(id) ON DELETE CASCADE,
  target_authority_id BIGINT          REFERENCES authorities(id) ON DELETE CASCADE,
  chunk_index         INT NOT NULL,
  start_offset        INT NOT NULL,
  end_offset          INT NOT NULL,
  chunk_text          TEXT NOT NULL,
  case_type           TEXT,
  chunk_type          TEXT NOT NULL DEFAULT 'citation_context',  -- 'citation_context' | 'supreme_reasoning'
  embedding           vector(1024),         -- pgvector 語意向量（voyage-law-2，由 embed_and_index.py 填充）
  created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX cc_decision_citation_uniq ON chunks(decision_id, citation_id)
  WHERE chunk_type = 'citation_context';
CREATE UNIQUE INDEX cc_supreme_uniq ON chunks(decision_id, chunk_index)
  WHERE chunk_type = 'supreme_reasoning';
CREATE INDEX        cc_decision_chunk_idx     ON chunks(decision_id, chunk_index);
-- HNSW index 在 embed_and_index.py 跑完後另行執行（見 sql/002_pgvector_migration.sql）
-- CREATE INDEX cc_embedding_hnsw ON chunks USING hnsw (embedding vector_cosine_ops)
--   WITH (m = 16, ef_construction = 64);
