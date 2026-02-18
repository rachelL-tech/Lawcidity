-- Migration 004：新增 authorities 表，合併 resolutions，重構 citations
-- 適用情境：truncate + reload（citations 與 citation_snippet_statutes 都會清空重建）
--
-- 執行前確認：
--   docker exec -i casemap-db psql -U postgres -d citations -f /path/to/004_authorities.sql

BEGIN;

-- =========================
-- 清空引用資料（截斷後重跑 ingest）
-- =========================
TRUNCATE citations CASCADE;   -- CASCADE 同時清空 citation_snippet_statutes

-- =========================
-- 移除舊的 target_resolution_id 欄位（FK 自動跟著 drop）
-- =========================
ALTER TABLE citations DROP COLUMN target_resolution_id;

-- =========================
-- 刪除舊 resolutions 表（citations FK 已移除，可安全刪除）
-- =========================
DROP TABLE resolutions;

-- =========================
-- 建立 authorities 表（開放 auth_type，不用 ENUM）
--
-- auth_type 慣用值：
--   'resolution'     最高法院民事/刑事庭會議決議（原 resolutions 表）
--   'grand_interp'   司法院大法官釋字
--   'conference'     法律座談會（高等法院/高等行政法院/司法院）
--   'agency_opinion' 研審小組意見
--   （未來可直接新增值，不需 ALTER TYPE）
-- =========================
CREATE TABLE authorities (
  id         BIGSERIAL PRIMARY KEY,
  auth_type  TEXT NOT NULL,
  auth_key   TEXT NOT NULL,   -- 自然鍵：'民事庭|77|9'、'釋字|144'、'高等法院|111|21'
  display    TEXT,            -- 顯示用完整名稱
  meta       JSONB,           -- 備用結構化欄位
  created_at TIMESTAMPTZ DEFAULT now(),

  UNIQUE (auth_type, auth_key)
);

CREATE INDEX authorities_type_idx ON authorities(auth_type);

-- =========================
-- citations 加第三個 target 欄位
-- =========================
ALTER TABLE citations
  ADD COLUMN target_authority_id BIGINT
  REFERENCES authorities(id) ON DELETE CASCADE;

-- 更新 check constraint（target_id 和 target_authority_id 二選一，恰好一個非 NULL）
ALTER TABLE citations DROP CONSTRAINT IF EXISTS citations_target_check;
ALTER TABLE citations ADD CONSTRAINT citations_target_check CHECK (
  num_nonnulls(target_id, target_authority_id) = 1
);

-- authority 引用的 unique index（NULL 各自獨立，不受影響）
CREATE UNIQUE INDEX citations_authority_uniq
  ON citations(source_id, target_authority_id, match_start);
CREATE INDEX citations_authority_idx ON citations(target_authority_id);

-- =========================
-- match_start IS NULL 去重用 partial unique index
-- （避免同一 source + target + raw_match 的 NULL 列重複堆疊）
-- =========================
CREATE UNIQUE INDEX citations_null_match_decision_uniq
  ON citations(source_id, target_id, raw_match)
  WHERE match_start IS NULL;

CREATE UNIQUE INDEX citations_null_match_authority_uniq
  ON citations(source_id, target_authority_id, raw_match)
  WHERE match_start IS NULL;

COMMIT;
