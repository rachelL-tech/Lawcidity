-- Migration: 加入最高法院會議決議引用支援
-- 1) 建立 resolutions 表
-- 2) 修改 citations 表支援兩種目標

-- =========================
-- 1) resolutions 表
-- =========================
CREATE TABLE resolutions (
  id          BIGSERIAL PRIMARY KEY,
  jyear       SMALLINT NOT NULL,
  seq_no      SMALLINT NOT NULL,
  court_type  TEXT NOT NULL,  -- '民事庭' | '刑事庭' | '民刑事庭總會' 等
  title       TEXT,           -- 完整標題，如「最高法院77年度第9次民事庭會議決議」
  created_at  TIMESTAMPTZ DEFAULT now()
);

CREATE UNIQUE INDEX resolutions_natural_key ON resolutions(jyear, seq_no, court_type);


-- =========================
-- 2) 修改 citations 表
-- =========================

-- target_id 改為允許 NULL（resolution citation 時為 NULL）
ALTER TABLE citations ALTER COLUMN target_id DROP NOT NULL;

-- 加入 target_resolution_id
ALTER TABLE citations
  ADD COLUMN target_resolution_id BIGINT REFERENCES resolutions(id) ON DELETE CASCADE;

-- CHECK：兩種目標恰好一個非 NULL
ALTER TABLE citations
  ADD CONSTRAINT citations_target_check
    CHECK (
      (target_id IS NOT NULL AND target_resolution_id IS NULL) OR
      (target_id IS NULL     AND target_resolution_id IS NOT NULL)
    );

-- resolution citation 的查詢 index
CREATE INDEX citations_resolution_idx ON citations(target_resolution_id);

-- resolution citation 的去重 index（對應 decisions 的 citations_uniq）
CREATE UNIQUE INDEX citations_resolution_uniq
  ON citations(source_id, target_resolution_id, match_start);
