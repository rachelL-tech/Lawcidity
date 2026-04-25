-- 012: 簡化 chunks 表 — 刪除 supreme_reasoning 資料、移除 chunk_type 和 case_type 欄位
-- 搬家到新 RDS 後執行（pg_dump 還原完畢後跑此腳本）

BEGIN;

-- 1. 刪除 supreme_reasoning 資料
DELETE FROM chunks WHERE chunk_type = 'supreme_reasoning';

-- 2. 移除不再使用的欄位
ALTER TABLE chunks DROP COLUMN chunk_type;
ALTER TABLE chunks DROP COLUMN case_type;

-- 3. 移除 supreme 專用 index
DROP INDEX IF EXISTS cc_supreme_uniq;

-- 4. 移除帶 WHERE chunk_type 條件的舊 index，重建簡化版
DROP INDEX IF EXISTS cc_decision_citation_uniq;
DROP INDEX IF EXISTS cc_citation_context_citation_idx;

CREATE UNIQUE INDEX cc_decision_citation_uniq ON chunks(decision_id, citation_id);
CREATE INDEX cc_citation_idx ON chunks(citation_id) WHERE embedding IS NOT NULL;

-- 5. 移除 Path B 專用 index（009_optimize_rag_path_b_indexes.sql 建的）
DROP INDEX IF EXISTS idx_chunks_citation_context_citation_id;
DROP INDEX IF EXISTS idx_chunks_supreme_reasoning_decision_id;

COMMIT;
