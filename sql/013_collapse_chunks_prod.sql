-- 013 (正式 RDS, drop-index-first)：折疊 chunks 成「一列 = 一個 (decision_id, chunk_index)」
-- prod 已確認：無 chunk_type / case_type；向量索引 cc_embedding_ivfflat_new (ivfflat, lists=100)。
-- 破壞性：DROP COLUMN + DELETE。執行前務必備份。
-- 執行期間向量查詢會降級（索引重建中走 seq scan）→ 選低流量時段。

BEGIN;

-- 0. 先拿掉向量索引 + 對折疊無用的 citation 索引（折疊時不必維護）
--    保留 cc_decision_chunk_idx_new：下方 EXISTS 刪除要靠 (decision_id, chunk_index) 索引。
DROP INDEX IF EXISTS cc_embedding_ivfflat_new;
DROP INDEX IF EXISTS cc_decision_citation_uniq;
DROP INDEX IF EXISTS cc_citation_idx;

-- 1. 新增 target 陣列欄位
ALTER TABLE chunks ADD COLUMN target_ids           bigint[] DEFAULT '{}';
ALTER TABLE chunks ADD COLUMN target_authority_ids bigint[] DEFAULT '{}';

-- 2. 聚合 target 到每組倖存列（min(id)）
WITH agg AS (
  SELECT decision_id, chunk_index, min(id) AS keep_id,
         array_remove(array_agg(DISTINCT target_id),           NULL) AS t_ids,
         array_remove(array_agg(DISTINCT target_authority_id), NULL) AS ta_ids
  FROM chunks
  GROUP BY decision_id, chunk_index
)
UPDATE chunks c
SET target_ids = agg.t_ids, target_authority_ids = agg.ta_ids
FROM agg
WHERE c.id = agg.keep_id;

-- 3. 刪冗餘列（同組中非最小 id 的）。用 EXISTS 走 (decision_id, chunk_index) 索引，
--    取代 NOT IN（巨大子查詢在小 RAM 會 spill、近乎平方慢）。
DELETE FROM chunks c
WHERE EXISTS (
  SELECT 1 FROM chunks c2
  WHERE c2.decision_id = c.decision_id
    AND c2.chunk_index = c.chunk_index
    AND c2.id < c.id
);

-- 4. 移除 scalar 欄位 + 重複 index，建新 unique
ALTER TABLE chunks
  DROP COLUMN target_id,
  DROP COLUMN target_authority_id,
  DROP COLUMN citation_id;

DROP INDEX IF EXISTS cc_decision_chunk_idx_new;   -- 非唯一，與下方新 unique 重複
CREATE UNIQUE INDEX cc_decision_chunk_uniq ON chunks(decision_id, chunk_index);

COMMIT;

-- 5. 重建 ivfflat（txn 外，沿用原名與 lists=100）。
--    1GB RDS：單執行緒 + 保守 maintenance_work_mem，避免平行 build 的共享記憶體段不足。
SET max_parallel_maintenance_workers = 0;
SET maintenance_work_mem = '256MB';
CREATE INDEX cc_embedding_ivfflat_new ON chunks
  USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- 6. 回收刪列空間
VACUUM (ANALYZE) chunks;
