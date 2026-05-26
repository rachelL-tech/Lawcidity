-- 013 (本地版, drop-index-first)：折疊 citation_context chunks 成「一列 = 一個 (decision_id, chunk_index)」
-- 策略：先 DROP HNSW 索引 → 折疊（無索引維護，快）→ 一次性 bulk 重建 HNSW。
-- 本地保留 supreme_reasoning（WHERE 過濾自然跳過，重建索引時一併收回）。
-- 破壞性：DROP COLUMN + DELETE。執行前已備份 chunks（chunks_backup_*.dump）。

BEGIN;

-- 0. 先拿掉 HNSW —— 折疊時就不必逐列維護圖（這是慢的根源）
DROP INDEX IF EXISTS cc_embedding_hnsw;

-- 1. 新增 target 陣列欄位
ALTER TABLE chunks ADD COLUMN target_ids           bigint[] DEFAULT '{}';
ALTER TABLE chunks ADD COLUMN target_authority_ids bigint[] DEFAULT '{}';

-- 2. 聚合 target 到每組倖存列（min(id)），只作用於 citation_context
WITH agg AS (
  SELECT decision_id, chunk_index, min(id) AS keep_id,
         array_remove(array_agg(DISTINCT target_id),           NULL) AS t_ids,
         array_remove(array_agg(DISTINCT target_authority_id), NULL) AS ta_ids
  FROM chunks
  WHERE chunk_type = 'citation_context'
  GROUP BY decision_id, chunk_index
)
UPDATE chunks c
SET target_ids = agg.t_ids, target_authority_ids = agg.ta_ids
FROM agg
WHERE c.id = agg.keep_id;

-- 3. 刪冗餘列（非倖存），只刪 citation_context；supreme_reasoning 全保留
DELETE FROM chunks
WHERE chunk_type = 'citation_context'
  AND id NOT IN (
    SELECT min(id) FROM chunks
    WHERE chunk_type = 'citation_context'
    GROUP BY decision_id, chunk_index
  );

-- 4. 移除 scalar 欄位 + 舊 citation 相關 index，建新 unique（partial）
ALTER TABLE chunks
  DROP COLUMN target_id,
  DROP COLUMN target_authority_id,
  DROP COLUMN citation_id;

DROP INDEX IF EXISTS cc_decision_citation_uniq;
DROP INDEX IF EXISTS cc_citation_context_citation_idx;

CREATE UNIQUE INDEX cc_decision_chunk_uniq
  ON chunks(decision_id, chunk_index)
  WHERE chunk_type = 'citation_context';
-- cc_supreme_uniq 不動

COMMIT;

-- 5. 一次性重建 HNSW（txn 外）
--    關閉平行 maintenance workers：平行 build 會要大的 DSM 共享記憶體段，
--    在本機常因 shm 上限報 "No space left on device"。單執行緒建較慢但穩。
SET max_parallel_maintenance_workers = 0;
SET maintenance_work_mem = '1GB';
CREATE INDEX cc_embedding_hnsw ON chunks
  USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);

-- 6. 回收刪列空間
VACUUM (ANALYZE) chunks;
