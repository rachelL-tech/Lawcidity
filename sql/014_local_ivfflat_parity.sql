-- 014 (本地版)：把本地向量索引從 HNSW 換成 IVFFlat，對齊正式 RDS（1GB 只能 IVFFlat）。
-- 目的：dev/prod parity —— 本地用跟 prod 同型別索引，才測得到 prod 的真實 recall。
--
-- lists = 100：經 recall sweep 實測，在 prod 被 1GB 逼出的低 probes 預算（≤10）下，
--   lists=100 的 recall 反而高於 heuristic 的 400（recall≈probes×rows/lists，lists 少掃得多）。
--   prod 現值也是 100，沿用即可、不必重建 prod 索引。
-- 查詢用 ivfflat.probes=3（rag_service 設定），recall≈0.90 的 knee。
-- 非資料破壞：只動索引，可重建。

DROP INDEX IF EXISTS cc_embedding_hnsw;
DROP INDEX IF EXISTS cc_embedding_ivfflat;   -- 清掉 recall sweep 留下的（lists=400）

SET max_parallel_maintenance_workers = 0;    -- 避免平行 build 的共享記憶體段問題
SET maintenance_work_mem = '512MB';

CREATE INDEX cc_embedding_ivfflat ON chunks
  USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

ANALYZE chunks;
