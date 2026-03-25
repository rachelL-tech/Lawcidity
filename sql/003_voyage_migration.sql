-- Migration: 切換 embedding backend 至 voyage-law-2
-- voyage-law-2 只支援 1024 dims，需將 vector(512) → vector(1024)
-- 更新日期：2026-03-25

-- 1. 清空舊的 512-dim embeddings（格式不相容，必須重跑）
UPDATE chunks SET embedding = NULL;

-- 2. 移除舊欄位，重建為 1024 dims
ALTER TABLE chunks DROP COLUMN embedding;
ALTER TABLE chunks ADD COLUMN embedding vector(1024);

-- 3. citation_id 改為 nullable（supreme chunk 不需要 citation_id）
ALTER TABLE chunks ALTER COLUMN citation_id DROP NOT NULL;

-- 注意：HNSW index 需在 embed_and_index.py 跑完後另行建立
-- CREATE INDEX cc_embedding_hnsw ON chunks
--   USING hnsw (embedding vector_cosine_ops)
--   WITH (m = 16, ef_construction = 64);
