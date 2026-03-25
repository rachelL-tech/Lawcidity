-- Migration: pgvector + RAG 語意搜尋
-- 對現有 DB 執行（不需要重建整個 schema）
-- 更新日期：2026-03-24

-- 1. 啟用 pgvector 擴充
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. citation_chunks 加入 embedding 欄位
ALTER TABLE citation_chunks
  ADD COLUMN IF NOT EXISTS embedding vector(512);

-- 3. RAG statute filter 索引（只需 law + article_raw，sub_ref 非必要）
CREATE INDEX IF NOT EXISTS css_law_article_idx
  ON citation_snippet_statutes(law, article_raw);

-- 4. chunk_type 欄位（區分 citation chunk 與 supreme chunk）
ALTER TABLE citation_chunks
  ADD COLUMN IF NOT EXISTS chunk_type TEXT NOT NULL DEFAULT 'citation';

-- 5. supreme chunk 唯一索引（decision_id + chunk_index 不重複）
CREATE UNIQUE INDEX IF NOT EXISTS cc_supreme_uniq
  ON citation_chunks(decision_id, chunk_index)
  WHERE chunk_type = 'supreme';

-- 若原 cc_decision_citation_uniq 需加 WHERE 子句（避免 supreme chunks 衝突）：
-- DROP INDEX IF EXISTS cc_decision_citation_uniq;
-- CREATE UNIQUE INDEX cc_decision_citation_uniq ON citation_chunks(decision_id, citation_id)
--   WHERE chunk_type = 'citation';

-- ──────────────────────────────────────────────────────────────────────────
-- 步驟 4：HNSW 向量索引
-- 注意：請在 embed_and_index.py 跑完（所有 embedding 填充後）才執行
--       預估建立時間：230K chunks 約 5-10 分鐘
-- ──────────────────────────────────────────────────────────────────────────
-- CREATE INDEX cc_embedding_hnsw
--   ON citation_chunks
--   USING hnsw (embedding vector_cosine_ops)
--   WITH (m = 16, ef_construction = 64);

-- 若記憶體不足（t3.small），改用 partial index（每次只搜一種 case_type）：
-- CREATE INDEX cc_embedding_hnsw_civil
--   ON citation_chunks
--   USING hnsw (embedding vector_cosine_ops)
--   WITH (m = 16, ef_construction = 64)
--   WHERE case_type = '民事';
