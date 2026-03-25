-- Migration: rename citation_chunks → chunks, update chunk_type values
-- Run once on existing DB.

-- 1. Rename table
ALTER TABLE citation_chunks RENAME TO chunks;

-- 2. Update chunk_type values
UPDATE chunks SET chunk_type = 'citation_context'  WHERE chunk_type = 'citation';
UPDATE chunks SET chunk_type = 'supreme_reasoning' WHERE chunk_type = 'supreme';

-- 3. Update default value on column
ALTER TABLE chunks ALTER COLUMN chunk_type SET DEFAULT 'citation_context';

-- 4. Rebuild partial indexes (old WHERE clause values are now stale)
DROP INDEX IF EXISTS cc_decision_citation_uniq;
DROP INDEX IF EXISTS cc_supreme_uniq;

CREATE UNIQUE INDEX cc_decision_citation_uniq ON chunks(decision_id, citation_id)
  WHERE chunk_type = 'citation_context';
CREATE UNIQUE INDEX cc_supreme_uniq ON chunks(decision_id, chunk_index)
  WHERE chunk_type = 'supreme_reasoning';
