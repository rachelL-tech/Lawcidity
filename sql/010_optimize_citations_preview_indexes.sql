-- Rebuild preview indexes so DISTINCT ON (source_id) can pick representative
-- citations cheaply before joining decision metadata for preview ordering.

CREATE INDEX CONCURRENTLY IF NOT EXISTS citations_target_canonical_source_idx_tmp
ON citations(target_canonical_id, source_id, id)
WHERE target_canonical_id IS NOT NULL;

DROP INDEX CONCURRENTLY IF EXISTS citations_target_canonical_source_idx;

ALTER INDEX citations_target_canonical_source_idx_tmp RENAME TO citations_target_canonical_source_idx;

CREATE INDEX CONCURRENTLY IF NOT EXISTS citations_authority_idx_tmp
ON citations(target_authority_id, source_id, id)
WHERE target_authority_id IS NOT NULL;

DROP INDEX CONCURRENTLY IF EXISTS citations_authority_idx;

ALTER INDEX citations_authority_idx_tmp RENAME TO citations_authority_idx;
