-- Migration: optimize RAG Path B lookup paths
-- Added: 2026-04-09

CREATE INDEX CONCURRENTLY IF NOT EXISTS cc_citation_context_citation_idx
ON chunks (citation_id)
WHERE chunk_type = 'citation_context' AND embedding IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS drs_law_article_decision_idx
ON decision_reason_statutes (law, article_raw, decision_id);
