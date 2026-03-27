-- Migration: add total_citation_count to authorities table
-- Run once on existing DB.

ALTER TABLE authorities
  ADD COLUMN IF NOT EXISTS total_citation_count INTEGER NOT NULL DEFAULT 0;

UPDATE authorities a
SET total_citation_count = (
    SELECT COUNT(*)
    FROM citations c
    WHERE c.target_authority_id = a.id
);
