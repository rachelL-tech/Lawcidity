-- Migration: citation offsets are now required for every citation row.
-- Added: 2026-04-24

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM citations
    WHERE match_start IS NULL OR match_end IS NULL
  ) THEN
    RAISE EXCEPTION
      'citations contains NULL offsets; clean legacy rows before applying 011_make_citation_offsets_not_null.sql';
  END IF;
END $$;

DROP INDEX IF EXISTS citations_null_match_decision_uniq;
DROP INDEX IF EXISTS citations_null_match_authority_uniq;

ALTER TABLE citations
  ALTER COLUMN match_start SET NOT NULL,
  ALTER COLUMN match_end SET NOT NULL;
