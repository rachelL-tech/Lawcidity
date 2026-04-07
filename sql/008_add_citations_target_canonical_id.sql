ALTER TABLE citations
ADD COLUMN IF NOT EXISTS target_canonical_id BIGINT REFERENCES decisions(id);

UPDATE citations c
SET target_canonical_id = d.canonical_id
FROM decisions d
WHERE c.target_id = d.id
  AND c.target_id IS NOT NULL
  AND c.target_canonical_id IS DISTINCT FROM d.canonical_id;

CREATE INDEX IF NOT EXISTS citations_target_canonical_source_idx
ON citations(target_canonical_id, source_id)
WHERE target_canonical_id IS NOT NULL;
