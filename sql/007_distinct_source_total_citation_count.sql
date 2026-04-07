-- Migration: normalize decisions/authorities.total_citation_count
-- 語意統一為：歷史上引用該 target 的 distinct source 數

WITH decision_counts AS (
    SELECT
        COALESCE(td.canonical_id, td.id) AS canonical_id,
        COUNT(DISTINCT c.source_id)::int AS total_citation_count
    FROM decisions td
    LEFT JOIN citations c ON c.target_id = td.id
    GROUP BY COALESCE(td.canonical_id, td.id)
)
UPDATE decisions d
SET total_citation_count = COALESCE(dc.total_citation_count, 0)
FROM decision_counts dc
WHERE COALESCE(d.canonical_id, d.id) = dc.canonical_id
  AND d.total_citation_count IS DISTINCT FROM COALESCE(dc.total_citation_count, 0);

UPDATE authorities a
SET total_citation_count = COALESCE(agg.total_citation_count, 0)
FROM (
    SELECT
        c.target_authority_id AS authority_id,
        COUNT(DISTINCT c.source_id)::int AS total_citation_count
    FROM citations c
    WHERE c.target_authority_id IS NOT NULL
    GROUP BY c.target_authority_id
) agg
WHERE a.id = agg.authority_id
  AND a.total_citation_count IS DISTINCT FROM COALESCE(agg.total_citation_count, 0);
