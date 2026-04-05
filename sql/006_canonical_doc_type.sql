-- Migration: decisions.canonical_doc_type
-- 對現有 DB 執行（本機 / RDS 都可重跑）
-- 更新日期：2026-04-06

ALTER TABLE decisions
  ADD COLUMN IF NOT EXISTS canonical_doc_type TEXT;

WITH canonical_doc_type_agg AS (
  SELECT
    COALESCE(canonical_id, id) AS canonical_group_id,
    CASE
      WHEN COUNT(DISTINCT doc_type) FILTER (WHERE doc_type IS NOT NULL) > 1 THEN '裁判'
      ELSE MAX(doc_type)
    END AS canonical_doc_type
  FROM decisions
  GROUP BY COALESCE(canonical_id, id)
)
UPDATE decisions d
SET canonical_doc_type = agg.canonical_doc_type
FROM canonical_doc_type_agg agg
WHERE COALESCE(d.canonical_id, d.id) = agg.canonical_group_id
  AND d.canonical_doc_type IS DISTINCT FROM agg.canonical_doc_type;
