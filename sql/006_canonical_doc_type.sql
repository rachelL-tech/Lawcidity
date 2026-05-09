-- Migration: decisions.canonical_doc_type
-- 對現有 DB 執行（本機 / RDS 都可重跑）
-- 更新日期：2026-05-10
--
-- 語意：每個 canonical group 給一個展示用 doc_type（不為 NULL）
--   多種具體 doc_type 混合 → '裁判'
--   單一具體 doc_type      → 該 doc_type
--   全 NULL placeholder + 有判例 citation → '判例'
--   全 NULL placeholder + 無判例 citation（含孤兒） → '裁判'（兜底）

ALTER TABLE decisions
  ADD COLUMN IF NOT EXISTS canonical_doc_type TEXT;

WITH
all_groups AS (
  SELECT DISTINCT COALESCE(canonical_id, id) AS gid FROM decisions
),
all_doc_types AS (
  -- decisions 自己的 doc_type
  SELECT COALESCE(canonical_id, id) AS gid, doc_type AS dt
  FROM decisions
  WHERE doc_type IS NOT NULL

  UNION ALL

  -- citations 的 target_doc_type，只取「判例」這個信號
  SELECT target_canonical_id AS gid, target_doc_type AS dt
  FROM citations
  WHERE target_canonical_id IS NOT NULL
    AND target_doc_type = '判例'
),
agg AS (
  SELECT g.gid,
    CASE
      WHEN COUNT(DISTINCT a.dt) FILTER (WHERE a.dt IS NOT NULL) > 1 THEN '裁判'
      WHEN MAX(a.dt) IS NOT NULL THEN MAX(a.dt)
      ELSE '裁判'
    END AS canonical_doc_type
  FROM all_groups g
  LEFT JOIN all_doc_types a ON a.gid = g.gid
  GROUP BY g.gid
)
UPDATE decisions d
SET canonical_doc_type = agg.canonical_doc_type
FROM agg
WHERE COALESCE(d.canonical_id, d.id) = agg.gid
  AND d.canonical_doc_type IS DISTINCT FROM agg.canonical_doc_type;
