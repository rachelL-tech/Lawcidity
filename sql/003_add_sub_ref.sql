-- 新增 sub_ref 欄位：記錄項/款/目 qualifier（如「第1項第1款」「前段」）
-- 執行前請確認 decision_reason_statutes / citation_snippet_statutes 已清空（或接受重跑）

ALTER TABLE decision_reason_statutes
  ADD COLUMN IF NOT EXISTS sub_ref TEXT NOT NULL DEFAULT '';

ALTER TABLE citation_snippet_statutes
  ADD COLUMN IF NOT EXISTS sub_ref TEXT NOT NULL DEFAULT '';

-- 更新 unique index：加入 sub_ref
DROP INDEX IF EXISTS drs_uniq;
CREATE UNIQUE INDEX drs_uniq
  ON decision_reason_statutes(decision_id, law, article_raw, sub_ref);

DROP INDEX IF EXISTS css_uniq;
CREATE UNIQUE INDEX css_uniq
  ON citation_snippet_statutes(citation_id, law, article_raw, sub_ref);
