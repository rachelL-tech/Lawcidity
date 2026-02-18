-- Task 1: 加入 case_type 欄位（民事/刑事/行政/憲法）
-- 從資料夾後綴解析，少年及家事法院的 case_type = 民事

ALTER TABLE decisions ADD COLUMN IF NOT EXISTS case_type TEXT;

CREATE INDEX IF NOT EXISTS decisions_case_type_idx ON decisions(case_type);
