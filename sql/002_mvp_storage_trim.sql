-- 一次性 migration（現有 DB 用）
-- 目的：降低儲存體積，先聚焦 OpenSearch 主搜尋流程

BEGIN;

-- 現階段先移除不必要查詢索引（可日後補建）
DROP INDEX IF EXISTS decisions_ref_key_idx;
DROP INDEX IF EXISTS decisions_root_year_idx;
DROP INDEX IF EXISTS decisions_date_idx;
DROP INDEX IF EXISTS decisions_cleantext_trgm;
DROP INDEX IF EXISTS decisions_title_trgm;
DROP INDEX IF EXISTS court_units_root_idx;
DROP INDEX IF EXISTS court_units_county_district_idx;
DROP INDEX IF EXISTS court_units_geo_idx;
DROP INDEX IF EXISTS authorities_doctype_idx;
DROP INDEX IF EXISTS authorities_root_idx;
DROP INDEX IF EXISTS css_law_article_idx;
DROP INDEX IF EXISTS ingest_error_log_resolved_idx;

-- 移除大欄位（原始檔已在 S3 保留）
ALTER TABLE decisions DROP COLUMN IF EXISTS full_text;
ALTER TABLE decisions DROP COLUMN IF EXISTS raw;

COMMIT;

-- 可選：migration 後做統計更新
ANALYZE decisions;
