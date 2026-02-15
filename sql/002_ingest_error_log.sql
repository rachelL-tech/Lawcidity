-- =========================
-- ingest_error_log：記錄匯入時的 A/B/D 類錯誤
-- A = JSON 讀取失敗
-- B = 欄位缺失 / 資料異常（ingest_decision 失敗）
-- D = Citation 抽取 / 寫入失敗
-- =========================
CREATE TABLE IF NOT EXISTS ingest_error_log (
  id           BIGSERIAL PRIMARY KEY,
  logged_at    TIMESTAMPTZ DEFAULT now(),

  folder_name  TEXT NOT NULL,  -- 如 202511/臺灣高等法院民事
  file_name    TEXT NOT NULL,  -- 如 TPDHV,114,重訴,1234.json
  error_type   TEXT NOT NULL,  -- 'A' | 'B' | 'D'
  error_msg    TEXT,           -- 錯誤訊息

  resolved     BOOLEAN DEFAULT false,
  resolved_at  TIMESTAMPTZ
);

CREATE INDEX ingest_error_log_folder_idx   ON ingest_error_log(folder_name);
CREATE INDEX ingest_error_log_resolved_idx ON ingest_error_log(resolved);
