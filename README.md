# 台灣判決引用關係排行榜（MVP）

Week 1 目標：建立「高等法院引用最高法院判決」的 citation graph

---

## 快速啟動

### 1. 啟動 PostgreSQL

```bash
# 確保 Docker Desktop 已啟動
docker compose up -d

# 驗證 DB 是否啟動成功
docker compose ps
docker compose exec db psql -U postgres -d citations -c "\dt"
```

應該看到 5 張表：
- `court_units`
- `decisions`
- `citations`
- `decision_reason_statutes`
- `citation_snippet_statutes`

### 2. 安裝 Python 依賴

```bash
# 建議使用虛擬環境
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate  # Windows

# 安裝依賴
pip install -r requirements.txt
```

### 3. 匯入判決（你的學習重點）

```bash
# 測試：匯入高等法院民事判決
python etl/ingest_decisions.py /Users/rachel/Downloads/202511/臺灣高等法院民事
```

**TODO（你需要實作）**：
- [ ] `etl/ingest_decisions.py` 中的 `normalize_jcase()` 函式
- [ ] `etl/ingest_decisions.py` 中的 `parse_decision_date()` 函式（民國年轉西元年）

### 4. 驗證資料

```bash
# 連進 DB 查詢
docker compose exec db psql -U postgres -d citations

# 查看有多少判決
SELECT COUNT(*) FROM decisions;

# 查看法院列表
SELECT root_norm, level, COUNT(*) FROM court_units
JOIN decisions ON court_units.id = decisions.court_unit_id
GROUP BY root_norm, level;
```

---

## 專案結構

```
.
├── docker-compose.yml          # PostgreSQL 容器配置
├── sql/
│   └── 001_schema.sql          # DB schema（自動執行）
├── etl/
│   ├── simple_court_mapping.py # 簡易庭對應表
│   ├── court_parser.py         # 法院名稱解析
│   └── ingest_decisions.py     # 判決匯入腳本（你的學習重點）
├── data/
│   └── simple_court_mapping.csv
├── requirements.txt
└── CLAUDE.md                   # 完整專案規格
```

---

## Week 1 Checklist

- [x] Docker Compose + PostgreSQL
- [x] Schema 建立
- [ ] 簡易庭對應表（35 個）
- [ ] 實作 `normalize_jcase()` 和 `parse_decision_date()`
- [ ] 測試匯入高等法院判決
- [ ] 抽取 citation（`etl/extract_citations.py`）
- [ ] 抽取法條（`etl/extract_statutes.py`）
- [ ] 驗證引用網路是否正確

---

## 常用指令

```bash
# 停止 DB
docker compose down

# 停止並刪除資料（重新開始）
docker compose down -v

# 查看 DB logs
docker compose logs db

# 進入 DB shell
docker compose exec db psql -U postgres -d citations
```

---

## 下一步

完成判決匯入後，開始寫 `etl/extract_citations.py`（從 `full_text` 抽取引用）。