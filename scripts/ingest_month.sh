#!/usr/bin/env bash
# ingest_month.sh — 單月份匯入 + 法條抽取
#
# 用法：
#   ./scripts/ingest_month.sh <月份資料夾> [案件類型關鍵字]
#
# 範例：
#   ./scripts/ingest_month.sh /Users/rachel/Downloads/202512 民事
#   ./scripts/ingest_month.sh /Users/rachel/Downloads/202512        # 全案件類型

set -euo pipefail

BATCH_DIR="${1:-}"
KEYWORD="${2:-}"

if [[ -z "$BATCH_DIR" ]]; then
  echo "用法：$0 <月份資料夾> [案件類型關鍵字]"
  exit 1
fi

# 讀取 .env（若存在）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
[[ -f "$ROOT_DIR/.env" ]] && set -a && source "$ROOT_DIR/.env" && set +a

# ── 1. Ingest ────────────────────────────────────────────────────────
echo "[1/2] 匯入：$BATCH_DIR ${KEYWORD:+(keyword: $KEYWORD)}"
cd "$ROOT_DIR/etl"
python ingest_decisions.py --batch "$BATCH_DIR" $KEYWORD

# ── 2. 法條抽取 ──────────────────────────────────────────────────────
echo "[2/2] 法條抽取（--all）..."
python extract_statutes.py --all

echo ""
echo "完成：$BATCH_DIR"
