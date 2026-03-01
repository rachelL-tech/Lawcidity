#!/usr/bin/env bash
# 用途：初始化 OpenSearch 索引（若不存在才建立），供本機/EC2 部署後重建使用。
#
# Analyzer 決策（2026-03）：
#   目前：OPENSEARCH_ANALYZER=ik_smart（index + search 相同）
#   原因：IK 內建詞典未收錄法律複合詞（損害賠償、侵權行為等），ik_smart/ik_max_word 輸出
#         相同（字元級 token），改 analyzer 目前零效益且須重新 sync。
#   未來（加自訂法律詞典時）：改為 index=ik_max_word + search=ik_smart，同步時一次重建。
#   搜尋策略：使用 match_phrase（字元連續等同 ILIKE），不使用 match+operator:and。
set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-https://localhost:9200}"
OPENSEARCH_URL="${OPENSEARCH_URL%/}"
OPENSEARCH_INDEX="${OPENSEARCH_INDEX:-decisions_v1}"
OPENSEARCH_USERNAME="${OPENSEARCH_USERNAME:-admin}"
OPENSEARCH_PASSWORD="${OPENSEARCH_PASSWORD:-}"
OPENSEARCH_VERIFY_CERTS="${OPENSEARCH_VERIFY_CERTS:-false}"
OPENSEARCH_ANALYZER="${OPENSEARCH_ANALYZER:-ik_smart}"
OPENSEARCH_SEARCH_ANALYZER="${OPENSEARCH_SEARCH_ANALYZER:-$OPENSEARCH_ANALYZER}"

if [[ -z "${OPENSEARCH_PASSWORD}" ]] && command -v docker >/dev/null 2>&1; then
  OPENSEARCH_PASSWORD="$(docker compose exec -T opensearch printenv OPENSEARCH_INITIAL_ADMIN_PASSWORD 2>/dev/null || true)"
fi

if [[ -z "${OPENSEARCH_PASSWORD}" ]]; then
  echo "ERROR: 請設定 OPENSEARCH_PASSWORD（或先啟動 opensearch 容器供自動讀取密碼）" >&2
  exit 1
fi

CURL_EXTRA=()
if [[ "${OPENSEARCH_VERIFY_CERTS}" != "true" ]]; then
  CURL_EXTRA+=("-k")
fi

AUTH=(-u "${OPENSEARCH_USERNAME}:${OPENSEARCH_PASSWORD}")
HEAD_CODE="$(curl "${CURL_EXTRA[@]}" -sS -o /dev/null -w "%{http_code}" "${AUTH[@]}" \
  "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}")"

if [[ "${HEAD_CODE}" == "200" ]]; then
  echo "Index already exists: ${OPENSEARCH_INDEX}"
  exit 0
fi

if [[ "${HEAD_CODE}" != "404" ]]; then
  echo "ERROR: 無法檢查 index 狀態，HTTP=${HEAD_CODE}" >&2
  exit 1
fi

INDEX_BODY="$(cat <<JSON
{
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "source_id": { "type": "long" },
        "case_type": { "type": "keyword" },
        "clean_text": {
          "type": "text",
          "analyzer": "${OPENSEARCH_ANALYZER}",
          "search_analyzer": "${OPENSEARCH_SEARCH_ANALYZER}"
        },
        "statutes": {
          "type": "nested",
          "properties": {
            "law": { "type": "keyword" },
            "article_raw": { "type": "keyword" },
            "sub_ref": { "type": "keyword" }
          }
        }
      }
    }
  }
}
JSON
)"

curl "${CURL_EXTRA[@]}" -sS "${AUTH[@]}" -X PUT "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}" \
  -H "Content-Type: application/json" \
  -d "${INDEX_BODY}"
echo
echo "Index created: ${OPENSEARCH_INDEX}"
