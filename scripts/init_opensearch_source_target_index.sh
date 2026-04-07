#!/usr/bin/env bash
# 初始化 source-target window OpenSearch 索引。
set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-https://localhost:9200}"
OPENSEARCH_URL="${OPENSEARCH_URL%/}"
OPENSEARCH_SOURCE_TARGET_INDEX="${OPENSEARCH_SOURCE_TARGET_INDEX:-source_target_windows_v2}"
OPENSEARCH_USERNAME="${OPENSEARCH_USERNAME:-admin}"
OPENSEARCH_PASSWORD="${OPENSEARCH_PASSWORD:-}"
OPENSEARCH_VERIFY_CERTS="${OPENSEARCH_VERIFY_CERTS:-false}"

OPENSEARCH_NGRAM_MIN_GRAM="${OPENSEARCH_NGRAM_MIN_GRAM:-2}"
OPENSEARCH_NGRAM_MAX_GRAM="${OPENSEARCH_NGRAM_MAX_GRAM:-2}"

if [[ -z "${OPENSEARCH_PASSWORD}" ]] && command -v docker >/dev/null 2>&1; then
  OPENSEARCH_PASSWORD="$(
    docker compose exec -T opensearch printenv OPENSEARCH_INITIAL_ADMIN_PASSWORD 2>/dev/null || true
  )"
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
HEAD_CODE="$(
  curl "${CURL_EXTRA[@]}" -sS -o /dev/null -w "%{http_code}" "${AUTH[@]}" \
    "${OPENSEARCH_URL}/${OPENSEARCH_SOURCE_TARGET_INDEX}"
)"

if [[ "${HEAD_CODE}" == "200" ]]; then
  echo "Index already exists: ${OPENSEARCH_SOURCE_TARGET_INDEX}"
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
    "number_of_replicas": 0,
    "analysis": {
      "tokenizer": {
        "zh_ngram_tokenizer": {
          "type": "ngram",
          "min_gram": ${OPENSEARCH_NGRAM_MIN_GRAM},
          "max_gram": ${OPENSEARCH_NGRAM_MAX_GRAM},
          "token_chars": ["letter", "digit"]
        }
      },
      "analyzer": {
        "zh_ngram": {
          "type": "custom",
          "tokenizer": "zh_ngram_tokenizer"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "source_id": { "type": "long" },
      "target_id": { "type": "long" },
      "target_authority_id": { "type": "long" },
      "target_type": { "type": "keyword" },
      "target_uid": { "type": "keyword" },
      "case_type": { "type": "keyword" },
      "merged_citation_count": { "type": "integer" },
      "window_text_snippet": {
        "type": "text",
        "analyzer": "zh_ngram"
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
JSON
)"

curl "${CURL_EXTRA[@]}" -sS "${AUTH[@]}" -X PUT "${OPENSEARCH_URL}/${OPENSEARCH_SOURCE_TARGET_INDEX}" \
  -H "Content-Type: application/json" \
  -d "${INDEX_BODY}"
echo
echo "Index created: ${OPENSEARCH_SOURCE_TARGET_INDEX} (ngram ${OPENSEARCH_NGRAM_MIN_GRAM}-${OPENSEARCH_NGRAM_MAX_GRAM})"
