#!/usr/bin/env bash
# 初始化 source-target window OpenSearch 索引。
set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
OPENSEARCH_URL="${OPENSEARCH_URL%/}"
OPENSEARCH_SOURCE_TARGET_INDEX="${OPENSEARCH_SOURCE_TARGET_INDEX:-source_target_windows_v2}"

OPENSEARCH_NGRAM_MIN_GRAM=2
OPENSEARCH_NGRAM_MAX_GRAM=2

HEAD_CODE="$(
  curl -sS -o /dev/null -w "%{http_code}" "${OPENSEARCH_URL}/${OPENSEARCH_SOURCE_TARGET_INDEX}"
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

curl -sS -X PUT "${OPENSEARCH_URL}/${OPENSEARCH_SOURCE_TARGET_INDEX}" \
  -H "Content-Type: application/json" \
  -d "${INDEX_BODY}"
echo
echo "Index created: ${OPENSEARCH_SOURCE_TARGET_INDEX} (ngram ${OPENSEARCH_NGRAM_MIN_GRAM}-${OPENSEARCH_NGRAM_MAX_GRAM})"
