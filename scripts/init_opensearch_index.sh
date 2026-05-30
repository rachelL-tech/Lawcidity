#!/usr/bin/env bash
# 初始化 OpenSearch ngram 索引（若不存在才建立）。
# 預設建立 decisions_v3，並使用 bigram（min_gram=max_gram=2）。
set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-http://localhost:9200}"
OPENSEARCH_URL="${OPENSEARCH_URL%/}"
OPENSEARCH_INDEX="${OPENSEARCH_INDEX:-decisions_v3}"

OPENSEARCH_NGRAM_MIN_GRAM=2
OPENSEARCH_NGRAM_MAX_GRAM=2

HEAD_CODE="$(
  curl -sS -o /dev/null -w "%{http_code}" "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}"
)"

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
    "_source": {
      "excludes": ["clean_text"]
    },
    "properties": {
      "source_id": { "type": "long" },
      "case_type": { "type": "keyword" },
      "clean_text": {
        "type": "text",
        "analyzer": "zh_ngram",
        "norms": false
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

curl --fail-with-body -sS -X PUT "${OPENSEARCH_URL}/${OPENSEARCH_INDEX}" \
  -H "Content-Type: application/json" \
  -d "${INDEX_BODY}"
echo
echo "Index created: ${OPENSEARCH_INDEX} (ngram ${OPENSEARCH_NGRAM_MIN_GRAM}-${OPENSEARCH_NGRAM_MAX_GRAM})"
