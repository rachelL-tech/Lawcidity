import os

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/citations",
)

OPENSEARCH_URL = os.environ.get("OPENSEARCH_URL", "https://localhost:9200")
OPENSEARCH_INDEX = os.environ.get("OPENSEARCH_INDEX", "decisions_v2")
