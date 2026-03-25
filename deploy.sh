#!/usr/bin/env bash
set -euo pipefail

echo "=== Lawcidity Deploy ==="

if [ ! -f .env ]; then
    echo "ERROR: .env not found. Create .env and fill in values."
    exit 1
fi

docker compose -f docker-compose.prod.yml --env-file .env up -d --build

echo ""
docker compose -f docker-compose.prod.yml ps
echo ""
echo "Done. OpenSearch may take 30-60s to initialize on first run."
