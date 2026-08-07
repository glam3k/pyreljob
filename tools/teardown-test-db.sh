#!/usr/bin/env bash
set -euo pipefail

# Stop and remove the PostgreSQL test database and its volume.

cd "$(dirname "$0")/.."

echo "Stopping PostgreSQL test database..."
docker compose down -v

echo "Test database torn down."
