#!/usr/bin/env bash
set -euo pipefail

# Start the PostgreSQL test database via Docker Compose.
# SQLite tests require no database — they use ephemeral tmp_path files.

cd "$(dirname "$0")/.."

echo "Starting PostgreSQL test database..."
docker compose up -d postgres

echo "Waiting for PostgreSQL to be ready..."
until docker compose exec postgres pg_isready -U pyreljob -d pyreljob > /dev/null 2>&1; do
  sleep 1
done

echo "PostgreSQL is ready on localhost:5433"
echo "  Connection: postgresql+psycopg://pyreljob:pyreljob@localhost:5433/pyreljob"
