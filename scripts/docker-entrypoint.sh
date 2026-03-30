#!/usr/bin/env bash
set -e

echo "Solar Model API — starting up"

# ---------------------------------------------------------------------------
# Wait for PostgreSQL
# ---------------------------------------------------------------------------
if [ -n "$DATABASE_URL" ]; then
    # Extract host and port from DATABASE_URL
    # Format: postgresql://user:pass@host:port/dbname
    DB_HOST=$(echo "$DATABASE_URL" | sed -n 's|.*@\([^:/]*\).*|\1|p')
    DB_PORT=$(echo "$DATABASE_URL" | sed -n 's|.*:\([0-9]*\)/.*|\1|p')
    DB_HOST=${DB_HOST:-localhost}
    DB_PORT=${DB_PORT:-5432}

    echo "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."
    retries=30
    until pg_isready -h "$DB_HOST" -p "$DB_PORT" -q 2>/dev/null || [ "$retries" -eq 0 ]; do
        retries=$((retries - 1))
        sleep 1
    done

    if [ "$retries" -eq 0 ]; then
        echo "WARNING: PostgreSQL not reachable after 30s — starting anyway"
    else
        echo "PostgreSQL is ready"
    fi

    # -----------------------------------------------------------------------
    # Run Alembic migrations
    # -----------------------------------------------------------------------
    echo "Running database migrations..."
    alembic upgrade head || echo "WARNING: Alembic migration failed — check DATABASE_URL and schema"
fi

# ---------------------------------------------------------------------------
# Start the API server
# ---------------------------------------------------------------------------
echo "Starting uvicorn on 0.0.0.0:8000"
exec uvicorn src.api.app:create_app --factory --host 0.0.0.0 --port 8000
