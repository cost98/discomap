#!/bin/bash
set -e

echo "Waiting for database..."
until PGPASSWORD=${DB_PASSWORD} psql -h "${DB_HOST}" -U "${DB_USER}" -d "${DB_NAME}" -c '\q' 2>/dev/null; do
  echo "Postgres is unavailable - sleeping"
  sleep 1
done

echo "Running database migrations..."
alembic upgrade head

echo "Starting API server..."
exec uvicorn src.main:app --host 0.0.0.0 --port 8000
