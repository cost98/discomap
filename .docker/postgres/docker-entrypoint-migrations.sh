#!/bin/bash
# Docker entrypoint script to run Alembic migrations
# Executed after PostgreSQL is initialized

set -e

echo "======================================"
echo "Running Alembic migrations..."
echo "======================================"

# Database is already running during init scripts
# No need to wait - just run migrations

# Set database connection for Alembic
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME="$POSTGRES_DB"
export DB_USER="$POSTGRES_USER"
export DB_PASSWORD="$POSTGRES_PASSWORD"

# Navigate to app directory
cd /app

# Run Alembic migrations
echo "Running: alembic upgrade head"
python3 -m alembic upgrade head

if [ $? -eq 0 ]; then
    echo "✓ Alembic migrations completed successfully"
else
    echo "✗ Alembic migrations failed"
    exit 1
fi

echo "======================================"
echo "Database schema is ready!"
echo "======================================"
