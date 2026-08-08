#!/bin/sh
# Applies storage/migrations/*.sql in filename order, tracked in schema_migrations.
# Runs via psql (autocommit per statement) because continuous aggregates cannot
# be created inside a transaction block.
set -eu

DB_HOST="${DB_HOST:-timescaledb}"
DB_PORT="${DB_PORT:-5432}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-/storage/migrations}"

export PGPASSWORD="$POSTGRES_PASSWORD"
PSQL="psql -h $DB_HOST -p $DB_PORT -U $POSTGRES_USER -d $POSTGRES_DB -v ON_ERROR_STOP=1 -q"

echo "Waiting for TimescaleDB at $DB_HOST:$DB_PORT ..."
i=0
until $PSQL -c "SELECT 1" >/dev/null 2>&1; do
    i=$((i + 1))
    [ "$i" -ge 60 ] && echo "Database not reachable after 60 attempts" && exit 1
    sleep 2
done

$PSQL -c "CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   text PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
)"

for f in "$MIGRATIONS_DIR"/*.sql; do
    name=$(basename "$f")
    applied=$($PSQL -tAc "SELECT 1 FROM schema_migrations WHERE filename = '$name'")
    if [ "$applied" = "1" ]; then
        echo "skip  $name"
    else
        echo "apply $name"
        $PSQL -f "$f"
        $PSQL -c "INSERT INTO schema_migrations (filename) VALUES ('$name')"
    fi
done

echo "Migrations complete."
