#!/usr/bin/env bash
# Creates the pg_otel SQL helpers in template1, so that databases created later
# get them, and in the initial database. Runs once, when the data directory is
# initialised.
set -euo pipefail

for db in template1 "$POSTGRES_DB"; do
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$db" \
    --file /usr/local/share/pg_otel/setup.sql
done
