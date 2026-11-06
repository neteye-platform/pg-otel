#!/usr/bin/env bash
# Entrypoint of the pg_otel image: starts PostgreSQL with pg_otel preloaded
# through the official image's entrypoint. Other commands pass through.
set -euo pipefail

# Same rule as the official entrypoint: leading options are for postgres.
first="${1:-}"
if [ "${first:0:1}" = '-' ]; then
  set -- postgres "$@"
fi

# Placed first so that a user's own -c shared_preload_libraries wins. Not for
# help and version options: postgres only handles them before refusing to run
# as root when they are argv[1], and the official entrypoint runs these as root.
if [ "${1:-}" = 'postgres' ]; then
  inject=1
  for arg; do
    case "$arg" in
      -'?' | --help | --describe-config | -V | --version) inject=0 ;;
    esac
  done
  if [ "$inject" = 1 ]; then
    shift
    set -- postgres -c shared_preload_libraries=pg_otel "$@"
  fi
fi

exec docker-entrypoint.sh "$@"
