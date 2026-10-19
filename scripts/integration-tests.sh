#!/usr/bin/env bash
# Runs the integration tests against PostgreSQL (with pg_otel built for it) and
# an OpenTelemetry collector, both in containers. The tests themselves run on
# the host.
#
#   scripts/integration-tests.sh [--keep] [cargo test args...]
#
# Environment:
#   PG_MAJOR          PostgreSQL major version (default 18)
#   PGPORT            host port for PostgreSQL (default 55432)
#   COLLECTOR_IMAGE   collector image
#   CONTAINER_ENGINE  podman or docker (default: whichever is installed)
set -euo pipefail

cd "$(dirname "$0")/.."

PG_MAJOR="${PG_MAJOR:-18}"
PGPORT="${PGPORT:-55432}"
COLLECTOR_IMAGE="${COLLECTOR_IMAGE:-docker.io/otel/opentelemetry-collector:0.163.1}"
ENGINE="${CONTAINER_ENGINE:-$(command -v podman || command -v docker || true)}"
[ -n "$ENGINE" ] || { echo "need podman or docker" >&2; exit 1; }

keep=0
if [ "${1:-}" = "--keep" ]; then keep=1; shift; fi

suffix="pg-otel-it-$$"
network="$suffix"
out_dir="$(mktemp -d)"
chmod 777 "$out_dir"
image="pg-otel-it:pg${PG_MAJOR}"

cleanup() {
  if [ "$keep" = 1 ]; then
    echo "Leaving containers running: $suffix-postgres, $suffix-collector (network $network, output $out_dir)"
    return
  fi
  "$ENGINE" rm -f "$suffix-postgres" "$suffix-collector" >/dev/null 2>&1 || true
  "$ENGINE" network rm "$network" >/dev/null 2>&1 || true
  rm -rf "$out_dir"
}
trap cleanup EXIT

dump_logs() {
  "$ENGINE" logs "$suffix-collector" 2>&1 | sed 's/^/collector: /' || true
  "$ENGINE" logs "$suffix-postgres" 2>&1 | sed 's/^/postgres: /' || true
}

"$ENGINE" build --build-arg "PG_MAJOR=$PG_MAJOR" -t "$image" -f tests/integration/Dockerfile .
"$ENGINE" network create "$network" >/dev/null

# --user 0 so that, with rootless podman, the output files belong to the caller.
"$ENGINE" run -d --name "$suffix-collector" --network "$network" --network-alias collector \
  --user 0 \
  -v "$PWD/tests/integration/collector.yaml:/etc/otelcol/config.yaml:ro,z" \
  -v "$out_dir:/out:z" \
  "$COLLECTOR_IMAGE" --config /etc/otelcol/config.yaml >/dev/null

# The tests configure pg_otel with ALTER SYSTEM, so do not pass pg_otel.* with -c:
# command line settings take precedence over it.
"$ENGINE" run -d --name "$suffix-postgres" --network "$network" \
  -p "127.0.0.1:$PGPORT:5432" \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  "$image" \
  -c shared_preload_libraries=pg_otel >/dev/null

for _ in $(seq 60); do
  "$ENGINE" exec "$suffix-postgres" pg_isready -U postgres -q && break
  sleep 1
done
"$ENGINE" exec "$suffix-postgres" pg_isready -U postgres -q || { dump_logs; exit 1; }

export PGHOST=127.0.0.1 PGPORT PGUSER=postgres PGDATABASE=postgres
export COLLECTOR_HOST=collector COLLECTOR_OUT_DIR="$out_dir"

if ! cargo test --test integration --no-default-features --features "pg${PG_MAJOR}" \
  -- --include-ignored "$@"; then
  dump_logs
  exit 1
fi
