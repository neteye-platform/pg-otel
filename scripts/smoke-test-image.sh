#!/usr/bin/env bash
# Starts the image and checks that pg_otel is preloaded and its SQL functions
# were installed at initialisation.
#
#   scripts/smoke-test-image.sh IMAGE
#
# Environment:
#   CONTAINER_ENGINE  podman or docker (default: whichever is installed)
set -euo pipefail

image="${1:?usage: $0 IMAGE}"
ENGINE="${CONTAINER_ENGINE:-$(command -v podman || command -v docker || true)}"
[ -n "$ENGINE" ] || { echo "need podman or docker" >&2; exit 1; }

cid="$("$ENGINE" run -d -e POSTGRES_HOST_AUTH_METHOD=trust "$image")"
trap '"$ENGINE" rm -f "$cid" >/dev/null' EXIT

fail() {
  echo "smoke test failed: $1" >&2
  "$ENGINE" logs "$cid" >&2
  exit 1
}
running() {
  [ "$("$ENGINE" inspect -f '{{.State.Running}}' "$cid")" = true ]
}

# The official entrypoint logs this line once the temporary init server has
# stopped and before the final server starts, so wait for it first, then for
# the final server to accept connections.
init_done=false
for _ in $(seq 60); do
  running || fail "container exited during initialisation"
  if "$ENGINE" logs "$cid" 2>&1 | grep -q 'PostgreSQL init process complete'; then
    init_done=true
    break
  fi
  sleep 2
done
"$init_done" || fail "initialisation did not finish"

ready=false
for _ in $(seq 60); do
  running || fail "container exited after initialisation"
  if "$ENGINE" exec "$cid" pg_isready -U postgres -q; then
    ready=true
    break
  fi
  sleep 2
done
"$ready" || fail "PostgreSQL did not become ready"

preload="$("$ENGINE" exec "$cid" psql -U postgres -tAc 'SHOW shared_preload_libraries')" \
  || fail "SHOW shared_preload_libraries failed"
case "$preload" in
  *pg_otel*) ;;
  *) fail "pg_otel is not preloaded: '$preload'" ;;
esac
"$ENGINE" exec "$cid" psql -U postgres -c 'SELECT pg_otel_dropped_spans()' \
  || fail "pg_otel_dropped_spans() failed"
echo "smoke test passed: $image"
