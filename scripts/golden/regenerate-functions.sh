#!/usr/bin/env bash
# Regenerates the Trino 431 golden fixtures of the function library
# (crates/codelake-core/tests/fixtures/trino431-functions/*.json).
#
# Every case's SQL runs on a real trinodb/trino:431 through the client REST
# protocol (session zone UTC, parametric datetime capability) and the result
# (types + rows, or the error name) is written back into the fixture file.
# The gate test compares CodeLake with these files and needs no Trino.
#
#   scripts/golden/regenerate-functions.sh              # starts and removes its own container
#   TRINO_GOLDEN_URL=http://127.0.0.1:18431 scripts/golden/regenerate-functions.sh
#   scripts/golden/regenerate-functions.sh strings.json # only some files
#
# Requires docker and python3 on the host. Review the diff before committing:
# a changed expectation means CodeLake or Trino semantics moved.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FIXTURES="$ROOT/crates/codelake-core/tests/fixtures/trino431-functions"
IMAGE="trinodb/trino:431"
NAME="${TRINO_GOLDEN_CONTAINER:-codelake-golden-trino}"
PORT="${TRINO_GOLDEN_PORT:-18431}"
URL="${TRINO_GOLDEN_URL:-}"

if [ -z "$URL" ]; then
  URL="http://127.0.0.1:${PORT}"
  # The image sizes the heap at 80% of the container memory, which leaves no
  # room for code cache and metaspace under a 2 GB limit (OOM kill). Pin it.
  JVM_CONFIG="$(mktemp)"
  docker run --rm --entrypoint cat "$IMAGE" /etc/trino/jvm.config \
    | grep -v RAMPercentage > "$JVM_CONFIG"
  printf '%s\n' '-Xms1G' '-Xmx1G' >> "$JVM_CONFIG"
  chmod 644 "$JVM_CONFIG"
  docker run -d --rm --name "$NAME" --memory=2g -p "127.0.0.1:${PORT}:8080" \
    -v "$JVM_CONFIG:/etc/trino/jvm.config:ro" "$IMAGE" >/dev/null
  trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true; rm -f "$JVM_CONFIG"' EXIT
fi

echo "waiting for Trino at $URL"
ready=0
for _ in $(seq 1 120); do
  if curl -fsS "$URL/v1/info" 2>/dev/null | grep -q '"starting":false'; then ready=1; break; fi
  sleep 2
done
if [ "$ready" -ne 1 ]; then echo "Trino did not start at $URL" >&2; exit 1; fi

files=()
if [ "$#" -gt 0 ]; then
  for f in "$@"; do files+=("$FIXTURES/$(basename "$f")"); done
else
  files=("$FIXTURES"/*.json)
fi

python3 "$ROOT/scripts/golden/trino_golden.py" "$URL" "${files[@]}"
