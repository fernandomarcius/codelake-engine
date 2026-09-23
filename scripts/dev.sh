#!/usr/bin/env bash
# Runs any command inside the pinned toolbelt image (docker/dev.Dockerfile).
# Usage: scripts/dev.sh cargo test --workspace
#        scripts/dev.sh cargo xtask gates
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
IMAGE="${CODELAKE_DEV_IMAGE:-codelake-dev:local}"

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  docker build -f "$ROOT/docker/dev.Dockerfile" -t "$IMAGE" "$ROOT/docker"
fi

# Docker Desktop's file sharing caches file attributes for about a second; a
# build started right after an edit can read a stale size (truncated file).
sleep 1

TTY_FLAGS=()
[ -t 1 ] && TTY_FLAGS=(-t)

# Caches: named Docker volumes locally; host directories in CI (CODELAKE_CACHE_DIR)
# so actions/cache can persist them between runs.
if [ -n "${CODELAKE_CACHE_DIR:-}" ]; then
  mkdir -p "$CODELAKE_CACHE_DIR/registry" "$CODELAKE_CACHE_DIR/target"
  CACHE_FLAGS=(-v "$CODELAKE_CACHE_DIR/registry":/usr/local/cargo/registry -v "$CODELAKE_CACHE_DIR/target":/work/target)
else
  CACHE_FLAGS=(-v codelake-cargo:/usr/local/cargo/registry -v codelake-target:/work/target)
fi

ENV_FLAGS=()
for var in MUTANTS_BASE GITHUB_ACTIONS CI; do
  [ -n "${!var:-}" ] && ENV_FLAGS+=(-e "$var")
done

# Docker socket is forwarded so integration tests can start Testcontainers.
exec docker run --rm ${TTY_FLAGS[@]+"${TTY_FLAGS[@]}"} \
  -v "$ROOT":/work \
  "${CACHE_FLAGS[@]}" \
  ${ENV_FLAGS[@]+"${ENV_FLAGS[@]}"} \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-auto}" \
  -e TESTCONTAINERS_HOST_OVERRIDE="${TESTCONTAINERS_HOST_OVERRIDE:-host.docker.internal}" \
  --add-host=host.docker.internal:host-gateway \
  -w /work "$IMAGE" "$@"
