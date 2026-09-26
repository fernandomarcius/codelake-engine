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
  # One target volume per checkout: cargo decides freshness by mtime, so two
  # worktrees sharing a target (both mounted at /work) could reuse each other's
  # binaries and test code that is not on disk.
  CACHE_FLAGS=(-v codelake-cargo:/usr/local/cargo/registry -v "${CODELAKE_TARGET_VOLUME:-codelake-target}":/work/target)
fi

# Compilation cache (sccache), shared by every checkout on this machine. Unlike
# the target, it is safe to share: an entry is keyed by the hash of the
# compiler, its arguments and the content of every input file (never mtime), so
# two worktrees that changed the same file differently get different entries.
# On by default locally; off in CI (the target is cached there) unless
# CODELAKE_SCCACHE=1. CODELAKE_SCCACHE=0 turns it off. docs/gates.md.
SCCACHE_FLAGS=()
if [ -n "${CODELAKE_CACHE_DIR:-}" ]; then SCCACHE_DEFAULT=0; else SCCACHE_DEFAULT=1; fi
if [ "${CODELAKE_SCCACHE:-$SCCACHE_DEFAULT}" = 1 ]; then
  if [ -n "${CODELAKE_CACHE_DIR:-}" ]; then
    mkdir -p "$CODELAKE_CACHE_DIR/sccache"
    SCCACHE_FLAGS=(-v "$CODELAKE_CACHE_DIR/sccache":/sccache)
  else
    SCCACHE_FLAGS=(-v "${CODELAKE_SCCACHE_VOLUME:-codelake-sccache}":/sccache)
  fi
  SCCACHE_FLAGS+=(-e CODELAKE_SCCACHE=1 -e SCCACHE_DIR=/sccache
                  -e SCCACHE_CACHE_SIZE="${CODELAKE_SCCACHE_SIZE:-20G}")
fi

ENV_FLAGS=()
for var in MUTANTS_BASE GITHUB_ACTIONS CI; do
  [ -n "${!var:-}" ] && ENV_FLAGS+=(-e "$var")
done

# Wraps rustc in sccache when asked and the image has it; an image built before
# sccache was added keeps working, uncached.
IN_CONTAINER='if [ "${CODELAKE_SCCACHE:-0}" = 1 ]; then
  if command -v sccache >/dev/null 2>&1; then export RUSTC_WRAPPER=sccache
  else echo "dev.sh: no sccache in this image, building uncached (docs/gates.md: rebuild the image)" >&2; fi
fi
exec "$@"'

# Docker socket is forwarded so integration tests can start Testcontainers.
exec docker run --rm ${TTY_FLAGS[@]+"${TTY_FLAGS[@]}"} \
  -v "$ROOT":/work \
  "${CACHE_FLAGS[@]}" \
  ${SCCACHE_FLAGS[@]+"${SCCACHE_FLAGS[@]}"} \
  ${ENV_FLAGS[@]+"${ENV_FLAGS[@]}"} \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-auto}" \
  -e TESTCONTAINERS_HOST_OVERRIDE="${TESTCONTAINERS_HOST_OVERRIDE:-host.docker.internal}" \
  --add-host=host.docker.internal:host-gateway \
  -w /work "$IMAGE" bash -c "$IN_CONTAINER" dev.sh "$@"
