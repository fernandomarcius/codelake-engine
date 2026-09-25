#!/usr/bin/env bash
# Prints the harness image name, building it first when it is missing. The
# tag is a hash of everything that goes into the image, so an edit rebuilds it.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -n "${CODELAKE_SHADOW_IMAGE:-}" ]; then echo "$CODELAKE_SHADOW_IMAGE"; exit 0; fi
TAG="$(cat "$DIR/Dockerfile" "$DIR/requirements.txt" "$DIR"/codelake_shadow/*.py "$DIR"/codelake_shadow/*.json "$DIR"/tests/*.py \
  | { sha256sum 2>/dev/null || shasum -a 256; } | cut -c1-12)"
IMAGE="codelake-shadow:$TAG"
if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "building $IMAGE" >&2
  docker build -q -t "$IMAGE" "$DIR" >/dev/null
fi
echo "$IMAGE"
