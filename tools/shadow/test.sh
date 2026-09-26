#!/usr/bin/env bash
# Unit tests of the shadow-run harness, in its image (built when missing).
# CI runs this (job `shadow-harness`); locally, run it under the heavy lock:
#   heavy.sh tools/shadow/test.sh [unittest args]
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="$("$DIR/image.sh")"
docker run --rm --entrypoint python "$IMAGE" -m unittest discover -s tests "$@"
