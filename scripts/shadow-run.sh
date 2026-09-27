#!/usr/bin/env bash
# Shadow run: the same dbt project against a reference engine and a candidate
# engine, cycle after cycle, every output table compared. See docs/shadow-run.md.
#
#   scripts/shadow-run.sh <config.env> [run]              # SHADOW_CYCLES cycles
#   scripts/shadow-run.sh <config.env> compare --reference-warehouse … --candidate-warehouse … --table s.t
#   scripts/shadow-run.sh <config.env> summary            # rebuild the cumulative report
#   scripts/shadow-run.sh <config.env> test               # the harness's unit tests
#
# <config.env>: KEY=value lines (no quotes, no shell expansion), SHADOW_*
# variables only. Secrets never go in it: list the variables the dbt profile
# reads in SHADOW_PASS_ENV and export them in the calling environment.
#
# The harness runs in its own image (tools/shadow/Dockerfile: dbt-trino and
# DuckDB pinned by hash). Local paths of the configuration are mounted at the
# same path inside the container.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHADOW="$ROOT/tools/shadow"

[ $# -ge 1 ] || { sed -n '2,15p' "$0" >&2; exit 2; }
CONFIG="$1"; shift
COMMAND="${1:-run}"; [ $# -gt 0 ] && shift
[ -f "$CONFIG" ] || { echo "shadow-run: config file $CONFIG not found" >&2; exit 2; }

# Read SHADOW_* lines without evaluating them into an env file for the
# harness container (mode 0600: a profile variable could end up here by
# mistake). Works with the bash 3.2 macOS ships.
env_file="$(mktemp)"; trap 'rm -f "$env_file"' EXIT
chmod 600 "$env_file"
while IFS= read -r line || [ -n "$line" ]; do
  case "$line" in ''|'#'*) continue ;; esac
  key="${line%%=*}"
  if [[ ! "$key" =~ ^SHADOW_[A-Z0-9_]+$ ]] || [ "$key" = "$line" ]; then
    echo "shadow-run: $CONFIG: expected SHADOW_NAME=value lines (got a line starting with ${key:0:20})" >&2
    exit 2
  fi
  printf '%s\n' "$line" >> "$env_file"
done < "$CONFIG"
# Last value of a key (as docker --env-file reads it), or the default.
cfg() {
  local v
  v="$(grep "^$1=" "$env_file" | tail -n 1 | cut -d= -f2- || true)"
  printf '%s' "${v:-${2:-}}"
}

IMAGE="$("$SHADOW/image.sh")"

args=(--rm --user "$(id -u):$(id -g)")
[ -t 1 ] && args+=(-t)

# The configuration, as environment variables of the harness.
args+=(--env-file "$env_file")

# Variables the dbt profiles read (passwords, tokens): passed by name, their
# values taken from this environment and never written anywhere.
IFS=',' read -r -a pass <<< "$(cfg SHADOW_PASS_ENV)"
for var in ${pass[@]+"${pass[@]}"}; do
  var="$(echo "$var" | tr -d ' ')"
  [ -z "$var" ] && continue
  if [ -z "${!var:-}" ]; then echo "shadow-run: $var (SHADOW_PASS_ENV) is not set" >&2; exit 2; fi
  args+=(-e "$var")
done

mount_dir() { # path mode
  local path="$1" mode="$2"
  case "$path" in /*) ;; *) return 0 ;; esac
  if [ "$mode" = rw ]; then mkdir -p "$path"; fi
  [ -e "$path" ] && args+=(-v "$path:$path:$mode")
  return 0
}
if [ "$COMMAND" != test ]; then
  mount_dir "$(cfg SHADOW_DBT_PROJECT_DIR)" ro
  mount_dir "$(cfg SHADOW_DBT_PROFILES_DIR)" ro
  mount_dir "$(cfg SHADOW_OUT_DIR)" rw
  # A rule file given as a local path (not `default`, which ships in the image).
  expected_file="$(cfg SHADOW_EXPECTED_DIFFERENCES)"
  case "$expected_file" in
    /*) [ -f "$expected_file" ] && args+=(-v "$expected_file:$expected_file:ro") ;;
  esac
fi
# Warehouses: host directories (mounted read-only), or paths inside the
# volumes of SHADOW_DOCKER_VOLUMES.
IFS=',' read -r -a volumes <<< "$(cfg SHADOW_DOCKER_VOLUMES)"
for spec in ${volumes[@]+"${volumes[@]}"}; do
  spec="$(echo "$spec" | tr -d ' ')"
  [ -n "$spec" ] && args+=(-v "$spec")
done
for side in REFERENCE CANDIDATE; do
  wh="$(cfg "SHADOW_${side}_WAREHOUSE")"
  covered=0
  for spec in ${volumes[@]+"${volumes[@]}"}; do
    target="$(echo "$spec" | cut -d: -f2)"
    case "$wh" in "$target"|"$target"/*) covered=1 ;; esac
  done
  [ "$covered" = 0 ] && mount_dir "$wh" ro
  # RSS meters.
  meter="$(cfg "SHADOW_${side}_RSS")"
  case "$meter" in
    container:*)
      id="$(docker inspect -f '{{.Id}}' "${meter#container:}")" \
        || { echo "shadow-run: SHADOW_${side}_RSS: no container ${meter#container:}" >&2; exit 2; }
      args+=(-e "SHADOW_${side}_CONTAINER_ID=$id")
      [[ " ${args[*]} " == *" /sys/fs/cgroup:/hostcg:ro "* ]] || args+=(-v /sys/fs/cgroup:/hostcg:ro)
      ;;
    pid:*) [[ " ${args[*]} " == *" --pid=host "* ]] || args+=(--pid=host) ;;
  esac
done
# Reports and logs in the operator's time zone, not the container's UTC:
# SHADOW_TZ, else TZ, else the zone /etc/localtime points to.
zone="$(cfg SHADOW_TZ)"; zone="${zone:-${TZ:-}}"
if [ -z "$zone" ]; then
  link="$(readlink /etc/localtime 2>/dev/null || true)"
  case "$link" in *zoneinfo/*) zone="${link##*zoneinfo/}" ;; *) zone=UTC ;; esac
fi
args+=(-e "TZ=$zone")
network="$(cfg SHADOW_DOCKER_NETWORK)"
[ -n "$network" ] && args+=(--network "$network")

status=0
case "$COMMAND" in
  run|compare|summary)
    docker run "${args[@]}" "$IMAGE" "$COMMAND" "$@" || status=$? ;;
  test)
    docker run "${args[@]}" --entrypoint python "$IMAGE" -m unittest discover -s tests "$@" || status=$? ;;
  mutate) # self-test only (tools/shadow/selftest.sh)
    docker run "${args[@]}" --entrypoint python "$IMAGE" -m codelake_shadow.mutate "$@" || status=$? ;;
  *)
    echo "shadow-run: unknown command $COMMAND (run, compare, summary, test)" >&2; status=2 ;;
esac
exit "$status"
