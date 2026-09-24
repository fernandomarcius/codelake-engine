#!/usr/bin/env bash
# Benchmark + correctness eval: CodeLake against Trino 431 on the same data,
# with the same container limits. See docs/benchmarks.md.
#
#   scripts/bench/run.sh                         # TPC-H SF1, every workload
#   BENCH_SCALE=tiny BENCH_REPS=1 scripts/bench/run.sh   # smoke run of the harness
#
# Measure on a reserved machine: nothing else building or running queries.
# Output: bench/out/<date>-<commit>/ (raw runs), bench/baselines/<commit>.json
# and bench/reports/<date>-<commit>.md (versioned).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

SCALE="${BENCH_SCALE:-1}"
CPUS="${BENCH_CPUS:-4}"
MEMORY_MIB="${BENCH_MEMORY_MIB:-4096}"
REPS="${BENCH_REPS:-5}"
WARMUP="${BENCH_WARMUP:-1}"
WORKLOADS="${BENCH_WORKLOADS:-tpch,microbatch,metadata,pipeline}"
PHASES="${BENCH_PHASES:-build generate trino codelake embedded compare}"
TRINO_IMAGE="${BENCH_TRINO_IMAGE:-trinodb/trino:431}"
# The binaries are built in the toolbelt image, so they run on it: same
# Debian release and shared libraries. (The release image's distroless/cc base
# lacks libz.so.1, which the binaries link through flate2/libz-sys.) Engines
# run as the production non-root uid.
RUNTIME_IMAGE="${BENCH_RUNTIME_IMAGE:-${CODELAKE_DEV_IMAGE:-codelake-dev:local}}"
ENGINE_USER="65532:65532"
TARGET_VOLUME="${CODELAKE_TARGET_VOLUME:-codelake-target}"
BUILD_ENV="${BENCH_BUILD_ENV-CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_LTO=off}"
WAREHOUSE="${BENCH_WAREHOUSE_VOLUME:-codelake-bench-warehouse}"
PREFIX="codelake-bench"
NET="$PREFIX-net"

COMMIT="$(git -C "$ROOT" rev-parse --short=12 HEAD)"
[ -z "$(git -C "$ROOT" status --porcelain --untracked-files=no)" ] || COMMIT="$COMMIT-dirty"
DATE="$(date +%Y-%m-%d)"
OUT="${BENCH_OUT:-$ROOT/bench/out/$DATE-$COMMIT}"
mkdir -p "$OUT"

log() { printf '[bench %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
has_phase() { case " $PHASES " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

cleanup() {
  docker rm -f "$PREFIX-trino" "$PREFIX-codelake" "$PREFIX-embedded" "$PREFIX-client" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  if [ -n "${BENCH_CLEAN:-}" ]; then docker volume rm "$WAREHOUSE" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

limits=(--cpus "$CPUS" --memory "${MEMORY_MIB}m" --memory-swap "${MEMORY_MIB}m")

# The bench client: outside the engine's limits, reads the engine's cgroup.
client() {
  docker run --rm --name "$PREFIX-client" --network "$NET" \
    -v "$TARGET_VOLUME":/opt/target:ro \
    -v /sys/fs/cgroup:/hostcg:ro \
    -v "$ROOT/bench":/bench \
    -v "$OUT":/out \
    --entrypoint /opt/target/release/codelake-bench \
    "$RUNTIME_IMAGE" "$@"
}

start_trino() {
  docker run -d --name "$PREFIX-trino" --network "$NET" "${limits[@]}" \
    -v "$ROOT/bench/trino/catalog":/etc/trino/catalog:ro \
    -v "$WAREHOUSE":/warehouse \
    "$TRINO_IMAGE" >/dev/null
}

# CPU used by containers that are not ours, sampled before each engine phase
# and written into the report: the lock serializes builds, not other agents'
# containers.
others_cpu() {
  local pct
  pct="$(docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' | grep -v "^$PREFIX-" \
    | awk '{gsub("%","",$2); s+=$2} END {printf "%.0f", s}')"
  echo "$1: ${pct}%" >> "$OUT/others-cpu.txt"
  log "other containers use ${pct}% CPU (100% = one core) before $1"
}

started_at() { docker inspect -f '{{.State.StartedAt}}' "$1"; }
container_id() { docker inspect -f '{{.Id}}' "$1"; }

if has_phase trino; then : > "$OUT/others-cpu.txt"; fi

common_run_args=(--workloads-dir /bench/workloads --workloads "$WORKLOADS" --scale "$SCALE"
  --reps "$REPS" --warmup "$WARMUP" --cgroup-root /hostcg)

if has_phase build; then
  log "building release binaries (codelake, codelake-bench) into volume $TARGET_VOLUME"
  # One binary at a time. The release profile's thin-LTO link of the whole
  # engine needs more memory than a 12 GB Docker VM shared with other work has,
  # so by default the bench binaries are built without LTO and debug info
  # (opt-level 3 and codegen-units 1 kept). The report records BUILD_ENV; set
  # BENCH_BUILD_ENV="" on a larger machine to measure the exact release profile.
  for package in codelake-server codelake-bench; do
    CODELAKE_TARGET_VOLUME="$TARGET_VOLUME" "$ROOT/scripts/dev.sh" \
      env $BUILD_ENV cargo build --release --locked -p "$package"
  done
fi

cleanup
docker network create "$NET" >/dev/null
docker volume create "$WAREHOUSE" >/dev/null
# Trino (uid 1000) and CodeLake (uid 65532) both write schemas at the root.
docker run --rm --user 0 -v "$WAREHOUSE":/warehouse --entrypoint /bin/chmod "$TRINO_IMAGE" 1777 /warehouse

CODELAKE_VERSION="$(docker run --rm -v "$TARGET_VOLUME":/opt/target:ro --entrypoint /opt/target/release/codelake "$RUNTIME_IMAGE" --version)"

if has_phase generate; then
  log "generating TPC-H scale $SCALE with Trino's tpch connector (skipped per table if present)"
  start_trino
  client generate --trino "http://$PREFIX-trino:8080" --scale "$SCALE" --out /out/dataset.json
  docker rm -f "$PREFIX-trino" >/dev/null
fi

if has_phase trino; then
  log "Trino phase: fresh container, $CPUS CPUs, ${MEMORY_MIB} MiB"
  others_cpu trino
  start_trino
  TRINO_JVM="$(docker exec "$PREFIX-trino" sh -c 'grep -E "RAMPercentage|Xmx" /etc/trino/jvm.config | tr "\n" " "')"
  client run --engine trino --driver trino --url "http://$PREFIX-trino:8080" \
    "${common_run_args[@]}" --container-id "$(container_id "$PREFIX-trino")" \
    --started-at "$(started_at "$PREFIX-trino")" --engine-version "$TRINO_IMAGE" \
    --cases all --out /out/trino.json
  docker rm -f "$PREFIX-trino" >/dev/null
fi

# CodeLake keeps ~75% of the container for query memory (ADR 0003), the same
# share the Trino image gives its JVM heap (MaxRAMPercentage=80).
QUERY_MEMORY_MIB=$((MEMORY_MIB * 3 / 4))
cat > "$OUT/codelake.toml" <<EOF
[server]
pg_listen = "0.0.0.0:5432"
http_listen = "0.0.0.0:8080"
# Isolated benchmark network only.
allow_unauthenticated_network = true
max_connections = 16
shutdown_grace_secs = 1

[memory]
limit = "${QUERY_MEMORY_MIB}MiB"
spill_dir = "/tmp/codelake-spill"
spill_limit = "32GiB"

[execution]
target_partitions = $CPUS

[[source]]
name = "hive"
kind = "hive"
warehouse = "/warehouse"
EOF

codelake_container() {
  local name="$1"; shift
  docker run -d --name "$name" --network "$NET" "${limits[@]}" --user "$ENGINE_USER" \
    -v "$TARGET_VOLUME":/opt/target:ro \
    -v "$WAREHOUSE":/warehouse \
    -v "$OUT/codelake.toml":/etc/codelake/codelake.toml:ro \
    "$@" >/dev/null
}

if has_phase codelake; then
  log "CodeLake phase (PostgreSQL wire, read cases): fresh container, $CPUS CPUs, ${MEMORY_MIB} MiB"
  others_cpu codelake-pg
  codelake_container "$PREFIX-codelake" --entrypoint /opt/target/release/codelake \
    "$RUNTIME_IMAGE" serve --config /etc/codelake/codelake.toml
  client run --engine codelake --driver pg \
    --url "host=$PREFIX-codelake port=5432 user=bench dbname=datafusion" \
    --metrics "$PREFIX-codelake:8080" \
    "${common_run_args[@]}" --container-id "$(container_id "$PREFIX-codelake")" \
    --started-at "$(started_at "$PREFIX-codelake")" --engine-version "$CODELAKE_VERSION" \
    --cases reads --out /out/codelake-pg.json
  docker rm -f "$PREFIX-codelake" >/dev/null
fi

if has_phase embedded; then
  # The PostgreSQL wire path does not route DDL to the Hive catalog yet, so
  # write cases run with the engine inside the bench process, in a container
  # with the same limits. Not a like-for-like protocol comparison.
  log "CodeLake phase (embedded, write cases): $CPUS CPUs, ${MEMORY_MIB} MiB"
  others_cpu codelake-embedded
  codelake_container "$PREFIX-embedded" -v "$ROOT/bench":/bench:ro -v "$OUT":/out \
    --entrypoint /opt/target/release/codelake-bench "$RUNTIME_IMAGE" \
    run --engine codelake --driver embedded --config /etc/codelake/codelake.toml \
    --workloads-dir /bench/workloads --workloads "$WORKLOADS" --scale "$SCALE" \
    --reps "$REPS" --warmup "$WARMUP" --cgroup-self --engine-version "$CODELAKE_VERSION" \
    --cases writes --out /out/codelake-embedded.json
  status="$(docker wait "$PREFIX-embedded")"
  docker logs "$PREFIX-embedded" 2>&1 | tail -n 40 >&2
  docker rm -f "$PREFIX-embedded" >/dev/null
  [ "$status" = "0" ] || { log "embedded phase failed (exit $status)"; exit 1; }
fi

json_str() { printf '"%s"' "$(printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g' | tr -d '\n')"; }

if has_phase compare; then
  host_cpu="$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2- || echo unknown)"
  others="$(docker ps --format '{{.Names}}' | grep -cv "^$PREFIX-" || true)"
  {
    printf '{\n'
    printf '  "cpus": %s,\n' "$(json_str "$CPUS")"
    printf '  "memory_mib": %s,\n' "$(json_str "$MEMORY_MIB")"
    printf '  "host_cpu": %s,\n' "$(json_str "$host_cpu")"
    printf '  "host_os": %s,\n' "$(json_str "$(uname -srm)")"
    printf '  "docker": %s,\n' "$(json_str "$(docker info --format 'server {{.ServerVersion}}, {{.OperatingSystem}}, kernel {{.KernelVersion}}, {{.NCPU}} CPUs, {{.MemTotal}} bytes, cgroup v{{.CgroupVersion}}')")"
    printf '  "other_containers_running": %s,\n' "$(json_str "$others, not ours")"
    printf '  "other_containers_cpu_before_phase": %s,\n' "$(json_str "$(tr '\n' ';' < "$OUT/others-cpu.txt" 2>/dev/null) (100% = one core; VM has $(docker info --format '{{.NCPU}}') CPUs)")"
    printf '  "trino": %s,\n' "$(json_str "$TRINO_IMAGE ($(docker image inspect -f '{{index .RepoDigests 0}}' "$TRINO_IMAGE" 2>/dev/null || echo 'no digest')), JVM: ${TRINO_JVM:-image default}, config: image defaults + bench/trino/catalog")"
    printf '  "codelake": %s,\n' "$(json_str "$CODELAKE_VERSION, release profile ($BUILD_ENV), runtime $RUNTIME_IMAGE, memory.limit ${QUERY_MEMORY_MIB}MiB, target_partitions $CPUS")"
    printf '  "codelake_protocols": %s,\n' "$(json_str "reads over PostgreSQL wire; writes embedded (PG wire does not route DDL to hive yet)")"
    printf '  "dataset": %s\n' "$(json_str "TPC-H scale $SCALE from Trino 431 tpch connector, Parquet via Hive file metastore, money as decimal(15,2): $(tr -d ' \n' < "$OUT/dataset.json" 2>/dev/null || echo unknown)")"
    printf '}\n'
  } > "$OUT/env.json"
  codelake_runs=()
  for f in codelake-pg.json codelake-embedded.json; do
    [ -f "$OUT/$f" ] && codelake_runs+=(--codelake "/out/$f")
  done
  client compare --trino /out/trino.json "${codelake_runs[@]}" --env /out/env.json \
    --commit "$COMMIT" --date "$DATE" \
    --baseline-out "/bench/baselines/$COMMIT.json" \
    --report-out "/bench/reports/$DATE-$COMMIT.md"
  log "report: bench/reports/$DATE-$COMMIT.md, baseline: bench/baselines/$COMMIT.json"
fi
