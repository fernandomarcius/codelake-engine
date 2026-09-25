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
# Per-case repetitions over the workload files, e.g. at SF100
# "ctas_lineitem=1/0,ctas_revenue_mart=1/0". Recorded in the report.
CASE_REPS="${BENCH_CASE_REPS:-}"
# Per-statement timeout of the bench client; a CTAS of lineitem at SF100 can
# take longer than the default half hour on a small container.
TIMEOUT_SECS="${BENCH_TIMEOUT_SECS:-1800}"
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
# Container and network name prefix: parallel checkouts on one Docker host use
# their own, so a run never removes another checkout's containers.
PREFIX="${BENCH_PREFIX:-codelake-bench}"
NET="$PREFIX-net"

COMMIT="$(git -C "$ROOT" rev-parse --short=12 HEAD)"
[ -z "$(git -C "$ROOT" status --porcelain --untracked-files=no)" ] || COMMIT="$COMMIT-dirty"
DATE="$(date +%Y-%m-%d)"
# Baselines, reports and raw runs are named after the commit; scales other than
# the default SF1 add a suffix, so SF10 and SF100 of one commit do not collide.
TAG="$COMMIT"
[ "$SCALE" = "1" ] || TAG="$COMMIT-sf$SCALE"
OUT="${BENCH_OUT:-$ROOT/bench/out/$DATE-$TAG}"
mkdir -p "$OUT"

log() { printf '[bench %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
has_phase() { case " $PHASES " in *" $1 "*) return 0 ;; *) return 1 ;; esac; }

cleanup() {
  [ -z "${WATCH_PID:-}" ] || kill "$WATCH_PID" 2>/dev/null || true
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

# Trino query memory in the same proportion as production (-Xmx8G,
# query.max-memory=6GB: 75% of the heap). The image keeps its JVM heap (80% of
# the container, MaxRAMPercentage), but its default query.max-memory-per-node
# is 30% of the heap: large joins and aggregations that production runs fail on
# a bench container, while CodeLake gets 75% of the container. Both query
# limits go to 75% of the heap; the heap headroom (default 30%; per-node limit
# plus headroom must fit in the heap) goes to 20%.
TRINO_HEAP_MIB=$((MEMORY_MIB * 80 / 100))
TRINO_QUERY_MEMORY_MIB=$((TRINO_HEAP_MIB * 75 / 100))
TRINO_HEADROOM_MIB=$((TRINO_HEAP_MIB * 20 / 100))
mkdir -p "$OUT/trino-etc"
cat > "$OUT/trino-etc/config.properties" <<CONFIG
# image default (single node)
coordinator=true
node-scheduler.include-coordinator=true
http-server.http.port=8080
discovery.uri=http://localhost:8080
# bench: production proportion of query memory to heap (docs/benchmarks.md)
query.max-memory=${TRINO_QUERY_MEMORY_MIB}MB
query.max-memory-per-node=${TRINO_QUERY_MEMORY_MIB}MB
memory.heap-headroom-per-node=${TRINO_HEADROOM_MIB}MB
CONFIG
chmod 644 "$OUT/trino-etc/config.properties"
TRINO_MEMORY="query.max-memory = query.max-memory-per-node = ${TRINO_QUERY_MEMORY_MIB}MB (75% of the ${TRINO_HEAP_MIB} MiB heap), memory.heap-headroom-per-node=${TRINO_HEADROOM_MIB}MB"

start_trino() {
  docker run -d --name "$PREFIX-trino" --network "$NET" "${limits[@]}" \
    -v "$ROOT/bench/trino/catalog":/etc/trino/catalog:ro \
    -v "$OUT/trino-etc/config.properties":/etc/trino/config.properties:ro \
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

# An engine container that dies during a phase (e.g. killed for exceeding its
# memory limit) is recorded and started again; the bench client waits for it
# and reports the case that was running as an error. Without this one crash
# would turn every later case of the phase into an error.
watch_engine() {
  local name="$1" state
  while docker wait "$name" >/dev/null 2>&1; do
    state="$(docker inspect -f 'exit {{.State.ExitCode}}, OOMKilled={{.State.OOMKilled}}, at {{.State.FinishedAt}}' "$name" 2>/dev/null)" || return 0
    echo "$name: $state" >> "$OUT/engine-deaths.txt"
    log "engine $name died ($state); starting it again"
    docker start "$name" >/dev/null 2>&1 || return 0
  done
}
WATCH_PID=""
start_watch() { watch_engine "$1" & WATCH_PID=$!; }
stop_watch() { [ -z "$WATCH_PID" ] || kill "$WATCH_PID" 2>/dev/null || true; WATCH_PID=""; }

started_at() { docker inspect -f '{{.State.StartedAt}}' "$1"; }
container_id() { docker inspect -f '{{.Id}}' "$1"; }

if has_phase trino; then : > "$OUT/others-cpu.txt"; : > "$OUT/engine-deaths.txt"; fi

case_reps_args=()
[ -z "$CASE_REPS" ] || case_reps_args=(--case-reps "$CASE_REPS")
common_run_args=(--workloads-dir /bench/workloads --workloads "$WORKLOADS" --scale "$SCALE"
  --reps "$REPS" --warmup "$WARMUP" ${case_reps_args[@]+"${case_reps_args[@]}"} --timeout-secs "$TIMEOUT_SECS"
  --cgroup-root /hostcg)

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
  gen_start="$(date +%s)"
  start_trino
  client generate --trino "http://$PREFIX-trino:8080" --scale "$SCALE" --out /out/dataset.json
  docker rm -f "$PREFIX-trino" >/dev/null
  # Wall time of the phase: CTAS of the missing tables plus the row-count
  # check of every table (a reused data set only pays the check).
  echo "$(( $(date +%s) - gen_start )) s with $CPUS CPUs, ${MEMORY_MIB} MiB (Trino 431 CTAS; existing tables only re-counted)" \
    > "$OUT/generate.txt"
fi

if has_phase trino; then
  log "Trino phase: fresh container, $CPUS CPUs, ${MEMORY_MIB} MiB"
  others_cpu trino
  start_trino
  TRINO_JVM="$(docker exec "$PREFIX-trino" sh -c 'grep -E "RAMPercentage|Xmx" /etc/trino/jvm.config | tr "\n" " "')"
  log "Trino JVM: $TRINO_JVM; $TRINO_MEMORY"
  start_watch "$PREFIX-trino"
  client run --engine trino --driver trino --url "http://$PREFIX-trino:8080" \
    "${common_run_args[@]}" --container-id "$(container_id "$PREFIX-trino")" \
    --started-at "$(started_at "$PREFIX-trino")" --engine-version "$TRINO_IMAGE" \
    --cases all --out /out/trino.json
  stop_watch
  docker rm -f "$PREFIX-trino" >/dev/null
fi

# CodeLake keeps ~75% of the container for query memory (ADR 0003); Trino
# keeps 75% of its heap (80% of the container), see start_trino.
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
  start_watch "$PREFIX-codelake"
  client run --engine codelake --driver pg \
    --url "host=$PREFIX-codelake port=5432 user=bench dbname=datafusion" \
    --metrics "$PREFIX-codelake:8080" \
    "${common_run_args[@]}" --container-id "$(container_id "$PREFIX-codelake")" \
    --started-at "$(started_at "$PREFIX-codelake")" --engine-version "$CODELAKE_VERSION" \
    --cases reads --out /out/codelake-pg.json
  stop_watch
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
    --reps "$REPS" --warmup "$WARMUP" ${case_reps_args[@]+"${case_reps_args[@]}"} --timeout-secs "$TIMEOUT_SECS" --cgroup-self --engine-version "$CODELAKE_VERSION" \
    --cases writes --out /out/codelake-embedded.json
  status="$(docker wait "$PREFIX-embedded")"
  docker logs "$PREFIX-embedded" 2>&1 | tail -n 40 >&2
  if [ "$status" != "0" ]; then
    # The write cases then show as MISSING; the read cases are still compared.
    state="$(docker inspect -f 'exit {{.State.ExitCode}}, OOMKilled={{.State.OOMKilled}}, at {{.State.FinishedAt}}' "$PREFIX-embedded")"
    echo "$PREFIX-embedded: $state (phase aborted)" >> "$OUT/engine-deaths.txt"
    log "embedded phase failed ($state)"
  fi
  docker rm -f "$PREFIX-embedded" >/dev/null
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
    printf '  "trino": %s,\n' "$(json_str "$TRINO_IMAGE ($(docker image inspect -f '{{index .RepoDigests 0}}' "$TRINO_IMAGE" 2>/dev/null || echo 'no digest')), JVM: ${TRINO_JVM:-image default}, query memory: $TRINO_MEMORY, config: image defaults + bench/trino/catalog + query memory")"
    printf '  "codelake": %s,\n' "$(json_str "$CODELAKE_VERSION, release profile ($BUILD_ENV), runtime $RUNTIME_IMAGE, memory.limit ${QUERY_MEMORY_MIB}MiB, target_partitions $CPUS")"
    printf '  "dataset_generation": %s,\n' "$(json_str "$(cat "$OUT/generate.txt" 2>/dev/null || echo 'not run in this output directory')")"
    deaths="$(tr '\n' ';' < "$OUT/engine-deaths.txt" 2>/dev/null || true)"
    printf '  "engine_container_deaths": %s,\n' "$(json_str "${deaths:-none}")"
    printf '  "case_repetition_overrides": %s,\n' "$(json_str "${CASE_REPS:-none}")"
    printf '  "statement_timeout_secs": %s,\n' "$(json_str "$TIMEOUT_SECS")"
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
    --baseline-out "/bench/baselines/$TAG.json" \
    --report-out "/bench/reports/$DATE-$TAG.md"
  log "report: bench/reports/$DATE-$TAG.md, baseline: bench/baselines/$TAG.json"
fi
