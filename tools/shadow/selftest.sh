#!/usr/bin/env bash
# Self-test of the shadow-run harness, on disposable containers only:
#
#   SQL Server 2022 with CDC  ->  reference engine (trinodb/trino:431)
#                             ->  candidate (codelake, configured by `codelake config import`)
#
# 1. `codelake config import` turns the fixture catalogs into the candidate's
#    configuration: secrets stay references, the unsupported catalog is
#    skipped with a reason, no secret value is printed or written.
# 2. Two shadow-run cycles of the fixture dbt project, with CDC changes
#    between them: both must report zero divergences.
# 3. Mutations injected into a copy of the candidate's output (a row more, a
#    row less, a decimal, a timestamp +3h, a trailing space, NULL -> '', a
#    declared type, INT96 copied as the stored instant): each must be
#    detected, on the right column. The copy is written by DuckDB, which
#    cannot write INT96: every mutation also shows the timestamps' storage
#    difference, and a rewrite that keeps the values shows nothing else. A
#    documented type difference is reported as expected only with a rule.
#
# Heavy (three engines): run it under the lock, from the repository root:
#   CODELAKE_TARGET_VOLUME=… /path/to/heavy.sh tools/shadow/selftest.sh
# It builds `codelake` with scripts/dev.sh into CODELAKE_TARGET_VOLUME
# (SHADOW_SELFTEST_SKIP_BUILD=1 reuses the binary there).
# SHADOW_SELFTEST_STRICT=1 runs the fixture without the workarounds for the
# candidate's known findings (docs/shadow-run.md, "Findings"): it fails until
# they are fixed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SHADOW="$ROOT/tools/shadow"
FIXTURE="$SHADOW/fixture"
WORK="${SHADOW_SELFTEST_DIR:-$ROOT/target/shadow-selftest}"
TARGET_VOLUME="${CODELAKE_TARGET_VOLUME:-codelake-target}"
PROFILE="${SHADOW_SELFTEST_PROFILE:-debug}"
RUNTIME_IMAGE="${CODELAKE_DEV_IMAGE:-codelake-dev:local}"
REFERENCE_IMAGE="${SHADOW_SELFTEST_REFERENCE_IMAGE:-trinodb/trino:431}"
MSSQL_IMAGE="${SHADOW_SELFTEST_MSSQL_IMAGE:-mcr.microsoft.com/mssql/server:2022-latest}"
NET="codelake-shadow-selftest"
# Names the fixture catalogs use (src.properties: shadow-mssql).
MSSQL="shadow-mssql"; REFERENCE="shadow-reference"; CODELAKE="shadow-codelake"
BIN="/opt/target/$PROFILE/codelake"

log() { printf '[selftest %s] %s\n' "$(date +%H:%M:%S)" "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }
CHECKS=()
pass() { CHECKS+=("PASS $*"); log "PASS: $*"; }

cleanup() {
  if [ -n "${SHADOW_SELFTEST_KEEP:-}" ]; then log "containers kept (SHADOW_SELFTEST_KEEP)"; return; fi
  docker rm -f "$MSSQL" "$REFERENCE" "$CODELAKE" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

if [ -z "${SHADOW_SELFTEST_SKIP_BUILD:-}" ]; then
  log "building codelake ($PROFILE) into volume $TARGET_VOLUME"
  release=(); [ "$PROFILE" = release ] && release=(--release)
  CODELAKE_TARGET_VOLUME="$TARGET_VOLUME" "$ROOT/scripts/dev.sh" \
    cargo build --locked ${release[@]+"${release[@]}"} -p codelake-server --bin codelake
fi
IMAGE="$("$SHADOW/image.sh")"
log "harness image $IMAGE"

cleanup
rm -rf "$WORK"
mkdir -p "$WORK"/{reference-wh,candidate-wh,out,catalog-reference,candidate-config}
# The reference image (uid 1000) and CodeLake (uid 65532) write their own warehouse.
chmod 1777 "$WORK/reference-wh" "$WORK/candidate-wh" "$WORK/candidate-config"
cp "$FIXTURE/catalog/src.properties" "$FIXTURE/catalog/hive.properties" "$WORK/catalog-reference/"

# Throwaway credentials for the disposable SQL Server, in a 0600 file that
# every container reads with --env-file. Never printed.
SECRETS="$WORK/secrets.env"
( umask 077; printf 'MSSQL_SA_PASSWORD=Sh4dow-%s\nACCEPT_EULA=Y\nMSSQL_AGENT_ENABLED=true\n' \
    "$(od -An -N12 -tx1 /dev/urandom | tr -d ' \n')" > "$SECRETS" )
secret_value() { sed -n 's/^MSSQL_SA_PASSWORD=//p' "$SECRETS"; }

docker network create "$NET" >/dev/null

# --- SQL Server with CDC ----------------------------------------------------
log "starting SQL Server 2022 with the agent (CDC capture)"
docker run -d --name "$MSSQL" --network "$NET" --memory 2g --env-file "$SECRETS" "$MSSQL_IMAGE" >/dev/null
sqlcmd() { # file.sql [extra sqlcmd args]
  local file="$1"; shift
  docker cp "$file" "$MSSQL:/tmp/$(basename "$file")" >/dev/null
  docker exec "$MSSQL" sh -c '/opt/mssql-tools18/bin/sqlcmd -C -b -f 65001 -S localhost -U sa -P "$MSSQL_SA_PASSWORD" "$@"' \
    sqlcmd -i "/tmp/$(basename "$file")" "$@"
}
printf 'SELECT 1;\n' > "$WORK/ping.sql"
for i in $(seq 1 90); do
  if sqlcmd "$WORK/ping.sql" -h -1 >/dev/null 2>&1; then break; fi
  [ "$i" = 90 ] && fail "SQL Server did not start"
  sleep 2
done
sqlcmd "$FIXTURE/sqlserver/setup.sql" >/dev/null

wait_captured() { # expected change rows
  local out
  for i in $(seq 1 90); do
    out="$(sqlcmd "$FIXTURE/sqlserver/captured.sql" -h -1 -W | tr -s ' \r\n' ' ' | sed 's/^ //; s/ $//')"
    [ "$out" = "1 $1" ] && { log "CDC captured $1 change rows"; return 0; }
    sleep 2
  done
  fail "CDC capture did not reach $1 change rows (last: $out)"
}
sqlcmd "$FIXTURE/sqlserver/seed.sql" >/dev/null
wait_captured 6

# --- Reference engine ---------------------------------------------------------
log "starting the reference engine ($REFERENCE_IMAGE)"
docker run -d --name "$REFERENCE" --network "$NET" --memory 3g --env-file "$SECRETS" \
  -v "$WORK/catalog-reference":/etc/trino/catalog:ro \
  -v "$WORK/reference-wh":/warehouse \
  "$REFERENCE_IMAGE" >/dev/null

# --- Candidate configuration: `codelake config import` ------------------------
log "codelake config import (fixture catalogs, base with \${ENV:…} references)"
set +e
docker run --rm --user "$(id -u):$(id -g)" \
  -v "$TARGET_VOLUME":/opt/target:ro \
  -v "$FIXTURE/catalog":/catalog:ro \
  -v "$FIXTURE/candidate-base.toml":/base.toml:ro \
  -v "$WORK/candidate-config":/out \
  --entrypoint "$BIN" "$RUNTIME_IMAGE" \
  config import --from /catalog --base /base.toml --out /out/codelake.toml --env-out /out/codelake.env \
  > "$WORK/import-report.txt" 2>&1
status=$?
set -e
cat "$WORK/import-report.txt" >&2
[ "$status" = 3 ] || fail "config import exit $status, expected 3 (one catalog skipped)"
grep -q '^legacy .*skipped.*hive.metastore is not set' "$WORK/import-report.txt" \
  || fail "the thrift catalog is not reported as skipped with its reason"
grep -q '^src .*imported.*secrets: MSSQL_SA_PASSWORD' "$WORK/import-report.txt" \
  || fail "src is not imported with its password as a reference"
grep -q '^password_env = "MSSQL_SA_PASSWORD"$' "$WORK/candidate-config/codelake.toml" \
  || fail "codelake.toml does not reference MSSQL_SA_PASSWORD"
grep -q '^parquet_time_zone = "America/Sao_Paulo"$' "$WORK/candidate-config/codelake.toml" \
  || fail "codelake.toml does not carry hive.parquet.time-zone"
if grep -rqF -- "$(secret_value)" "$WORK/import-report.txt" "$WORK/candidate-config"; then
  fail "a secret value reached the import report or its files"
fi
mode="$(stat -f '%Lp' "$WORK/candidate-config/codelake.env" 2>/dev/null || stat -c '%a' "$WORK/candidate-config/codelake.env")"
[ "$mode" = 600 ] || fail "env file mode $mode, expected 600"
pass "config import: 2 catalogs imported (time zone carried over), thrift skipped with reason, no secret value written, env file 0600"

docker run --rm --user "$(id -u):$(id -g)" --env-file "$SECRETS" -e CODELAKE_MEMORY_LIMIT=640MiB \
  -v "$TARGET_VOLUME":/opt/target:ro -v "$WORK/candidate-config":/cfg:ro \
  --entrypoint "$BIN" "$RUNTIME_IMAGE" check-config --config /cfg/codelake.toml >/dev/null 2>&1 \
  || fail "check-config refused the imported configuration"
set +e
docker run --rm --user "$(id -u):$(id -g)" -v "$TARGET_VOLUME":/opt/target:ro -v "$WORK/candidate-config":/cfg:ro \
  --entrypoint "$BIN" "$RUNTIME_IMAGE" check-config --config /cfg/codelake.toml > "$WORK/check-missing.txt" 2>&1
status=$?
set -e
[ "$status" = 2 ] && grep -q 'CODELAKE_MEMORY_LIMIT' "$WORK/check-missing.txt" \
  || fail "check-config without the environment must name the missing \${ENV:…} variable (exit $status)"
pass "check-config resolves \${ENV:…} at load; a missing variable is named and exits 2"

log "starting the candidate (codelake $PROFILE build)"
docker run -d --name "$CODELAKE" --network "$NET" --memory 1g --user 65532:65532 \
  --env-file "$SECRETS" -e CODELAKE_MEMORY_LIMIT=640MiB \
  -v "$TARGET_VOLUME":/opt/target:ro \
  -v "$WORK/candidate-config/codelake.toml":/etc/codelake/codelake.toml:ro \
  -v "$WORK/candidate-wh":/warehouse \
  --entrypoint "$BIN" "$RUNTIME_IMAGE" serve --config /etc/codelake/codelake.toml >/dev/null

log "waiting for both engines"
docker run --rm --network "$NET" --entrypoint python "$IMAGE" -c '
import json, sys, time, urllib.request
for url in sys.argv[1:]:
    deadline = time.time() + 240
    while True:
        try:
            info = json.load(urllib.request.urlopen(url, timeout=5))
            if not info.get("starting", False):
                break
        except Exception:
            pass
        if time.time() > deadline:
            sys.exit(f"{url} not ready")
        time.sleep(2)
' "http://$REFERENCE:8080/v1/info" "http://$CODELAKE:8080/v1/info" \
  || { docker logs --tail 50 "$CODELAKE" >&2 || true; fail "an engine did not start"; }

# --- Shadow run ----------------------------------------------------------------
CONFIG="$WORK/shadow.env"
cat > "$CONFIG" <<EOF
SHADOW_REFERENCE_URL=http://$REFERENCE:8080
SHADOW_CANDIDATE_URL=http://$CODELAKE:8080
SHADOW_REFERENCE_TARGET=reference
SHADOW_CANDIDATE_TARGET=candidate
SHADOW_DBT_PROJECT_DIR=$FIXTURE
SHADOW_DBT_PROFILES_DIR=$FIXTURE
SHADOW_REFERENCE_WAREHOUSE=$WORK/reference-wh
SHADOW_CANDIDATE_WAREHOUSE=$WORK/candidate-wh
SHADOW_OUT_DIR=$WORK/out
SHADOW_REFERENCE_RSS=container:$REFERENCE
SHADOW_CANDIDATE_RSS=container:$CODELAKE
SHADOW_CUTOFF_SQL=SELECT max(start_lsn) FROM src.cdc.lsn_time_mapping
SHADOW_IGNORE_COLUMNS=shadow_pilot.orders.id,shadow_pilot.orders.loaded_at
SHADOW_PARQUET_TIME_ZONE=America/Sao_Paulo
SHADOW_DOCKER_NETWORK=$NET
EOF
# Strict: the fixture exactly as the acceptance pipelines write it, without
# the workarounds for the candidate's known findings (docs/shadow-run.md).
if [ -n "${SHADOW_SELFTEST_STRICT:-}" ]; then
  echo 'SHADOW_DBT_VARS={"shadow_workarounds": false}' >> "$CONFIG"
  log "strict: fixture without workarounds (findings F8-1..F8-3 expected to diverge)"
fi

log "harness unit tests"
"$ROOT/scripts/shadow-run.sh" "$CONFIG" test >&2 || fail "harness unit tests"
pass "harness unit tests"

show_cycle() { cat "$WORK/out/cycle-000$1.md" >&2; }
log "cycle 1"
"$ROOT/scripts/shadow-run.sh" "$CONFIG" run || { show_cycle 1 || true; fail "cycle 1 diverged"; }
show_cycle 1

log "CDC changes between cycles"
sqlcmd "$FIXTURE/sqlserver/changes.sql" >/dev/null
wait_captured 12
log "cycle 2"
"$ROOT/scripts/shadow-run.sh" "$CONFIG" run || { show_cycle 2 || true; fail "cycle 2 diverged"; }
show_cycle 2

python3 - "$WORK/out" <<'PY' || fail "cycle reports do not show 2 equal cycles with the CDC changes applied"
import json, sys
out = sys.argv[1]
summary = json.load(open(f"{out}/summary.json"))
assert summary["cycles"] == 2 and summary["verdict"] == "equal", summary
rows = {}
for n in (1, 2):
    cycle = json.load(open(f"{out}/cycle-000{n}.json"))
    assert cycle["verdict"] == "equal", cycle["failures"]
    assert len(cycle["tables"]) == 5, [t["table"] for t in cycle["tables"]]
    for side in ("reference", "candidate"):
        assert cycle["sides"][side]["rss"]["peak_bytes"], (side, cycle["sides"][side]["rss"])
    rows[n] = {t["table"]: t["rows"]["reference"] for t in cycle["tables"]}
assert rows[1]["shadow_raw.dbo_orders_ct"] == 6 and rows[2]["shadow_raw.dbo_orders_ct"] == 12, rows
assert rows[2]["shadow_pilot.orders"] > rows[1]["shadow_pilot.orders"], rows
print("cycles:", json.dumps(summary["per_cycle"]), file=sys.stderr)
print("peak RSS:", summary["peak_rss_bytes"], "ratio:", summary["peak_rss_ratio"], file=sys.stderr)
PY
pass "2 cycles, 5 tables each, 0 divergences; CDC changes appended in cycle 2; RSS sampled on both sides"

# --- Mutations -----------------------------------------------------------------
TABLE="shadow_pilot.orders"
mutation() { # name column expected-exit expected-text [compare args]
  local name="$1" column="$2" expect="$3" text="$4" status
  shift 4
  local col=(); [ -n "$column" ] && col=(--column "$column")
  "$ROOT/scripts/shadow-run.sh" "$CONFIG" mutate --from "$WORK/candidate-wh" --to "$WORK/out/mutations/$name" \
    --table "$TABLE" --mutation "$name" --parquet-time-zone America/Sao_Paulo ${col[@]+"${col[@]}"} 2>&1 | tail -n 1 >&2
  set +e
  "$ROOT/scripts/shadow-run.sh" "$CONFIG" compare --reference-warehouse "$WORK/reference-wh" \
    --candidate-warehouse "$WORK/out/mutations/$name" --table "$TABLE" \
    --ignore-column "$TABLE.id" --ignore-column "$TABLE.loaded_at" --parquet-time-zone America/Sao_Paulo \
    --work "$WORK/out/mutations/tmp" --out "$WORK/out/mutations/$name.json" "$@" > "$WORK/out/mutations/$name.txt"
  status=$?
  set -e
  cat "$WORK/out/mutations/$name.txt" >&2
  [ "$status" = "$expect" ] || fail "mutation $name: exit $status, expected $expect"
  grep -qxF -- "$text" "$WORK/out/mutations/$name.txt" || fail "mutation $name: output lacks '$text'"
  pass "mutation $name -> exit $status, '$text'"
}
# INT96 timestamps of the copy are INT64: a storage divergence, values equal.
TS_STORAGE="storage_types"
mutation neutral         ""         1 "$TABLE: divergent ($TS_STORAGE)"
# The stored UTC instant written as INT64: every engine reads it 3h later.
mutation int96_as_stored ""         1 "$TABLE: divergent ($TS_STORAGE, checksum) columns: changed_at, recorded_at, created_at"
# A row more or less changes every compared column's checksum.
ALL="__start_lsn, __seqval, operation, deleted, changed_at, recorded_at, order_key, order_id, status, amount, created_at, note, code, qty, seqval_number"
mutation extra_row       ""         1 "$TABLE: divergent ($TS_STORAGE, row_count, checksum) columns: $ALL"
mutation missing_row     ""         1 "$TABLE: divergent ($TS_STORAGE, row_count, checksum) columns: $ALL"
mutation decimal         amount     1 "$TABLE: divergent ($TS_STORAGE, checksum) columns: amount"
mutation timestamp_3h    created_at 1 "$TABLE: divergent ($TS_STORAGE, checksum) columns: created_at"
mutation trailing_space  note       1 "$TABLE: divergent ($TS_STORAGE, checksum) columns: note"
mutation null_to_empty   note       1 "$TABLE: divergent ($TS_STORAGE, checksum) columns: note"
# A declared type that differs (varchar(20) -> string) diverges; the shipped
# rule file classifies it as ISS-019, expected and documented.
mutation declared_type   status     1 "$TABLE: divergent (declared_types, $TS_STORAGE)"
mutation declared_type   status     1 "$TABLE: divergent ($TS_STORAGE) expected: ISS-019" --expected default

log "summary"
printf '  %s\n' "${CHECKS[@]}" >&2
log "reports: $WORK/out (cycle-0001.md, cycle-0002.md, summary.md, mutations/)"
