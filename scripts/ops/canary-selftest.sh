#!/usr/bin/env bash
# Self-test of scripts/ops/canary.sh against a local mock of the HTTP statement
# protocol (testdata/mock_statement_server.py). Needs bash, curl, jq and
# python3; touches no engine and no network beyond 127.0.0.1.
#
#   scripts/ops/canary-selftest.sh
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CANARY="$HERE/canary.sh"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/canary-selftest.XXXXXX")"
MOCK_PID=""
cleanup() { [ -z "$MOCK_PID" ] || { kill "$MOCK_PID" 2>/dev/null; wait "$MOCK_PID" 2>/dev/null; }; rm -rf "$WORK"; }
trap cleanup EXIT

python3 "$HERE/testdata/mock_statement_server.py" "$WORK/port" "$WORK/cancels" &
MOCK_PID=$!
for _ in $(seq 1 50); do [ -s "$WORK/port" ] && break; sleep 0.1; done
[ -s "$WORK/port" ] || { echo "mock server did not start" >&2; exit 1; }
URL="http://127.0.0.1:$(cat "$WORK/port")"

PASSED=0 FAILED=0
pass() { PASSED=$((PASSED + 1)); printf 'ok   %s\n' "$1"; }
fail() { FAILED=$((FAILED + 1)); printf 'FAIL %s\n' "$1"; [ -z "${2:-}" ] || printf '     %s\n' "$2"; }

if command -v sha256sum >/dev/null 2>&1; then sha() { sha256sum | cut -d' ' -f1; }
else sha() { shasum -a 256 | cut -d' ' -f1; }; fi

# config <file> <checks JSON array> [extra top-level JSON]
config() {
  local extra="${3:-}"
  [ -n "$extra" ] || extra='{}'
  jq -n --arg url "$URL" --argjson checks "$2" --argjson extra "$extra" '
    {name: "selftest", user: "canary", http: {url: $url, catalog: "lake", schema: "s"},
     defaults: {max_latency_ms: 5000, timeout_secs: 10}, checks: $checks} + $extra' >"$1"
}

# expect_run <label> <expected exit> <config> [canary args...]; leaves the
# output in $WORK/out and stderr in $WORK/err.
expect_run() {
  local label="$1" want="$2" cfg="$3"; shift 3
  "$CANARY" --config "$cfg" "$@" >"$WORK/out" 2>"$WORK/err"
  local got=$?
  if [ "$got" = "$want" ]; then pass "$label: exit $got"; else
    fail "$label: exit $got, expected $want" "$(head -c 400 "$WORK/out" "$WORK/err" | tr '\n' ' ')"; fi
}

# expect_jq <label> <jq filter that must be true on the output line>
expect_jq() {
  if jq -e "$2" "$WORK/out" >/dev/null 2>&1; then pass "$1"; else
    fail "$1" "$(head -c 600 "$WORK/out")"; fi
}

ROWS_SHA="$(printf '\\N\t3\na\t1\nb\t2\n' | sha)"

config "$WORK/ok.json" "$(jq -nc --arg sha "$ROWS_SHA" '[
  {name: "value", sql: "SELECT 42", expect: {value: "42"}},
  {name: "value_number", sql: "SELECT 42", expect: {value: 42, row_count: 1}},
  {name: "bounds", sql: "SELECT 42", expect: {min: 40, max: 42}},
  {name: "checksum", sql: "SELECT rows", expect: {sha256: $sha, row_count: 3}},
  {name: "rows", sql: "SELECT rows", expect: {rows: [["a", 1], ["b", 2], [null, 3]]}},
  {name: "fresh", sql: "SELECT fresh", freshness: {max_lag_secs: 300}},
  {name: "same_as_reference", sql: "SELECT 42", expect: {reference: true}},
  {name: "smoke", sql: "SELECT 42"}
]')" "$(jq -nc --arg ref "$URL/ref" '{reference: {url: $ref}}')"
expect_run "all checks pass" 0 "$WORK/ok.json"
expect_jq "run reported ok" '.status == "ok" and .checks_total == 8 and .checks_failed == 0'
if [ "$(wc -l <"$WORK/out" | tr -d ' ')" = 1 ]; then pass "one JSON line per run"; else fail "one JSON line per run" "$(cat "$WORK/out")"; fi
expect_jq "canonical sha256 of unordered rows" "[.checks[] | select(.name == \"checksum\") | .sha256 == \"$ROWS_SHA\"] | all"
expect_jq "freshness lag reported" '[.checks[] | select(.name == "fresh") | .freshness_lag_secs >= 9 and .freshness_lag_secs < 300] | all'

config "$WORK/mismatch.json" '[{"name": "value", "sql": "SELECT 42", "expect": {"value": "43"}},
  {"name": "rows", "sql": "SELECT rows", "expect": {"rows": [["a", 1]]}},
  {"name": "max", "sql": "SELECT 42", "expect": {"max": 10}},
  {"name": "ordered", "sql": "SELECT rows", "expect": {"rows": [["a", 1], ["b", 2], [null, 3]], "ordered": true}}]'
expect_run "wrong results fail" 1 "$WORK/mismatch.json"
expect_jq "every wrong result is a mismatch" '.status == "fail" and .checks_failed == 4 and ([.checks[].status] | all(. == "mismatch"))'
expect_jq "mismatch names both values" '.checks[0].message | test("value 42, expected 43")'

config "$WORK/slo.json" '[{"name": "stale", "sql": "SELECT stale", "freshness": {"max_lag_secs": 300}},
  {"name": "slow", "sql": "SELECT slow", "max_latency_ms": 500}]'
expect_run "SLO breaches fail" 1 "$WORK/slo.json"
expect_jq "stale and slow reported" '[.checks[].status] == ["stale", "slow"] and .checks[1].latency_ms >= 1500'

config "$WORK/errors.json" '[{"name": "missing", "sql": "SELECT fail"},
  {"name": "hang", "sql": "SELECT hang", "timeout_secs": 1},
  {"name": "stall", "sql": "SELECT stall", "timeout_secs": 1}]'
expect_run "query errors fail" 1 "$WORK/errors.json"
expect_jq "engine error name kept" '.checks[0].status == "error" and (.checks[0].message | startswith("TABLE_NOT_FOUND"))'
expect_jq "timeout between pages reported" '.checks[1].status == "error" and (.checks[1].message | test("^timeout after 1s"))'
expect_jq "timeout inside a page reported" '.checks[2].status == "error" and (.checks[2].message | test("^timeout after 1s"))'
if [ "$(wc -l <"$WORK/cancels" 2>/dev/null | tr -d ' ')" = 2 ]; then pass "timed-out queries canceled on the server"; else
  fail "timed-out queries canceled on the server" "$(cat "$WORK/cancels" 2>/dev/null)"; fi

config "$WORK/drift.json" '[{"name": "drift", "sql": "SELECT drift", "expect": {"reference": true}}]' \
  "$(jq -nc --arg ref "$URL/ref" '{reference: {url: $ref}}')"
expect_run "divergence from the reference fails" 1 "$WORK/drift.json"
expect_jq "divergence reported as mismatch" '.checks[0].status == "mismatch" and (.checks[0].message | test("differs from the reference"))'

"$CANARY" --config "$WORK/ok.json" --interval 1 --max-runs 2 >"$WORK/out" 2>"$WORK/err"
if [ "$?" = 0 ] && [ "$(wc -l <"$WORK/out" | tr -d ' ')" = 2 ] && [ "$(jq -s '[.[].run] == [1, 2]' "$WORK/out")" = true ]; then
  pass "periodic mode: one line per run"
else
  fail "periodic mode: one line per run" "$(cat "$WORK/out" "$WORK/err")"
fi

expect_run "textfile metrics" 1 "$WORK/slo.json" --textfile "$WORK/canary.prom"
if grep -q 'codelake_canary_run_ok{canary="selftest"} 0' "$WORK/canary.prom" \
  && grep -q 'codelake_canary_check_ok{canary="selftest",check="stale",status="stale"} 0' "$WORK/canary.prom"; then
  pass "textfile has run and check gauges"
else
  fail "textfile has run and check gauges" "$(cat "$WORK/canary.prom" 2>/dev/null)"
fi

printf '{"checks": [{"name": "bad name!", "sql": ""}, {"name": "x", "sql": "SELECT 1", "expect": {"sha256": "nope", "wat": 1}}], "pg": {"url": "postgresql://u:secret@h/db"}}' >"$WORK/invalid.json"
expect_run "invalid configuration" 2 "$WORK/invalid.json"
if grep -q 'name must match' "$WORK/err" && grep -q 'sql is required' "$WORK/err" && grep -q 'unknown expect keys wat' "$WORK/err" \
  && grep -q 'must not carry a password' "$WORK/err" && ! grep -q 'secret' "$WORK/err"; then
  pass "every configuration problem reported, password not echoed"
else
  fail "every configuration problem reported, password not echoed" "$(cat "$WORK/err")"
fi
expect_run "--check-config on a valid file" 0 "$WORK/ok.json" --check-config
printf 'not json' >"$WORK/garbage.json"
expect_run "configuration that is not JSON" 2 "$WORK/garbage.json"

printf '\n%d passed, %d failed\n' "$PASSED" "$FAILED"
[ "$FAILED" -eq 0 ]
