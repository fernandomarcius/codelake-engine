#!/usr/bin/env bash
# Production canary: runs a fixed list of queries against a CodeLake
# installation, checks each answer (value, row count, checksum, rows, bounds,
# or equality with a reference engine), its latency and optionally the
# freshness of a table, and prints one JSON line per run. Exits != 0 when any
# check fails, so cron, a systemd timer, a Kubernetes CronJob or an alerting
# pipeline can act on it. Runbook: docs/operations/canary.md.
#
#   scripts/ops/canary.sh --config canary.json                 # one run
#   scripts/ops/canary.sh --config canary.json --interval 60   # forever, every 60 s
#   scripts/ops/canary.sh --config canary.json --check-config  # validate and exit
#
# Requires bash (3.2 or newer), curl and jq; psql for checks over the
# PostgreSQL wire; sha256sum, shasum or openssl for checksums.
#
# Exit codes: 0 every check passed; 1 a check failed (wrong result, latency or
# freshness above the SLO, query error or timeout); 2 usage or configuration
# error.
#
# Secrets never go in the config file or on a command line: HTTP passwords are
# read from the variable named by `http.password_env`, PostgreSQL passwords
# from PGPASSWORD or a pgpass file.
set -uo pipefail

readonly EXIT_OK=0 EXIT_FAILED=1 EXIT_USAGE=2
readonly SOURCE_TAG="codelake-canary"
# The client protocol's header prefix. Configurable because the engine will
# make its own header names configurable (docs/issues/ISS-011).
readonly DEFAULT_HEADER_PREFIX="X-Trino-"
# Longest observed value copied into the JSON line.
readonly MAX_SHOWN=200

CONFIG="${CANARY_CONFIG:-}"
INTERVAL="${CANARY_INTERVAL_SECS:-}"
MAX_RUNS="${CANARY_MAX_RUNS:-}"
TEXTFILE="${CANARY_TEXTFILE:-}"
CHECK_ONLY=0

usage() {
  cat <<'EOF'
Usage: canary.sh --config FILE [--once | --interval SECS] [--max-runs N]
                 [--textfile PATH] [--check-config]

  --config FILE     JSON configuration (docs/examples/canary.json); env CANARY_CONFIG
  --once            run the checks once (default)
  --interval SECS   run forever, one run every SECS seconds; env CANARY_INTERVAL_SECS
  --max-runs N      with --interval, stop after N runs; env CANARY_MAX_RUNS
  --textfile PATH   also write Prometheus metrics for node_exporter's textfile
                    collector (atomic replace); env CANARY_TEXTFILE
  --check-config    validate the configuration and exit

Overrides: CANARY_NAME, CANARY_USER, CANARY_HTTP_URL, CANARY_PG_URL,
CANARY_REFERENCE_URL, CANARY_HEADER_PREFIX.
Exit codes: 0 all checks passed, 1 a check failed, 2 usage/configuration error.
EOF
}

die_usage() { printf 'canary: %s\n' "$*" >&2; exit "$EXIT_USAGE"; }
log() { printf 'canary %s: %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --config) [ $# -ge 2 ] || die_usage "--config needs a file"; CONFIG="$2"; shift 2 ;;
    --once) INTERVAL=""; shift ;;
    --interval) [ $# -ge 2 ] || die_usage "--interval needs seconds"; INTERVAL="$2"; shift 2 ;;
    --max-runs) [ $# -ge 2 ] || die_usage "--max-runs needs a number"; MAX_RUNS="$2"; shift 2 ;;
    --textfile) [ $# -ge 2 ] || die_usage "--textfile needs a path"; TEXTFILE="$2"; shift 2 ;;
    --check-config) CHECK_ONLY=1; shift ;;
    -h | --help) usage; exit "$EXIT_OK" ;;
    *) usage >&2; die_usage "unknown argument: $1" ;;
  esac
done

is_uint() { case "$1" in '' | *[!0-9]*) return 1 ;; *) return 0 ;; esac; }
is_number() { printf '%s' "$1" | grep -Eq '^-?[0-9]+(\.[0-9]+)?([eE][-+]?[0-9]+)?$'; }

[ -n "$CONFIG" ] || { usage >&2; die_usage "no configuration (--config or CANARY_CONFIG)"; }
[ -r "$CONFIG" ] || die_usage "cannot read configuration $CONFIG"
[ -z "$INTERVAL" ] || { is_uint "$INTERVAL" && [ "$INTERVAL" -gt 0 ]; } || die_usage "--interval must be a positive integer"
[ -z "$MAX_RUNS" ] || { is_uint "$MAX_RUNS" && [ "$MAX_RUNS" -gt 0 ]; } || die_usage "--max-runs must be a positive integer"
command -v jq >/dev/null 2>&1 || die_usage "jq is required"
command -v curl >/dev/null 2>&1 || die_usage "curl is required"

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum | cut -d' ' -f1; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 | cut -d' ' -f1; }
elif command -v openssl >/dev/null 2>&1; then
  sha256() { openssl dgst -sha256 -r | cut -d' ' -f1; }
else
  die_usage "sha256sum, shasum or openssl is required"
fi

# Milliseconds since the epoch. bash 5 has EPOCHREALTIME; older shells use perl
# (present on Linux and macOS), else whole seconds.
now_ms() {
  if [ -n "${EPOCHREALTIME:-}" ]; then
    local s="${EPOCHREALTIME%[.,]*}" f="${EPOCHREALTIME#*[.,]}000"
    printf '%s%s\n' "$s" "${f:0:3}"
  elif command -v perl >/dev/null 2>&1; then
    perl -MTime::HiRes=time -e 'printf "%d\n", time * 1000'
  else
    printf '%s000\n' "$(date +%s)"
  fi
}

WORK="$(mktemp -d "${TMPDIR:-/tmp}/codelake-canary.XXXXXX")" || die_usage "cannot create a temporary directory"
trap 'rm -rf "$WORK"' EXIT

# ---------------------------------------------------------------------------
# Configuration: load, apply environment overrides, validate everything at once.

CFG="$WORK/config.json"
if ! jq \
  --arg name "${CANARY_NAME:-}" --arg user "${CANARY_USER:-}" \
  --arg http "${CANARY_HTTP_URL:-}" --arg pg "${CANARY_PG_URL:-}" \
  --arg ref "${CANARY_REFERENCE_URL:-}" --arg prefix "${CANARY_HEADER_PREFIX:-}" '
  if type != "object" then error("the configuration must be a JSON object") else . end
  | if $name != "" then .name = $name else . end
  | if $user != "" then .user = $user else . end
  | if $http != "" then .http = ((.http // {}) + {url: $http}) else . end
  | if $pg != "" then .pg = ((.pg // {}) + {url: $pg}) else . end
  | if $ref != "" then .reference = ((.reference // {}) + {url: $ref}) else . end
  | if $prefix != "" then .http = ((.http // {}) + {header_prefix: $prefix}) else . end
' "$CONFIG" >"$CFG" 2>"$WORK/jq.err"; then
  die_usage "configuration $CONFIG is not valid JSON: $(head -c 300 "$WORK/jq.err")"
fi

# Every problem, one per line; empty when the configuration is valid.
PROBLEMS="$(jq -r '
  def pos: type == "number" and . > 0;
  def url_ok: type == "string" and test("^https?://[^/?#@ ]+(/[^?# ]*)?$");
  def pg_ok: type == "string" and test("^postgres(ql)?://");
  . as $root
  | (if (.checks | type) == "array" then .checks else [] end) as $checks
  | [
      (if (.checks | type) != "array" or ($checks | length) == 0 then "checks: at least one check is required" else empty end),
      (if has("user") and ((.user | type) != "string" or .user == "") then "user must be a non-empty string" else empty end),
      (if has("http") then
         (if (.http.url | url_ok) then empty else "http.url must be an http:// or https:// URL without credentials" end),
         (if (.http.password_env // "" | type) == "string" then empty else "http.password_env must be a variable name" end)
       else empty end),
      (if has("pg") then
         (if (.pg.url | pg_ok) then empty else "pg.url must be a postgresql:// URL" end),
         (if (.pg.url // "" | tostring | test("^postgres(ql)?://[^/@]*:[^/@]*@|[?&]password="; "i")) then
            "pg.url must not carry a password; use PGPASSWORD or a pgpass file" else empty end)
       else empty end),
      (if has("reference") then
         (if (.reference.url | url_ok) then empty else "reference.url must be an http:// or https:// URL without credentials" end)
       else empty end),
      (if has("defaults") then
         (if (.defaults.max_latency_ms // 1 | pos) then empty else "defaults.max_latency_ms must be > 0" end),
         (if (.defaults.timeout_secs // 1 | pos) then empty else "defaults.timeout_secs must be > 0" end),
         (if (.defaults.protocol // "http" | IN("http", "pg")) then empty else "defaults.protocol must be http or pg" end)
       else empty end),
      ($checks | to_entries[] | .key as $i | .value as $c
       | ("check " + ($i | tostring) + (if ($c.name | type) == "string" then " (" + $c.name + ")" else "" end)) as $at
       | if ($c | type) != "object" then "\($at): must be an object" else
         (if ($c.name | type) == "string" and ($c.name | test("^[A-Za-z0-9_.-]{1,64}$")) then empty
          else "\($at): name must match [A-Za-z0-9_.-]{1,64}" end),
         (if ($c.sql | type) == "string" and ($c.sql | test("\\S")) then empty else "\($at): sql is required" end),
         (($c.protocol // "") as $p
          | if $p == "" or $p == "http" or $p == "pg" then empty else "\($at): protocol must be http or pg" end),
         (if ($c | has("max_latency_ms")) and ($c.max_latency_ms | pos | not) then "\($at): max_latency_ms must be > 0" else empty end),
         (if ($c | has("timeout_secs")) and ($c.timeout_secs | pos | not) then "\($at): timeout_secs must be > 0" else empty end),
         (if ($c | has("expect")) then
            if ($c.expect | type) != "object" then "\($at): expect must be an object" else
              ($c.expect | keys - ["value", "row_count", "sha256", "rows", "min", "max", "reference", "ordered"]
               | if length > 0 then "\($at): unknown expect keys \(join(", "))" else empty end),
              (if ($c.expect | has("value")) and (($c.expect.value | type) | IN("string", "number", "boolean") | not)
               then "\($at): expect.value must be a string, number or boolean (a NULL cell is \"\\\\N\")" else empty end),
              (if ($c.expect | has("ordered")) and (($c.expect.ordered | type) != "boolean")
               then "\($at): expect.ordered must be true or false" else empty end),
              (if ($c.expect | has("reference")) and (($c.expect.reference | type) != "boolean")
               then "\($at): expect.reference must be true or false" else empty end),
              (if ($c.expect | has("row_count")) and (($c.expect.row_count | type) != "number" or $c.expect.row_count < 0)
               then "\($at): expect.row_count must be a number >= 0" else empty end),
              (if ($c.expect | has("sha256")) and (($c.expect.sha256 | tostring | test("^[0-9a-f]{64}$")) | not)
               then "\($at): expect.sha256 must be 64 lowercase hex digits" else empty end),
              (if ($c.expect | has("rows")) and (($c.expect.rows | type) != "array" or ([$c.expect.rows[] | type == "array"] | all | not))
               then "\($at): expect.rows must be an array of arrays" else empty end),
              (if ($c.expect | has("min")) and (($c.expect.min | type) != "number") then "\($at): expect.min must be a number" else empty end),
              (if ($c.expect | has("max")) and (($c.expect.max | type) != "number") then "\($at): expect.max must be a number" else empty end),
              (if ($c.expect.reference // false) == true and ($root | has("reference") | not)
               then "\($at): expect.reference needs a reference endpoint" else empty end)
            end
          else empty end),
         (if ($c | has("freshness")) then
            if ($c.freshness.max_lag_secs | pos) then empty else "\($at): freshness.max_lag_secs must be > 0" end
          else empty end)
       end)
    ]
  | . + ([$checks[] | objects | .name] | group_by(.) | map(select(length > 1) | "duplicate check name \(.[0])"))
  | .[]
' "$CFG" 2>&1)" || die_usage "cannot validate $CONFIG: $PROBLEMS"

# Protocol of each check, and the endpoint it needs.
while IFS= read -r line; do
  [ -n "$line" ] && PROBLEMS="${PROBLEMS:+$PROBLEMS
}$line"
done < <(jq -r '
  (if has("http") then "http" elif has("pg") then "pg" else "http" end) as $fallback
  | (.defaults.protocol // $fallback) as $default
  | . as $root
  | .checks // [] | .[] | objects
  | (.protocol // $default) as $p
  | if $p == "http" and ($root | has("http") | not) then "check \(.name): protocol http needs an http endpoint"
    elif $p == "pg" and ($root | has("pg") | not) then "check \(.name): protocol pg needs a pg endpoint"
    else empty end
' "$CFG" 2>/dev/null)

if [ -n "$PROBLEMS" ]; then
  printf 'canary: invalid configuration %s:\n' "$CONFIG" >&2
  printf '%s\n' "$PROBLEMS" | sed 's/^/  - /' >&2
  exit "$EXIT_USAGE"
fi

cfg() { jq -r "$1 // empty" "$CFG"; }

NAME="$(cfg '.name')"; NAME="${NAME:-codelake}"
USER_NAME="$(cfg '.user')"; USER_NAME="${USER_NAME:-canary}"
HTTP_URL="$(cfg '.http.url')"; HTTP_URL="${HTTP_URL%/}"
HTTP_CATALOG="$(cfg '.http.catalog')"
HTTP_SCHEMA="$(cfg '.http.schema')"
HTTP_TIME_ZONE="$(cfg '.http.time_zone')"
HTTP_PASSWORD_ENV="$(cfg '.http.password_env')"
HEADER_PREFIX="$(cfg '.http.header_prefix')"; HEADER_PREFIX="${HEADER_PREFIX:-$DEFAULT_HEADER_PREFIX}"
PG_URL="$(cfg '.pg.url')"
REF_URL="$(cfg '.reference.url')"; REF_URL="${REF_URL%/}"
REF_USER="$(cfg '.reference.user')"; REF_USER="${REF_USER:-$USER_NAME}"
REF_CATALOG="$(cfg '.reference.catalog')"; REF_CATALOG="${REF_CATALOG:-$HTTP_CATALOG}"
REF_SCHEMA="$(cfg '.reference.schema')"; REF_SCHEMA="${REF_SCHEMA:-$HTTP_SCHEMA}"
REF_PASSWORD_ENV="$(cfg '.reference.password_env')"
REF_HEADER_PREFIX="$(cfg '.reference.header_prefix')"; REF_HEADER_PREFIX="${REF_HEADER_PREFIX:-$DEFAULT_HEADER_PREFIX}"
DEFAULT_PROTOCOL="$(cfg '.defaults.protocol')"
if [ -z "$DEFAULT_PROTOCOL" ]; then
  if [ -n "$HTTP_URL" ]; then DEFAULT_PROTOCOL=http; else DEFAULT_PROTOCOL=pg; fi
fi
DEFAULT_MAX_LATENCY_MS="$(cfg '.defaults.max_latency_ms')"; DEFAULT_MAX_LATENCY_MS="${DEFAULT_MAX_LATENCY_MS:-300000}"
DEFAULT_TIMEOUT_SECS="$(cfg '.defaults.timeout_secs')"; DEFAULT_TIMEOUT_SECS="${DEFAULT_TIMEOUT_SECS:-600}"
CHECK_COUNT="$(jq '.checks | length' "$CFG")"

needs_pg="$(jq --arg d "$DEFAULT_PROTOCOL" '[.checks[] | select((.protocol // $d) == "pg")] | length' "$CFG")"
if [ "$needs_pg" -gt 0 ] && ! command -v psql >/dev/null 2>&1; then
  die_usage "psql is required for checks over the PostgreSQL wire"
fi
for var in "$HTTP_PASSWORD_ENV" "$REF_PASSWORD_ENV"; do
  [ -z "$var" ] && continue
  [ -n "${!var:-}" ] || die_usage "variable $var (password_env) is empty or unset"
done

if [ "$CHECK_ONLY" -eq 1 ]; then
  log "configuration $CONFIG is valid: $CHECK_COUNT checks"
  exit "$EXIT_OK"
fi

# ---------------------------------------------------------------------------
# HTTP client protocol: POST the statement, follow nextUri until it is gone.

# curl reading its options from stdin, so a password never shows in `ps`.
# $1 password variable name (may be empty), $2 user; rest: curl arguments.
curl_with_auth() {
  local pw_var="$1" user="$2"; shift 2
  if [ -n "$pw_var" ]; then
    printf 'user = "%s:%s"\n' "$user" "${!pw_var}" | curl -K - "$@"
  else
    curl "$@" </dev/null
  fi
}

# Runs $1 (SQL) against an HTTP endpoint. Writes one JSON array per row to $2.
# Sets HTTP_ERROR on failure. Arguments: sql rows_file base_url user catalog
# schema header_prefix password_var timeout_secs.
HTTP_ERROR=""
http_query() {
  local sql="$1" rows="$2" base="$3" user="$4" catalog="$5" schema="$6" prefix="$7" pw_var="$8" timeout="$9"
  local body="$WORK/body.json" sqlfile="$WORK/statement.sql" code next deadline now left attempt
  HTTP_ERROR=""
  : >"$rows"
  printf '%s' "$sql" >"$sqlfile"
  deadline=$(( $(now_ms) + timeout * 1000 ))
  local -a headers=(-H "${prefix}User: $user" -H "${prefix}Source: $SOURCE_TAG")
  [ -n "$catalog" ] && headers+=(-H "${prefix}Catalog: $catalog")
  [ -n "$schema" ] && headers+=(-H "${prefix}Schema: $schema")
  [ -n "$HTTP_TIME_ZONE" ] && headers+=(-H "${prefix}Time-Zone: $HTTP_TIME_ZONE")

  code="$(curl_with_auth "$pw_var" "$user" -sS -o "$body" -w '%{http_code}' --max-time "$timeout" \
    -X POST "${headers[@]}" -H 'Content-Type: text/plain; charset=utf-8' \
    --data-binary "@$sqlfile" "$base/v1/statement" 2>"$WORK/curl.err")" || {
    HTTP_ERROR="request failed: $(tr '\n' ' ' <"$WORK/curl.err" | head -c 300)"; return 1; }
  if [ "$code" != 200 ]; then
    HTTP_ERROR="HTTP $code on submit: $(head -c 300 "$body" | tr '\n' ' ')"; return 1
  fi
  while :; do
    if ! jq -e 'type == "object"' "$body" >/dev/null 2>&1; then
      HTTP_ERROR="response is not a JSON object"; return 1
    fi
    if jq -e '.error != null' "$body" >/dev/null; then
      HTTP_ERROR="$(jq -r '"\(.error.errorName // "ERROR"): \(.error.message // "no message")"' "$body" | head -c 500)"
      return 1
    fi
    jq -c '.data // [] | .[]' "$body" >>"$rows"
    next="$(jq -r '.nextUri // empty' "$body")"
    [ -z "$next" ] && return 0
    now="$(now_ms)"
    if [ "$now" -ge "$deadline" ]; then
      curl_with_auth "$pw_var" "$user" -sS -o /dev/null --max-time 10 -X DELETE "${headers[@]}" "$next" 2>/dev/null
      HTTP_ERROR="timeout after ${timeout}s; query canceled"; return 1
    fi
    left=$(( (deadline - now + 999) / 1000 ))
    # Retry what the protocol says is safe to retry (the same token answers
    # the same page).
    attempt=0
    while :; do
      code="$(curl_with_auth "$pw_var" "$user" -sS -o "$body" -w '%{http_code}' --max-time "$left" \
        "${headers[@]}" "$next" 2>"$WORK/curl.err")" || code="000"
      case "$code" in
        200) break ;;
        502 | 503 | 504)
          attempt=$((attempt + 1))
          [ "$attempt" -lt 5 ] || { HTTP_ERROR="HTTP $code while paging"; return 1; }
          sleep 1 ;;
        000)
          curl_with_auth "$pw_var" "$user" -sS -o /dev/null --max-time 10 -X DELETE "${headers[@]}" "$next" 2>/dev/null
          if [ "$(now_ms)" -ge "$deadline" ]; then
            HTTP_ERROR="timeout after ${timeout}s; query canceled"
          else
            HTTP_ERROR="paging failed: $(tr '\n' ' ' <"$WORK/curl.err" | head -c 300)"
          fi
          return 1 ;;
        *) HTTP_ERROR="HTTP $code while paging: $(head -c 300 "$body" | tr '\n' ' ')"; return 1 ;;
      esac
    done
  done
}

# Canonical text of the rows in $1 (one JSON array per line): one line per
# row, cells separated by tabs, NULL as \N, tab/newline/backslash escaped.
http_canonical() {
  jq -r 'map(if . == null then "\\N"
             elif type == "string" then gsub("\\\\"; "\\\\") | gsub("\t"; "\\t") | gsub("\n"; "\\n") | gsub("\r"; "\\r")
             else tojson end) | join("\t")' "$1"
}

PG_ERROR=""
# Runs $1 against the PostgreSQL wire and writes its rows to $2 in the same
# canonical text as http_canonical (unaligned, tab-separated, NULL as \N; a
# value holding a tab or a newline is not escaped by psql). $3: timeout in
# seconds, enforced with coreutils `timeout` when it is installed (closing the
# connection is the only way to stop a statement on this listener).
run_pg() {
  local sql="$1" out="$2" t="$3" status
  local -a limit=()
  PG_ERROR=""
  command -v timeout >/dev/null 2>&1 && limit=(timeout "$t")
  PGCONNECT_TIMEOUT="${PGCONNECT_TIMEOUT:-10}" ${limit[@]+"${limit[@]}"} \
    psql "$PG_URL" -X -q -A -t -F "$(printf '\t')" -P 'null=\N' -v ON_ERROR_STOP=1 \
    -c "$sql" >"$out" 2>"$WORK/psql.err" </dev/null
  status=$?
  [ "$status" -eq 0 ] && return 0
  if [ "$status" -eq 124 ] && [ "${#limit[@]}" -gt 0 ]; then
    PG_ERROR="timeout after ${t}s; connection closed"
  else
    PG_ERROR="$(tr '\n' ' ' <"$WORK/psql.err" | head -c 500)"
    PG_ERROR="${PG_ERROR:-psql exited with status $status}"
  fi
  return 1
}

# ---------------------------------------------------------------------------
# One run of every check.

shorten() { local v="$1"; [ "${#v}" -le "$MAX_SHOWN" ] || v="${v:0:$MAX_SHOWN}..."; printf '%s' "$v"; }
num_ge() { awk -v a="$1" -v b="$2" 'BEGIN { exit !((a + 0) >= (b + 0)) }'; }
num_gt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !((a + 0) > (b + 0)) }'; }

run_check() {
  local i="$1" out="$2"
  local c name sql protocol max_latency timeout ordered
  c="$(jq -c ".checks[$i]" "$CFG")"
  name="$(jq -r '.name' <<<"$c")"
  sql="$(jq -r '.sql' <<<"$c")"
  protocol="$(jq -r --arg d "$DEFAULT_PROTOCOL" '.protocol // $d' <<<"$c")"
  max_latency="$(jq -r --arg d "$DEFAULT_MAX_LATENCY_MS" '.max_latency_ms // ($d | tonumber)' <<<"$c")"
  timeout="$(jq -r --arg d "$DEFAULT_TIMEOUT_SECS" '.timeout_secs // ($d | tonumber) | ceil' <<<"$c")"
  ordered="$(jq -r '.expect.ordered // false' <<<"$c")"

  local rows="$WORK/rows.$i.json" canon="$WORK/canon.$i.txt" start end latency status="ok" message=""
  local error=""
  start="$(now_ms)"
  if [ "$protocol" = http ]; then
    if http_query "$sql" "$rows" "$HTTP_URL" "$USER_NAME" "$HTTP_CATALOG" "$HTTP_SCHEMA" \
      "$HEADER_PREFIX" "$HTTP_PASSWORD_ENV" "$timeout"; then
      http_canonical "$rows" >"$canon"
    else
      error="$HTTP_ERROR"
    fi
  else
    run_pg "$sql" "$canon" "$timeout" || error="$PG_ERROR"
  fi
  end="$(now_ms)"
  latency=$((end - start))

  local row_count=0 digest="" first="" line="" expected="" lag=""
  if [ -n "$error" ]; then
    status="error"; message="$error"
  else
    [ "$ordered" = true ] || { LC_ALL=C sort "$canon" >"$canon.sorted" && mv "$canon.sorted" "$canon"; }
    row_count="$(awk 'END { print NR }' "$canon")"
    digest="$(sha256 <"$canon")"
    first="$(head -n 1 "$canon" | cut -f1)"

    # Expectations, in order; the first mismatch is reported.
    if jq -e '.expect.row_count != null' <<<"$c" >/dev/null; then
      expected="$(jq -r '.expect.row_count' <<<"$c")"
      [ "$row_count" = "$expected" ] || { status="mismatch"; message="row count $row_count, expected $expected"; }
    fi
    if [ "$status" = ok ] && jq -e '.expect.value != null' <<<"$c" >/dev/null; then
      # One row; with several columns the value is the row's canonical text
      # (cells joined by tabs).
      expected="$(jq -r '.expect.value | if type == "string" then . else tojson end' <<<"$c")"
      line="$(head -n 1 "$canon")"
      if [ "$row_count" != 1 ]; then status="mismatch"; message="expected one row, got $row_count"
      elif [ "$line" != "$expected" ]; then
        status="mismatch"; message="value $(shorten "$line"), expected $(shorten "$expected")"
      fi
    fi
    if [ "$status" = ok ] && jq -e '.expect.sha256 != null' <<<"$c" >/dev/null; then
      expected="$(jq -r '.expect.sha256' <<<"$c")"
      [ "$digest" = "$expected" ] || { status="mismatch"; message="sha256 $digest, expected $expected"; }
    fi
    if [ "$status" = ok ] && jq -e '.expect.rows != null' <<<"$c" >/dev/null; then
      jq -c '.expect.rows[]' <<<"$c" >"$WORK/expected.$i.json"
      http_canonical "$WORK/expected.$i.json" >"$WORK/expected.$i.txt"
      [ "$ordered" = true ] || { LC_ALL=C sort "$WORK/expected.$i.txt" >"$WORK/expected.$i.s" && mv "$WORK/expected.$i.s" "$WORK/expected.$i.txt"; }
      if ! cmp -s "$canon" "$WORK/expected.$i.txt"; then
        status="mismatch"
        message="rows differ: $(diff "$WORK/expected.$i.txt" "$canon" | grep -E '^[<>]' | head -n 4 | tr '\n' ' ')"
        message="$(shorten "$message")"
      fi
    fi
    for bound in min max; do
      [ "$status" = ok ] || break
      jq -e ".expect.$bound != null" <<<"$c" >/dev/null || continue
      expected="$(jq -r ".expect.$bound" <<<"$c")"
      if [ "$row_count" != 1 ] || ! is_number "$first"; then
        status="mismatch"; message="expect.$bound needs one numeric value, got $(shorten "$first") in $row_count rows"
      elif [ "$bound" = min ] && ! num_ge "$first" "$expected"; then
        status="mismatch"; message="value $first below min $expected"
      elif [ "$bound" = max ] && ! num_ge "$expected" "$first"; then
        status="mismatch"; message="value $first above max $expected"
      fi
    done
    if [ "$status" = ok ] && [ "$(jq -r '.expect.reference // false' <<<"$c")" = true ]; then
      local ref_rows="$WORK/ref.$i.json" ref_canon="$WORK/ref.$i.txt" ref_digest ref_count
      if http_query "$sql" "$ref_rows" "$REF_URL" "$REF_USER" "$REF_CATALOG" "$REF_SCHEMA" \
        "$REF_HEADER_PREFIX" "$REF_PASSWORD_ENV" "$timeout"; then
        http_canonical "$ref_rows" >"$ref_canon"
        [ "$ordered" = true ] || { LC_ALL=C sort "$ref_canon" >"$ref_canon.s" && mv "$ref_canon.s" "$ref_canon"; }
        ref_digest="$(sha256 <"$ref_canon")"
        ref_count="$(awk 'END { print NR }' "$ref_canon")"
        expected="reference sha256 $ref_digest ($ref_count rows)"
        if [ "$ref_digest" != "$digest" ]; then
          status="mismatch"
          message="differs from the reference: $row_count rows, reference $ref_count; $(diff "$ref_canon" "$canon" | grep -E '^[<>]' | head -n 4 | tr '\n' ' ')"
          message="$(shorten "$message")"
        fi
      else
        status="error"; message="reference: $HTTP_ERROR"
      fi
    fi
    # Freshness: the first cell is the newest timestamp, in seconds since the
    # epoch (e.g. to_unixtime(max(ts))).
    if [ "$status" = ok ] && jq -e '.freshness != null' <<<"$c" >/dev/null; then
      local max_lag
      max_lag="$(jq -r '.freshness.max_lag_secs' <<<"$c")"
      if [ "$row_count" != 1 ] || ! is_number "$first"; then
        status="stale"; message="freshness needs one numeric epoch-seconds value, got $(shorten "$first")"
      else
        lag="$(awk -v now="$end" -v ts="$first" 'BEGIN { printf "%.3f", now / 1000 - ts }')"
        if num_gt "$lag" "$max_lag"; then
          status="stale"; message="newest row is ${lag}s old, SLO ${max_lag}s"
        fi
      fi
    fi
    if [ "$status" = ok ] && ! num_ge "$max_latency" "$latency"; then
      status="slow"; message="latency ${latency}ms above ${max_latency}ms"
    fi
  fi

  jq -cn --arg name "$name" --arg protocol "$protocol" --arg status "$status" \
    --argjson latency "$latency" --argjson max_latency "$max_latency" \
    --argjson rows "$row_count" --arg sha "$digest" --arg value "$(shorten "$first")" \
    --arg expected "$(shorten "$expected")" --arg message "$message" --arg lag "$lag" '
    {name: $name, protocol: $protocol, status: $status, latency_ms: $latency,
     max_latency_ms: $max_latency, rows: $rows}
    + (if $sha != "" then {sha256: $sha, value: $value} else {} end)
    + (if $expected != "" then {expected: $expected} else {} end)
    + (if $lag != "" then {freshness_lag_secs: ($lag | tonumber)} else {} end)
    + (if $message != "" then {message: $message} else {} end)' >>"$out"
}

write_textfile() {
  local results="$1" run_ok="$2" ts="$3" tmp
  tmp="$TEXTFILE.$$.tmp"
  {
    printf '# HELP codelake_canary_check_ok 1 when the check passed in the last run.\n'
    printf '# TYPE codelake_canary_check_ok gauge\n'
    jq -r --arg canary "$NAME" '"codelake_canary_check_ok{canary=\"\($canary)\",check=\"\(.name)\",status=\"\(.status)\"} \(if .status == "ok" then 1 else 0 end)"' "$results"
    printf '# HELP codelake_canary_latency_seconds Latency of the check in the last run.\n'
    printf '# TYPE codelake_canary_latency_seconds gauge\n'
    jq -r --arg canary "$NAME" '"codelake_canary_latency_seconds{canary=\"\($canary)\",check=\"\(.name)\"} \(.latency_ms / 1000)"' "$results"
    printf '# HELP codelake_canary_freshness_lag_seconds Age of the newest row of a freshness check.\n'
    printf '# TYPE codelake_canary_freshness_lag_seconds gauge\n'
    jq -r --arg canary "$NAME" 'select(.freshness_lag_secs != null) | "codelake_canary_freshness_lag_seconds{canary=\"\($canary)\",check=\"\(.name)\"} \(.freshness_lag_secs)"' "$results"
    printf '# HELP codelake_canary_run_ok 1 when every check passed in the last run.\n'
    printf '# TYPE codelake_canary_run_ok gauge\n'
    printf 'codelake_canary_run_ok{canary="%s"} %s\n' "$NAME" "$run_ok"
    printf '# HELP codelake_canary_last_run_timestamp_seconds When the last run finished.\n'
    printf '# TYPE codelake_canary_last_run_timestamp_seconds gauge\n'
    printf 'codelake_canary_last_run_timestamp_seconds{canary="%s"} %s\n' "$NAME" "$((ts / 1000))"
  } >"$tmp" && mv "$tmp" "$TEXTFILE" || log "cannot write $TEXTFILE"
}

RUN=0
run_once() {
  RUN=$((RUN + 1))
  local results="$WORK/results.jsonl" start end failed i
  : >"$results"
  start="$(now_ms)"
  i=0
  while [ "$i" -lt "$CHECK_COUNT" ]; do
    run_check "$i" "$results"
    i=$((i + 1))
  done
  end="$(now_ms)"
  failed="$(jq -s '[.[] | select(.status != "ok")] | length' "$results")"
  jq -cs --arg canary "$NAME" --argjson run "$RUN" --argjson duration "$((end - start))" \
    --arg time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --argjson failed "$failed" '
    {time: $time, canary: $canary, run: $run,
     status: (if $failed == 0 then "ok" else "fail" end),
     duration_ms: $duration, checks_total: length, checks_failed: $failed, checks: .}' "$results"
  if [ -n "$TEXTFILE" ]; then
    write_textfile "$results" "$([ "$failed" -eq 0 ] && echo 1 || echo 0)" "$end"
  fi
  [ "$failed" -eq 0 ]
}

if [ -z "$INTERVAL" ]; then
  run_once && exit "$EXIT_OK"
  exit "$EXIT_FAILED"
fi

# Periodic mode: one JSON line per run; the exit status is the last run's.
STOP=0
trap 'STOP=1' INT TERM
last="$EXIT_OK"
while [ "$STOP" -eq 0 ]; do
  started="$(now_ms)"
  if run_once; then last="$EXIT_OK"; else last="$EXIT_FAILED"; fi
  if [ -n "$MAX_RUNS" ] && [ "$RUN" -ge "$MAX_RUNS" ]; then break; fi
  wait_s=$(( INTERVAL - ($(now_ms) - started) / 1000 ))
  while [ "$wait_s" -gt 0 ] && [ "$STOP" -eq 0 ]; do sleep 1; wait_s=$((wait_s - 1)); done
done
exit "$last"
