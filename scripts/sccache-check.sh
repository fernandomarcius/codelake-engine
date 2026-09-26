#!/usr/bin/env bash
# Proves the shared compilation cache cannot hand one checkout another's code.
# Runs inside the toolbelt with the cache on:
#
#   scripts/dev.sh scripts/sccache-check.sh
#
# Two "worktrees" at the same path (/tmp/sccache-check/w, as every worktree is
# /work in dev.sh), each with its own, empty target at the same path (as each
# worktree has its own target volume at /work/target), change the same file
# differently:
#   A  lib.rs says 1          -> binary prints 1 (cache miss)
#   B  same file says 2       -> binary prints 2 (cache miss; a hit = false green)
#   A' A again, new mtime     -> binary prints 1 (cache hit: keyed by content)
# A random token in both variants makes A and B new to the cache on every run.
set -euo pipefail

if [ "${RUSTC_WRAPPER:-}" != sccache ]; then
  echo "sccache-check: RUSTC_WRAPPER is not sccache (run through scripts/dev.sh with CODELAKE_SCCACHE=1)" >&2
  exit 2
fi

BASE=/tmp/sccache-check
TOKEN="$(od -An -N8 -tx8 /dev/urandom | tr -d ' ')"
rm -rf "$BASE"; mkdir -p "$BASE/w/lib/src" "$BASE/w/app/src"

write_tree() { # $1 = value the library holds
  cat > "$BASE/w/Cargo.toml" <<'EOF'
[workspace]
members = ["lib", "app"]
resolver = "2"
EOF
  cat > "$BASE/w/lib/Cargo.toml" <<'EOF'
[package]
name = "probe-lib"
version = "0.1.0"
edition = "2021"
EOF
  cat > "$BASE/w/app/Cargo.toml" <<'EOF'
[package]
name = "probe-app"
version = "0.1.0"
edition = "2021"

[dependencies]
probe-lib = { path = "../lib" }
EOF
  printf '// run %s\npub const VALUE: u32 = %s;\n' "$TOKEN" "$1" > "$BASE/w/lib/src/lib.rs"
  printf 'fn main() { println!("{}", probe_lib::VALUE); }\n' > "$BASE/w/app/src/main.rs"
}

count() { # $1 = hits|misses, from the server's stats since the last zero
  sccache --show-stats --stats-format json \
    | jq "[.stats.cache_${1}.counts // {} | to_entries[] | .value] | add // 0"
}

fail=0
step() { # $1 = name, $2 = value in lib.rs, $3 = expected hits|misses
  write_tree "$2"
  rm -rf "$BASE/w/target"
  sccache --zero-stats >/dev/null
  local out
  out="$(cd "$BASE/w" && cargo run --quiet -p probe-app 2>&1)"
  local hits misses
  hits="$(count hits)"; misses="$(count misses)"
  local verdict=ok
  [ "$out" = "$2" ] || verdict="WRONG (printed ${out}, source says $2)"
  if [ "$3" = hits ] && [ "$hits" -lt 1 ]; then verdict="$verdict; expected a cache hit"; fi
  if [ "$3" = misses ] && [ "$misses" -lt 1 ]; then verdict="$verdict; expected a cache miss"; fi
  echo "step $1: lib says $2, binary printed ${out}, hits=${hits} misses=${misses}: ${verdict}"
  [ "$verdict" = ok ] || fail=1
}

step A 1 misses
step B 2 misses
sleep 1   # a different mtime for the same content
step A2 1 hits

rm -rf "$BASE"
if [ "$fail" -ne 0 ]; then echo "sccache-check: FAILED"; exit 1; fi
echo "sccache-check: OK (content-keyed: different content missed, same content hit)"
