#!/usr/bin/env bash
# Single entry point for every quality gate. CI calls exactly these commands, so
# "green locally" and "green in CI" mean the same thing.
#
#   scripts/dev.sh scripts/gates.sh fast        # fmt, lint, unit tests, arch, supply chain
#   scripts/dev.sh scripts/gates.sh <gate>      # one gate
#
# Thresholds are documented in docs/gates.md. Changing one is a gate exception.
set -euo pipefail

MSRV="1.94.0"
COVERAGE_MIN_LINES=75
MUTATION_MIN_CORE=80   # codelake-core criticality: HIGH

gate_fmt()        { cargo fmt --all --check; }
gate_clippy()     { cargo clippy --workspace --all-targets --all-features --locked -- -D warnings; }
gate_test()       { cargo nextest run --workspace --all-features --locked --profile ci; }
gate_integration(){ cargo nextest run --workspace --all-features --locked --profile integration; }
gate_doc()        { RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked; }
gate_msrv()       { cargo "+${MSRV}" check --workspace --all-features --locked; }
gate_arch()       { cargo xtask arch; }
gate_deny()       { cargo deny check; }
gate_machete()    { cargo machete; }
gate_typos()      { typos; }

# Exactly one DataFusion and one Arrow major in the graph (docs/adr/0002).
gate_train() {
  local dupes
  dupes="$(cargo tree --workspace --all-features -e normal -d --depth 0 --locked \
           | grep -E '^(datafusion|arrow|parquet|object_store) v' || true)"
  if [ -n "$dupes" ]; then
    echo "DataFusion version train broken, duplicated crates:"; echo "$dupes"; return 1
  fi
  echo "version train: OK"
}

# Unit + integration lanes together: connectors are exercised against real
# databases only, so measuring the unit lane alone would under-report them.
gate_coverage() {
  cargo llvm-cov clean --workspace
  cargo llvm-cov nextest --no-report --workspace --all-features --locked --profile ci
  cargo llvm-cov nextest --no-report --workspace --all-features --locked --profile integration
  cargo llvm-cov report --lcov --output-path lcov.info
  cargo llvm-cov report --summary-only --fail-under-lines "${COVERAGE_MIN_LINES}"
}

# Mutation testing on codelake-core; fails below MUTATION_MIN_CORE percent caught.
gate_mutants() {
  local extra=()
  [ -n "${MUTANTS_BASE:-}" ] && { git diff "${MUTANTS_BASE}"... > /tmp/pr.diff; extra=(--in-diff /tmp/pr.diff); }
  cargo mutants --package codelake-core --no-shuffle "${extra[@]+"${extra[@]}"}" || true
  local caught missed timeout score
  caught=$(jq '[.outcomes[] | select(.summary=="CaughtMutant")] | length' mutants.out/outcomes.json)
  missed=$(jq '[.outcomes[] | select(.summary=="MissedMutant")] | length' mutants.out/outcomes.json)
  timeout=$(jq '[.outcomes[] | select(.summary=="Timeout")] | length' mutants.out/outcomes.json)
  if [ $((caught + missed)) -eq 0 ]; then echo "mutation: no viable mutants"; return 0; fi
  score=$(( (caught + timeout) * 100 / (caught + missed + timeout) ))
  echo "mutation score codelake-core: ${score}% (caught=${caught} timeout=${timeout} missed=${missed}, min ${MUTATION_MIN_CORE}%)"
  [ "$score" -ge "$MUTATION_MIN_CORE" ]
}

gate_sbom() { cargo cyclonedx --format json --override-filename sbom; }

gate_fast() {
  for g in fmt clippy typos arch train test doc deny machete; do
    echo "::group::gate ${g}"; "gate_${g}"; echo "::endgroup::"
  done
}

gate="${1:-fast}"
if ! declare -F "gate_${gate}" >/dev/null; then
  echo "unknown gate: ${gate}"; declare -F | sed -n 's/declare -f gate_/  /p'; exit 2
fi
"gate_${gate}"
