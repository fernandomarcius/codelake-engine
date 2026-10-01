#!/usr/bin/env python3
"""Differential gate: runs tools/differential/cases/*.toml against CodeLake and
their oracles (Trino 431, PostgreSQL 17, fixed values). docs/differential.md.

  scripts/dev.sh scripts/gates.sh differential          # builds the binary, then this
  python3 tools/differential/run.py [--only char,cursors] [--binary PATH] [--report PATH]

Exit: 0 every case as its file expects (known defects are listed, not hidden);
1 an unexpected outcome (a WRONG not marked known, a regression, or a known
defect that is now fixed) or a setup error; 2 the harness itself failed.
"""

import argparse
import json
import os
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from harness import cases, runner, stack  # noqa: E402

HERE = os.path.dirname(os.path.abspath(__file__))


def log(msg):
    print(f"differential: {msg}", flush=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cases", default=os.path.join(HERE, "cases"))
    ap.add_argument("--only", help="comma-separated case file names (without .toml)")
    ap.add_argument("--binary", default="target/debug/codelake")
    ap.add_argument("--report", default=os.path.join(HERE, "out", "report.json"))
    ap.add_argument("--check", action="store_true", help="validate the case files and exit")
    args = ap.parse_args()
    try:
        suites = cases.load(args.cases, args.only.split(",") if args.only else None)
    except (cases.CaseError, OSError, ValueError) as e:
        log(f"case files: {e}")
        return 2
    total = sum(len(s.cases) for s in suites)
    if args.check:
        log(f"{len(suites)} case files, {total} cases: valid")
        return 0
    if not os.path.exists(args.binary):
        log(f"no CodeLake binary at {args.binary} (cargo build -p codelake-server --bin codelake)")
        return 2
    need = set().union(*(s.needs() for s in suites)) if suites else {"codelake"}
    try:
        postgres_setup = [(s.name, sql) for s in suites for sql in s.setup_postgres]
        with stack.Stack(need, args.binary, log=log, postgres_setup=postgres_setup) as st:
            engines = runner.Engines(st.endpoints)
            try:
                engines.wait(log, alive=st.codelake_alive)
                rep = runner.run(suites, engines, log, binary=args.binary, setup_errors=st.setup_errors)
            except BaseException:
                log("CodeLake log tail:\n" + st.codelake_logs())
                raise
    except Exception:  # noqa: BLE001 - any harness failure is exit 2, with its trace
        traceback.print_exc()
        return 2
    os.makedirs(os.path.dirname(os.path.abspath(args.report)), exist_ok=True)
    with open(args.report, "w") as f:
        json.dump(rep, f, indent=1, ensure_ascii=False)
    print(runner.summary_text(rep), flush=True)
    log(f"report: {args.report}")
    return 1 if rep["summary"]["fail"] or rep["summary"]["setup_errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
