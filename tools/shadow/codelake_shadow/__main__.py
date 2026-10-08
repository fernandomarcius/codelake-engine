"""Shadow-run harness: the same dbt project against a reference engine and a
candidate engine, cycle after cycle, with every output table compared.

    python -m codelake_shadow run        # cycles, configured by SHADOW_* variables
    python -m codelake_shadow compare --reference-warehouse A --candidate-warehouse B \\
        --table schema.table [--table ...] [--out report.json]
    python -m codelake_shadow summary    # rebuild the cumulative report of SHADOW_OUT_DIR

Exit codes: 0 every table equal and every model with the same status on both
sides; 1 a divergence (or a dbt failure); 2 configuration error; 3 the
harness itself failed.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
import time
from typing import Any

from . import compare, dbt, expected, report
from .config import Config, ConfigError, digits, expected_path, load, time_zone
from .rss import Meter, MeterError, Sampler

EXIT_EQUAL, EXIT_DIVERGENT, EXIT_CONFIG, EXIT_HARNESS = 0, 1, 2, 3


def log(message: str) -> None:
    now = dt.datetime.now().astimezone().strftime("%H:%M:%S")
    print(f"[shadow {now}] {message}", file=sys.stderr, flush=True)


def _now() -> str:
    return dt.datetime.now().astimezone().isoformat(timespec="seconds")


def cutoff_value(config: Config) -> str | None:
    """The watermark of this cycle, read once and given to both sides."""
    if config.cutoff_value:
        return config.cutoff_value
    if not config.cutoff_sql:
        return None
    import trino  # installed with dbt-trino

    side = config.reference
    conn = trino.dbapi.connect(
        host=side.host,
        port=side.port,
        http_scheme=side.scheme,
        user=os.environ.get("SHADOW_CUTOFF_USER", "shadow"),
    )
    try:
        cur = conn.cursor()
        cur.execute(config.cutoff_sql)
        rows = cur.fetchall()
    finally:
        conn.close()
    if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] is None:
        raise RuntimeError("SHADOW_CUTOFF_SQL must return exactly one non-NULL value")
    value = rows[0][0]
    if isinstance(value, (bytes, bytearray)):
        return value.hex().upper()
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    return str(value)


def connection(config: Config, work: str):
    s3 = None
    if config.reference.warehouse.startswith("s3://") or config.candidate.warehouse.startswith("s3://"):
        s3 = {
            "key_id": os.environ.get("AWS_ACCESS_KEY_ID", ""),
            "secret": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "region": config.s3_region,
            "endpoint": config.s3_endpoint,
            "path_style": "true" if config.s3_path_style else "false",
        }
    return compare.connect(config.duckdb_memory, os.path.join(work, "duckdb-tmp"), s3)


def meter(side) -> Meter | None:
    if not side.rss:
        return None
    return Meter(side.rss, container_id=side.container_id)


def model_status_differences(ref: dict[str, str], cand: dict[str, str]) -> list[dict[str, str]]:
    out = []
    for node in sorted(set(ref) | set(cand)):
        a, b = ref.get(node, "not run"), cand.get(node, "not run")
        if a != b:
            out.append({"node": node, "reference": a, "candidate": b})
    return out


def run_cycle(config: Config, number: int, project: str) -> dict[str, Any]:
    work = os.path.join(config.out_dir, f"cycle-{number:04d}")
    cycle: dict[str, Any] = {"cycle": number, "started_at": _now(), "cutoff_var": config.cutoff_var}
    cutoff = cutoff_value(config)
    cycle["cutoff"] = cutoff
    log(f"cycle {number}: cutoff {config.cutoff_var}={cutoff}")
    full_refresh = config.full_refresh == "always" or (config.full_refresh == "first" and number == 1)
    clock_vars = dbt.clock(config, dt.datetime.now(dt.timezone.utc))
    if clock_vars:
        cycle["clock"] = clock_vars
        log(f"cycle {number}: clock {clock_vars[config.clock_var]} UTC")
    runs = {}
    cycle["sides"] = {}
    for side in (config.reference, config.candidate):
        log(f"cycle {number}: dbt {config.dbt_command} on {side.name} (target {side.target})")
        sampler = Sampler(meter(side), config.rss_interval_ms / 1000)
        with sampler:
            runs[side.name] = dbt.run(
                config, side, project, os.path.join(work, side.name), cutoff, full_refresh, clock_vars
            )
        cycle["sides"][side.name] = {"dbt": dbt.summary(runs[side.name]), "rss": sampler.result()}
        log(
            f"cycle {number}: {side.name} exit {runs[side.name].exit_code} in "
            f"{runs[side.name].seconds:.1f}s, peak RSS {sampler.peak}"
        )
    ref_status = dbt.statuses(runs["reference"].artifact("run_results.json"))
    cand_status = dbt.statuses(runs["candidate"].artifact("run_results.json"))
    cycle["model_status_differences"] = model_status_differences(ref_status, cand_status)

    if config.tables:
        # Validated as schema.table by config.load.
        tables = [(t.split(".", 1)[0], t.split(".", 1)[1], "") for t in config.tables]
        skipped: list[dict[str, str]] = []
    else:
        merged = {**cand_status, **ref_status}
        tables, skipped = dbt.output_tables(runs["reference"].artifact("manifest.json"), merged, config.output_catalogs)
    cycle["skipped_models"] = skipped
    options = compare.Options(
        config.ignore_columns,
        config.sample,
        config.show_rows,
        config.expected,
        config.parquet_time_zone,
        config.double_digits,
        config.real_digits,
        config.float_recheck_limit,
    )
    cycle["expected_differences_file"] = config.expected_differences or None
    con = connection(config, work)
    results = []
    try:
        for schema, table, _uid in tables:
            try:
                results.append(
                    compare.compare_table(
                        con,
                        compare.Location(config.reference.warehouse, config.reference.metastore_file),
                        compare.Location(config.candidate.warehouse, config.candidate.metastore_file),
                        schema,
                        table,
                        options,
                    )
                )
            except Exception as err:  # noqa: BLE001 - one table's failure must not hide the others
                results.append({"table": f"{schema}.{table}", "status": "error", "error": type(err).__name__})
    finally:
        con.close()
    cycle["tables"] = results
    ref_peak = cycle["sides"]["reference"]["rss"]["peak_bytes"]
    cand_peak = cycle["sides"]["candidate"]["rss"]["peak_bytes"]
    cycle["rss_ratio"] = (cand_peak / ref_peak) if ref_peak and cand_peak is not None else None
    failures = []
    if any(r["status"] != "equal" for r in results):
        failures.append("tables")
    if cycle["model_status_differences"]:
        failures.append("model_statuses")
    if any(runs[s].exit_code != 0 for s in runs):
        failures.append("dbt_exit")
    if not results:
        failures.append("no_tables_compared")
    if config.rss_max_ratio is not None:
        # A configured memory gate that could not measure both sides fails
        # the cycle: an unmeasured side never passes it.
        if cycle["rss_ratio"] is None or not cand_peak:
            failures.append("rss_unmeasured")
        elif cycle["rss_ratio"] > config.rss_max_ratio:
            failures.append("rss_ratio")
    cycle["failures"] = failures
    cycle["verdict"] = "equal" if not failures else "divergent"
    cycle["finished_at"] = _now()
    return cycle


def write_cycle(config: Config, cycle: dict[str, Any]) -> None:
    base = os.path.join(config.out_dir, f"cycle-{cycle['cycle']:04d}")
    report.write_json(base + ".json", cycle)
    report.write_text(base + ".md", report.cycle_markdown(cycle))
    summary = report.cumulative(config.out_dir)
    report.write_json(os.path.join(config.out_dir, "summary.json"), summary)
    report.write_text(os.path.join(config.out_dir, "summary.md"), report.cumulative_markdown(summary))


def cmd_run() -> int:
    config = load()
    for side in (config.reference, config.candidate):
        meter(side)  # validate the spec before any dbt run
    os.makedirs(config.out_dir, exist_ok=True)
    project = dbt.freeze_project(config.project_dir, os.path.join(config.out_dir, "project"))
    verdict = EXIT_EQUAL
    first = report.next_cycle_number(config.out_dir)
    for index in range(config.cycles):
        if index and config.interval_secs:
            log(f"waiting {config.interval_secs:.0f}s before the next cycle")
            time.sleep(config.interval_secs)
        cycle = run_cycle(config, first + index, project)
        write_cycle(config, cycle)
        divergent = [t["table"] for t in cycle["tables"] if t["status"] != "equal"]
        log(
            f"cycle {cycle['cycle']}: {cycle['verdict']} — {len(cycle['tables'])} tables, "
            f"{len(divergent)} divergent{': ' + ', '.join(divergent) if divergent else ''}; "
            f"failures: {', '.join(cycle['failures']) or 'none'}"
        )
        if cycle["verdict"] != "equal":
            verdict = EXIT_DIVERGENT
    summary = report.cumulative(config.out_dir)
    log(f"cumulative: {summary['cycles']} cycles, verdict {summary['verdict']}; report {config.out_dir}/summary.md")
    return verdict


def cmd_compare(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="codelake_shadow compare")
    parser.add_argument("--reference-warehouse", required=True)
    parser.add_argument("--candidate-warehouse", required=True)
    parser.add_argument("--reference-metastore-file", default=compare.METASTORE_FILE)
    parser.add_argument("--candidate-metastore-file", default=compare.CANDIDATE_METASTORE_FILE)
    parser.add_argument("--table", action="append", required=True, help="schema.table (repeatable)")
    parser.add_argument("--ignore-column", action="append", default=[])
    parser.add_argument("--sample", type=int, default=10)
    parser.add_argument("--show-rows", action="store_true")
    parser.add_argument(
        "--parquet-time-zone",
        default="",
        help="IANA zone the hive catalogs convert INT96 timestamps with (needed to compare INT96 columns)",
    )
    parser.add_argument(
        "--double-digits",
        type=int,
        default=compare.DOUBLE_DIGITS,
        help="significant digits doubles are compared to (0: exact text)",
    )
    parser.add_argument(
        "--real-digits",
        type=int,
        default=compare.REAL_DIGITS,
        help="significant digits reals are compared to (0: exact text)",
    )
    parser.add_argument(
        "--float-recheck-limit",
        type=int,
        default=compare.FLOAT_RECHECK_LIMIT,
        help="most rows per side paired again at a float rounding boundary (0: none)",
    )
    parser.add_argument("--memory", default="2GB")
    parser.add_argument("--work", default="/tmp/codelake-shadow")
    parser.add_argument("--out", help="write the JSON result here")
    parser.add_argument(
        "--expected",
        default="",
        help="expected-differences file; `default` for the shipped one (default: no rule)",
    )
    args = parser.parse_args(argv)
    rules = expected.load(expected_path(args.expected))
    zone = time_zone(args.parquet_time_zone, "--parquet-time-zone")
    double_digits = digits(str(args.double_digits), "--double-digits", 17)
    real_digits = digits(str(args.real_digits), "--real-digits", 9)
    options = compare.Options(
        args.ignore_column,
        args.sample,
        args.show_rows,
        rules,
        zone,
        double_digits,
        real_digits,
        max(args.float_recheck_limit, 0),
    )
    con = compare.connect(args.memory, os.path.join(args.work, "duckdb-tmp"))
    results = []
    try:
        for name in args.table:
            if len(name.split(".")) != 2 or not all(name.split(".")):
                print(f"--table {name}: expected schema.table", file=sys.stderr)
                return EXIT_CONFIG
            schema, table = name.split(".", 1)
            results.append(
                compare.compare_table(
                    con,
                    compare.Location(args.reference_warehouse, args.reference_metastore_file),
                    compare.Location(args.candidate_warehouse, args.candidate_metastore_file),
                    schema,
                    table,
                    options,
                )
            )
    finally:
        con.close()
    verdict = "equal" if all(r["status"] == "equal" for r in results) else "divergent"
    data = {"verdict": verdict, "tables": results}
    if args.out:
        report.write_json(args.out, data)
    for r in results:
        cols = ", ".join(c["column"] for c in r.get("columns_differing", []))
        reasons = ", ".join(r.get("reasons", []))
        rules = ", ".join(sorted({e["rule"] for e in r.get("expected_differences", [])}))
        print(
            f"{r['table']}: {r['status']}"
            + (f" ({reasons})" if reasons else "")
            + (f" columns: {cols}" if cols else "")
            + (f" expected: {rules}" if rules else "")
            + (f" error: {r['error']}" if r.get("error") else "")
        )
    return EXIT_EQUAL if verdict == "equal" else EXIT_DIVERGENT


def cmd_summary() -> int:
    out_dir = os.environ.get("SHADOW_OUT_DIR", "")
    if not out_dir:
        print("SHADOW_OUT_DIR is required", file=sys.stderr)
        return EXIT_CONFIG
    summary = report.cumulative(out_dir)
    report.write_json(os.path.join(out_dir, "summary.json"), summary)
    report.write_text(os.path.join(out_dir, "summary.md"), report.cumulative_markdown(summary))
    print(report.cumulative_markdown(summary))
    return EXIT_EQUAL if summary["verdict"] == "equal" else EXIT_DIVERGENT


def main(argv: list[str]) -> int:
    if not argv or argv[0] in {"-h", "--help"}:
        print(__doc__)
        return EXIT_EQUAL if argv else EXIT_CONFIG
    command, rest = argv[0], argv[1:]
    try:
        if command == "run":
            return cmd_run()
        if command == "compare":
            return cmd_compare(rest)
        if command == "summary":
            return cmd_summary()
    except (ConfigError, MeterError, expected.ExpectedError) as err:
        print(str(err), file=sys.stderr)
        return EXIT_CONFIG
    except Exception as err:  # noqa: BLE001 - reported as a harness failure, without values
        print(f"shadow harness failed: {type(err).__name__}: {err}", file=sys.stderr)
        return EXIT_HARNESS
    print(f"unknown command {command}; see --help", file=sys.stderr)
    return EXIT_CONFIG


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
