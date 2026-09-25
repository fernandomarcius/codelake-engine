"""Self-test only: copies one table of a warehouse and injects a divergence.

    python -m codelake_shadow.mutate --from /wh/candidate --to /out/mut/x \\
        --table schema.table --mutation <name>

The table's rows are rewritten as one Parquet file by DuckDB, so every
mutation also changes the physical layout (file count, INT96 timestamps become
INT64, decimal encodings change): the ``neutral`` mutation checks that layout
alone is never reported as a divergence. Every other mutation changes exactly
one row (or adds/removes one) and must be detected.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys

import duckdb

from .compare import METASTORE_FILE, Location, data_files, find_table, quote_ident

# name -> (filter picking the row to change, SQL producing the changed table
# from `t` with its `__rn` row number and `__pick` = the chosen row).
MUTATIONS = {
    "neutral": "SELECT * EXCLUDE (__rn) FROM t",
    "extra_row": "SELECT * EXCLUDE (__rn) FROM t UNION ALL SELECT * EXCLUDE (__rn) FROM t WHERE __rn = __pick",
    "missing_row": "SELECT * EXCLUDE (__rn) FROM t WHERE __rn <> __pick",
    "decimal": "SELECT * EXCLUDE (__rn) REPLACE (CASE WHEN __rn = __pick THEN {col} + 0.000001 ELSE {col} END AS {col}) FROM t",
    "timestamp_3h": "SELECT * EXCLUDE (__rn) REPLACE (CASE WHEN __rn = __pick THEN {col} + INTERVAL 3 HOUR ELSE {col} END AS {col}) FROM t",
    "trailing_space": "SELECT * EXCLUDE (__rn) REPLACE (CASE WHEN __rn = __pick THEN {col} || ' ' ELSE {col} END AS {col}) FROM t",
    "null_to_empty": "SELECT * EXCLUDE (__rn) REPLACE (CASE WHEN __rn = __pick THEN '' ELSE {col} END AS {col}) FROM t",
}

# Which row a mutation changes: one where the column is set (or NULL).
PICK = {
    "decimal": "{col} IS NOT NULL",
    "timestamp_3h": "{col} IS NOT NULL",
    "trailing_space": "{col} IS NOT NULL",
    "null_to_empty": "{col} IS NULL",
}


def mutate(source: str, dest: str, table: str, mutation: str, column: str | None) -> None:
    schema, name = table.split(".", 1)
    src = find_table(duckdb.connect(), Location(source), schema, name)
    files = data_files(duckdb.connect(), src) if src is not None else []
    if not files:
        raise SystemExit(f"{table}: no data files under {source}")
    out_dir = Location(dest).table_dir(schema, name)
    if os.path.exists(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir)
    # The table's metastore file travels with it: declared types stay the
    # source's, only the data changes.
    for meta in (METASTORE_FILE, f".{METASTORE_FILE}.crc"):
        if os.path.isfile(os.path.join(src.path, meta)):
            shutil.copyfile(os.path.join(src.path, meta), os.path.join(out_dir, meta))
    con = duckdb.connect()
    listed = ", ".join("'" + f.replace("'", "''") + "'" for f in files)
    con.execute(
        f"CREATE TABLE t AS SELECT *, row_number() OVER () AS __rn "
        f"FROM read_parquet([{listed}], union_by_name = true)"
    )
    col = quote_ident(column) if column else ""
    condition = PICK.get(mutation, "true").format(col=col)
    pick = con.execute(f"SELECT min(__rn) FROM t WHERE {condition}").fetchone()[0]
    if pick is None:
        raise SystemExit(f"{table}: no row matches {condition} for mutation {mutation}")
    sql = MUTATIONS[mutation].format(col=col).replace("__pick", str(int(pick)))
    target = os.path.join(out_dir, "mutated.parquet")
    con.execute(f"COPY ({sql}) TO '{target}' (FORMAT PARQUET)")
    before = con.execute("SELECT count(*) FROM t").fetchone()[0]
    after = con.execute(f"SELECT count(*) FROM read_parquet('{target}')").fetchone()[0]
    print(f"{mutation}: {table} row {pick} ({before} rows -> {after})", file=sys.stderr)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="codelake_shadow.mutate")
    parser.add_argument("--from", dest="source", required=True)
    parser.add_argument("--to", dest="dest", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--mutation", required=True, choices=sorted(MUTATIONS))
    parser.add_argument("--column")
    args = parser.parse_args(argv)
    if "{col}" in MUTATIONS[args.mutation] and not args.column:
        parser.error(f"--mutation {args.mutation} needs --column")
    mutate(args.source, args.dest, args.table, args.mutation, args.column)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
