"""Table-by-table comparison of two warehouses with DuckDB over Parquet.

Each table is read from ``<warehouse>/<schema>/<table>/`` (the file-metastore
layout: one directory per schema and per table; dotfiles and ``_``-prefixed
files are metadata or in-flight writes and are skipped).

Per table and side, one pass computes:

- the row count;
- a checksum that ignores row order and counts duplicates: the sum of a
  128-bit hash (two 64-bit halves of MD5) of each row's canonical text;
- one checksum per column (sum of the hash of that column's canonical
  value), which says *which* columns differ when the row checksums do;
- the count of NULLs per column.

Canonical text of a value (see ``canonical_expr``):

- NULL is ``~``, distinct from every non-NULL value (so NULL != '');
- a string is itself, exactly: no trimming, no case folding;
- integers and decimals by value: ``12.500000`` and ``12.5`` are equal;
- timestamps as microseconds since the epoch, naive values read as UTC;
- binary as hex; dates, times, booleans and floats as their exact text.

Every non-NULL value is length-prefixed and tagged with its class, so the
concatenation of a row's values is unambiguous.

Rows that differ are shown only as hashes with their count on each side.
Their values are printed only with ``show_rows`` (opt-in), because the
tables hold production data.

Declared types come from the table's metastore file on both sides (the
file-metastore's JSON, ``dataColumns``); without it on either side, the
Parquet types stand for them. A declared type that differs is a divergence
unless an expected-difference rule documents it (``expected.py``); so is a
table or column name that differs only by case. Parquet storage types that
differ under equal declared types (INT96 vs INT64 timestamps) are reported,
not a divergence: the values are compared by class either way.

Table directories are found without regard to case. Partitioned tables (files
in subdirectories, or partition columns in the metastore) are refused, never
compared as empty.
"""

from __future__ import annotations

import dataclasses
import fnmatch
import json
import os
import re
from typing import Any

import duckdb

from . import expected as expected_rules

# Table metadata file of the file metastore: a compatibility name, the layout
# both engines write (docs/adr/0007).
METASTORE_FILE = ".trinoSchema"

# DuckDB type families, by the prefix of the type name DESCRIBE reports.
_INTEGER_TYPES = {
    "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT",
    "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT",
}
_FLOAT_TYPES = {"FLOAT", "DOUBLE", "REAL"}


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def type_class(duck_type: str) -> str:
    """Comparison class of a DuckDB type: N, F, B, T, D, H, X, S or O."""
    t = duck_type.upper()
    base = t.split("(")[0].strip()
    if base in _INTEGER_TYPES or base == "DECIMAL":
        return "N"
    if base in _FLOAT_TYPES:
        return "F"
    if base == "BOOLEAN":
        return "B"
    if base.startswith("TIMESTAMP"):
        return "T"
    if base == "DATE":
        return "D"
    if base.startswith("TIME"):
        return "H"
    if base in {"BLOB", "BYTEA", "VARBINARY", "BINARY"}:
        return "X"
    if base in {"VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR"}:
        return "S"
    return "O"


def _micros(col: str, duck_type: str) -> str:
    """Microseconds since the epoch, floored, for any timestamp type."""
    t = duck_type.upper()
    if t.startswith("TIMESTAMP_NS"):
        ns = f"epoch_ns({col})"
        return f"(({ns} - ((({ns} % 1000) + 1000) % 1000)) // 1000)"
    if t.startswith("TIMESTAMP_MS"):
        return f"(epoch_ms({col}) * 1000)"
    if t.startswith("TIMESTAMP_S"):
        return f"(epoch({col})::BIGINT * 1000000)"
    # TIMESTAMP (microseconds) and TIMESTAMP WITH TIME ZONE (UTC instant).
    return f"epoch_us({col})"


def canonical_expr(name: str, duck_type: str) -> str:
    """SQL for the canonical text of one column; never NULL."""
    col = quote_ident(name)
    cls = type_class(duck_type)
    if cls == "N":
        text = f"CAST({col} AS VARCHAR)"
        # Drop trailing zeros of the fraction, then a bare point; -0 is 0.
        text = (
            f"regexp_replace(regexp_replace({text}, '(\\.[0-9]*?)0+$', '\\1'), '\\.$', '')"
        )
        text = f"(CASE WHEN {text} = '-0' THEN '0' ELSE {text} END)"
    elif cls == "F":
        text = f"CAST({col} AS VARCHAR)"
    elif cls == "B":
        text = f"(CASE WHEN {col} THEN '1' ELSE '0' END)"
    elif cls == "T":
        text = f"CAST({_micros(col, duck_type)} AS VARCHAR)"
    elif cls == "X":
        text = f"hex({col})"
    else:
        text = f"CAST({col} AS VARCHAR)"
    value = f"'{cls}' || {text}"
    return (
        f"(CASE WHEN {col} IS NULL THEN '~' "
        f"ELSE CAST(length({value}) AS VARCHAR) || ':' || {value} END)"
    )


class TableError(ValueError):
    """A table that cannot be compared (the message never holds values)."""


@dataclasses.dataclass
class Location:
    """Where one side's tables are: a local directory or an s3:// prefix."""

    warehouse: str

    def table_dir(self, schema: str, table: str) -> str:
        return self.warehouse.rstrip("/") + "/" + schema.lower() + "/" + table.lower()

    @property
    def is_s3(self) -> bool:
        return self.warehouse.startswith("s3://")


@dataclasses.dataclass
class TableDir:
    """A table's directory, with the schema and table names as written on disk."""

    path: str
    schema: str
    table: str


def _visible(name: str) -> bool:
    return not (name.startswith(".") or name.startswith("_"))


def _entry(parent: str, name: str) -> str | None:
    """The one directory entry of ``parent`` named ``name`` in any case."""
    try:
        found = [e for e in os.listdir(parent) if e.lower() == name.lower() and os.path.isdir(os.path.join(parent, e))]
    except OSError:
        return None
    if len(found) > 1:
        raise TableError("two directories whose names differ only by case")
    return found[0] if found else None


def find_table(con: duckdb.DuckDBPyConnection, location: Location, schema: str, table: str) -> TableDir | None:
    """The table's directory, matched without regard to case; None when absent."""
    root = location.warehouse.rstrip("/")
    if location.is_s3:
        for s, t in dict.fromkeys(((schema, table), (schema.lower(), table.lower()))):
            path = f"{root}/{s}/{t}"
            if con.execute("SELECT count(*) FROM glob(?)", [path + "/**"]).fetchone()[0]:
                return TableDir(path, s, t)
        return None
    schema_name = _entry(root, schema)
    if schema_name is None:
        return None
    table_name = _entry(os.path.join(root, schema_name), table)
    if table_name is None:
        return None
    return TableDir(os.path.join(root, schema_name, table_name), schema_name, table_name)


def data_files(con: duckdb.DuckDBPyConnection, directory: TableDir) -> list[str]:
    """Data files of a table directory. A partitioned table is refused."""
    if directory.path.startswith("s3://"):
        rows = con.execute("SELECT file FROM glob(?)", [directory.path + "/**"]).fetchall()
        files = []
        for (path,) in rows:
            relative = path[len(directory.path) + 1 :]
            if "/" in relative and _visible(relative.split("/", 1)[0]):
                raise TableError("partitioned table (files in subdirectories): not compared")
            if "/" not in relative and _visible(relative):
                files.append(path)
        return sorted(files)
    files = []
    for name in os.listdir(directory.path):
        path = os.path.join(directory.path, name)
        if not _visible(name):
            continue
        if os.path.isdir(path):
            raise TableError("partitioned table (files in subdirectories): not compared")
        if os.path.isfile(path):
            files.append(path)
    return sorted(files)


def declared_columns(con: duckdb.DuckDBPyConnection, directory: TableDir) -> dict[str, tuple[str, str]] | None:
    """Declared columns of the table's metastore file: lower-case name ->
    (name as written, type). None when the file is absent or unreadable."""
    path = directory.path.rstrip("/") + "/" + METASTORE_FILE
    try:
        if directory.path.startswith("s3://"):
            row = con.execute("SELECT content FROM read_text(?)", [path]).fetchone()
            text = row[0] if row else None
        else:
            with open(path, encoding="utf-8") as f:
                text = f.read()
        metadata = json.loads(text) if text else None
    except (OSError, ValueError, duckdb.Error):
        return None
    if not isinstance(metadata, dict) or not isinstance(metadata.get("dataColumns"), list):
        return None
    if metadata.get("partitionColumns"):
        raise TableError("partitioned table (partition columns in the metastore): not compared")
    columns: dict[str, tuple[str, str]] = {}
    for column in metadata["dataColumns"]:
        if isinstance(column, dict) and isinstance(column.get("name"), str) and isinstance(column.get("type"), str):
            columns[column["name"].lower()] = (column["name"], column["type"])
    return columns


def _relation(files: list[str]) -> str:
    listed = ", ".join(quote_literal(f) for f in files)
    return f"read_parquet([{listed}], union_by_name = true)"


def describe(con: duckdb.DuckDBPyConnection, files: list[str]) -> dict[str, tuple[str, str]]:
    """Column name (lower case) -> (name as written in the files, DuckDB type)."""
    rows = con.execute(f"DESCRIBE SELECT * FROM {_relation(files)}").fetchall()
    return {r[0].lower(): (r[0], r[1]) for r in rows}


def ignored(table: str, column: str, patterns: list[str]) -> bool:
    """Whether ``schema.table.column`` (or ``table.column``) matches a pattern."""
    for pattern in patterns:
        pattern = pattern.strip().lower()
        if not pattern:
            continue
        parts = pattern.split(".")
        if len(parts) == 3:
            target = f"{table}.{column}"
        elif len(parts) == 2:
            target = f"{table.split('.', 1)[-1]}.{column}"
        else:
            target = column
        if fnmatch.fnmatchcase(target, pattern):
            return True
    return False


def _row_expr(columns: list[tuple[str, str]]) -> str:
    if not columns:
        return "''"
    parts = [f"{quote_literal(name + '=')} || {canonical_expr(name, t)}" for name, t in columns]
    return " || '|' || ".join(parts)


def _column_list(names: list[str], types: dict[str, str]) -> list[tuple[str, str]]:
    return [(n, types[n]) for n in names]


@dataclasses.dataclass
class SideStats:
    rows: int
    checksum: str
    columns: dict[str, str]
    nulls: dict[str, int]


def side_stats(
    con: duckdb.DuckDBPyConnection, files: list[str], columns: list[tuple[str, str]]
) -> SideStats:
    row = _row_expr(columns)
    col_sums = []
    null_counts = []
    for i, (name, t) in enumerate(columns):
        piece = f"{quote_literal(name + '=')} || {canonical_expr(name, t)}"
        col_sums.append(f"sum(md5_number_lower({piece}))::VARCHAR AS c{i}")
        null_counts.append(f"count(*) FILTER (WHERE {quote_ident(name)} IS NULL) AS n{i}")
    select = ", ".join(
        [
            "count(*) AS n",
            f"coalesce(sum(md5_number_lower({row})), 0)::VARCHAR AS lo",
            f"coalesce(sum(md5_number_upper({row})), 0)::VARCHAR AS hi",
        ]
        + col_sums
        + null_counts
    )
    result = con.execute(f"SELECT {select} FROM {_relation(files)}").fetchone()
    assert result is not None
    n, lo, hi = result[0], result[1], result[2]
    k = len(columns)
    sums = result[3 : 3 + k]
    nulls = result[3 + k : 3 + 2 * k]
    return SideStats(
        rows=int(n),
        checksum=f"{lo}:{hi}",
        columns={name: str(s or 0) for (name, _), s in zip(columns, sums)},
        nulls={name: int(c) for (name, _), c in zip(columns, nulls)},
    )


def differing_rows(
    con: duckdb.DuckDBPyConnection,
    ref_files: list[str],
    cand_files: list[str],
    columns_ref: list[tuple[str, str]],
    columns_cand: list[tuple[str, str]],
    limit: int,
    show_rows: bool,
) -> tuple[int, list[dict[str, Any]]]:
    """Hashes whose row count differs between sides: (total, sample)."""
    ref_row = _row_expr(columns_ref)
    cand_row = _row_expr(columns_cand)
    keep = ", any_value(r) AS r" if show_rows else ""
    sql = f"""
        WITH ref AS (
            SELECT md5(r) AS h, count(*) AS n{keep}
            FROM (SELECT {ref_row} AS r FROM {_relation(ref_files)}) GROUP BY 1
        ), cand AS (
            SELECT md5(r) AS h, count(*) AS n{keep}
            FROM (SELECT {cand_row} AS r FROM {_relation(cand_files)}) GROUP BY 1
        ), diff AS (
            SELECT coalesce(ref.h, cand.h) AS h,
                   coalesce(ref.n, 0) AS ref_n,
                   coalesce(cand.n, 0) AS cand_n
                   {", coalesce(ref.r, cand.r) AS r" if show_rows else ""}
            FROM ref FULL OUTER JOIN cand ON ref.h = cand.h
            WHERE coalesce(ref.n, 0) <> coalesce(cand.n, 0)
        )
        SELECT (SELECT count(*) FROM diff) AS total, * FROM diff ORDER BY h LIMIT {int(limit)}
    """
    rows = con.execute(sql).fetchall()
    total = int(rows[0][0]) if rows else 0
    sample = []
    for r in rows:
        item: dict[str, Any] = {"hash": r[1], "reference": int(r[2]), "candidate": int(r[3])}
        if show_rows:
            item["row"] = r[4]
        sample.append(item)
    return total, sample


@dataclasses.dataclass
class Options:
    ignore_columns: list[str] = dataclasses.field(default_factory=list)
    sample: int = 10
    show_rows: bool = False
    expected: list[expected_rules.Rule] = dataclasses.field(default_factory=list)


def _classify(
    options: Options,
    kind: str,
    table: str,
    column: str,
    item: dict[str, Any],
    expected_out: list[dict[str, Any]],
    unexpected_out: list[dict[str, Any]],
) -> None:
    """Files a type or name-case difference as expected (a rule documents it)
    or unexpected (a divergence)."""
    rule = expected_rules.match(
        options.expected, kind, table, column, item.get("reference", ""), item.get("candidate", "")
    )
    if rule is None:
        unexpected_out.append(item)
    else:
        expected_out.append(dict(item, kind=kind, rule=rule.id, reason=rule.reason))


def compare_table(
    con: duckdb.DuckDBPyConnection,
    reference: Location,
    candidate: Location,
    schema: str,
    table: str,
    options: Options,
) -> dict[str, Any]:
    """Compares one table; the result is JSON-ready."""
    name = f"{schema.lower()}.{table.lower()}"
    out: dict[str, Any] = {"table": name}
    try:
        return _compare_table(con, reference, candidate, schema, table, options, out)
    except TableError as err:
        out["status"] = "error"
        out["error"] = str(err)
        return out


def _compare_table(
    con: duckdb.DuckDBPyConnection,
    reference: Location,
    candidate: Location,
    schema: str,
    table: str,
    options: Options,
    out: dict[str, Any],
) -> dict[str, Any]:
    name = out["table"]
    ref_dir = find_table(con, reference, schema, table)
    cand_dir = find_table(con, candidate, schema, table)
    if ref_dir is None or cand_dir is None:
        if ref_dir is None and cand_dir is None:
            out["status"] = "missing_both"
        else:
            out["status"] = "missing_reference" if ref_dir is None else "missing_candidate"
        return out
    ref_files = data_files(con, ref_dir)
    cand_files = data_files(con, cand_dir)
    ref_declared = declared_columns(con, ref_dir)
    cand_declared = declared_columns(con, cand_dir)
    out["files"] = {"reference": len(ref_files), "candidate": len(cand_files)}
    reasons: list[str] = []
    expected: list[dict[str, Any]] = []
    name_case: list[dict[str, Any]] = []

    ref_table, cand_table = f"{ref_dir.schema}.{ref_dir.table}", f"{cand_dir.schema}.{cand_dir.table}"
    if ref_table != cand_table:
        item = {"object": "table", "reference": ref_table, "candidate": cand_table}
        _classify(options, "name_case", name, "", item, expected, name_case)

    if not ref_files or not cand_files:
        # A table without data files has no Parquet schema to read; compare
        # the row counts only.
        ref_rows = 0 if not ref_files else side_stats(con, ref_files, []).rows
        cand_rows = 0 if not cand_files else side_stats(con, cand_files, []).rows
        out["rows"] = {"reference": ref_rows, "candidate": cand_rows}
        if ref_rows != cand_rows:
            reasons.append("row_count")
        return _finish(out, reasons, expected, name_case)

    ref_parquet = describe(con, ref_files)
    cand_parquet = describe(con, cand_files)
    ref_types = {c: t for c, (_, t) in ref_parquet.items()}
    cand_types = {c: t for c, (_, t) in cand_parquet.items()}
    only_ref = sorted(set(ref_types) - set(cand_types))
    only_cand = sorted(set(cand_types) - set(ref_types))
    common = sorted(set(ref_types) & set(cand_types))
    skipped = [c for c in common if ignored(name, c, options.ignore_columns)]
    compared = [c for c in common if c not in skipped]
    class_mismatch = [c for c in compared if type_class(ref_types[c]) != type_class(cand_types[c])]

    # Declared types and names: the metastore of both sides, or else the
    # Parquet schema of both sides.
    from_metastore = ref_declared is not None and cand_declared is not None
    if from_metastore:
        ref_decl, cand_decl = ref_declared, cand_declared
    else:
        ref_decl, cand_decl = ref_parquet, cand_parquet
    type_differences: list[dict[str, Any]] = []
    for c in common:
        if c not in ref_decl or c not in cand_decl:
            continue
        (ref_name, ref_type), (cand_name, cand_type) = ref_decl[c], cand_decl[c]
        if ref_name != cand_name:
            item = {"object": "column", "column": c, "reference": ref_name, "candidate": cand_name}
            _classify(options, "name_case", name, c, item, expected, name_case)
        if c in skipped or c in class_mismatch:
            continue
        if expected_rules.normalize_type(ref_type) != expected_rules.normalize_type(cand_type):
            item = {"column": c, "reference": ref_type, "candidate": cand_type}
            _classify(options, "declared_type", name, c, item, expected, type_differences)
    physical = (
        [
            {"column": c, "reference": ref_types[c], "candidate": cand_types[c]}
            for c in compared
            if ref_types[c] != cand_types[c] and c not in class_mismatch
        ]
        if from_metastore
        else []
    )
    out["schema"] = {
        "declared_types_from": "metastore" if from_metastore else "parquet",
        "only_reference": only_ref,
        "only_candidate": only_cand,
        "type_class_mismatch": [
            {"column": c, "reference": ref_types[c], "candidate": cand_types[c]} for c in class_mismatch
        ],
        "type_differences": type_differences,
        "storage_type_differences": physical,
        "ignored_columns": skipped,
        "compared_columns": len(compared),
    }
    ref_cols = _column_list(compared, ref_types)
    cand_cols = _column_list(compared, cand_types)
    ref = side_stats(con, ref_files, ref_cols)
    cand = side_stats(con, cand_files, cand_cols)
    out["rows"] = {"reference": ref.rows, "candidate": cand.rows}
    out["checksum"] = {"reference": ref.checksum, "candidate": cand.checksum}
    if ref.rows != cand.rows:
        reasons.append("row_count")
    if ref.checksum != cand.checksum:
        reasons.append("checksum")
    if only_ref or only_cand:
        reasons.append("columns")
    if class_mismatch:
        reasons.append("column_types")
    if type_differences:
        reasons.append("declared_types")
    diff_columns = [c for c in compared if ref.columns[c] != cand.columns[c]]
    out["columns_differing"] = [
        {
            "column": c,
            "nulls": {"reference": ref.nulls[c], "candidate": cand.nulls[c]},
        }
        for c in diff_columns
    ]
    if ref.checksum != cand.checksum:
        total, sample = differing_rows(
            con, ref_files, cand_files, ref_cols, cand_cols, options.sample, options.show_rows
        )
        out["differing_row_hashes"] = total
        out["sample"] = sample
    return _finish(out, reasons, expected, name_case)


def _finish(
    out: dict[str, Any],
    reasons: list[str],
    expected: list[dict[str, Any]],
    name_case: list[dict[str, Any]],
) -> dict[str, Any]:
    if name_case:
        reasons.append("name_case")
    out["name_case_differences"] = name_case
    out["expected_differences"] = expected
    out["status"] = "divergent" if reasons else "equal"
    if reasons:
        out["reasons"] = reasons
    return out


def connect(memory_limit: str, temp_dir: str, s3: dict[str, str] | None = None) -> duckdb.DuckDBPyConnection:
    """A DuckDB connection with bounded memory that spills to ``temp_dir``."""
    os.makedirs(temp_dir, exist_ok=True)
    settings: dict[str, Any] = {"memory_limit": memory_limit, "temp_directory": temp_dir}
    extensions = os.environ.get("CODELAKE_SHADOW_DUCKDB_EXTENSIONS", "")
    if extensions:
        settings["extension_directory"] = extensions
    con = duckdb.connect(database=":memory:", config=settings)
    if s3 is not None:
        con.execute("LOAD httpfs")
        options = ["TYPE S3"]
        if s3.get("key_id"):
            options.append(f"KEY_ID {quote_literal(s3['key_id'])}")
            options.append(f"SECRET {quote_literal(s3.get('secret', ''))}")
        else:
            options.append("PROVIDER credential_chain")
        if s3.get("region"):
            options.append(f"REGION {quote_literal(s3['region'])}")
        endpoint = s3.get("endpoint", "")
        if endpoint:
            match = re.match(r"^(https?)://(.+?)/?$", endpoint)
            if match:
                options.append(f"ENDPOINT {quote_literal(match.group(2))}")
                options.append(f"USE_SSL {'true' if match.group(1) == 'https' else 'false'}")
            else:
                options.append(f"ENDPOINT {quote_literal(endpoint)}")
        if s3.get("path_style") == "true":
            options.append("URL_STYLE 'path'")
        try:
            con.execute(f"CREATE SECRET shadow_s3 ({', '.join(options)})")
        except duckdb.Error:
            # The statement holds the key; never let an error message quote it.
            raise RuntimeError("cannot configure DuckDB's S3 access (check SHADOW_S3_* and AWS_*)") from None
    return con
