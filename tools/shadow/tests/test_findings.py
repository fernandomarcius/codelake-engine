"""Findings of the F8 audit, pinned: each test failed on the harness that
reported them as equal (INT96 read as stored, column order, metastore gaps,
empty tables)."""

from __future__ import annotations

import json
import os
import unittest

from codelake_shadow import compare, config
from test_compare import Warehouses

# Written by a real trinodb/trino:431 whose JVM zone is UTC: `ts` is INT96,
# stored as the instant 2026-01-01 10:00:00.123Z (one row, id = 1).
INT96_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "trino431-int96.parquet")
ZONE = "America/Sao_Paulo"  # -03:00 in January

ENV = {
    "SHADOW_REFERENCE_URL": "http://reference:8080",
    "SHADOW_CANDIDATE_URL": "http://candidate:8080",
    "SHADOW_REFERENCE_WAREHOUSE": "/reference",
    "SHADOW_CANDIDATE_WAREHOUSE": "/candidate",
    "SHADOW_DBT_PROJECT_DIR": "/project",
    "SHADOW_DBT_PROFILES_DIR": "/project",
    "SHADOW_TABLES": "s.t",
}


class Int96(unittest.TestCase):
    def setUp(self) -> None:
        self.w = Warehouses()
        self.w.place("ref", "s.t", [INT96_FILE])

    def tearDown(self) -> None:
        self.w.dir.cleanup()

    def int64_copy(self, ts_sql: str) -> None:
        self.w.write(
            "cand", "s.t", f"SELECT * REPLACE ({ts_sql} AS ts) FROM read_parquet('{INT96_FILE}')"
        )

    def test_int96_is_compared_as_the_wall_clock_time_of_the_zone(self) -> None:
        # The engines read 07:00:00.123 in America/Sao_Paulo: an INT64 file
        # holding that value has the same values, in another storage.
        self.int64_copy("TIMESTAMP '2026-01-01 07:00:00.123'")
        result = self.w.compare(parquet_time_zone=ZONE)
        self.assertEqual(result["reasons"], ["storage_types"], result)
        self.assertEqual(result["checksum"]["reference"], result["checksum"]["candidate"])
        storage = {d["column"]: d for d in result["schema"]["storage_type_differences"]}
        self.assertEqual(list(storage), ["ts"])
        self.assertIn("INT96", storage["ts"]["reference"])
        self.assertIn("INT64 MICROS local", storage["ts"]["candidate"])

    def test_int96_copied_as_stored_is_a_value_divergence(self) -> None:
        # The layout rewrite that used to pass as equal: the stored instant
        # written as INT64, which every engine reads 3 hours later.
        self.int64_copy("ts")
        result = self.w.compare(parquet_time_zone=ZONE)
        self.assertEqual(result["reasons"], ["storage_types", "checksum"], result)
        self.assertEqual([c["column"] for c in result["columns_differing"]], ["ts"])

    def test_int96_without_a_zone_is_not_compared(self) -> None:
        self.w.place("cand", "s.t", [INT96_FILE])
        result = self.w.compare()
        self.assertEqual(result["status"], "error", result)
        self.assertIn("INT96", result["error"])
        self.assertEqual(self.w.compare(parquet_time_zone=ZONE)["status"], "equal")
        # An ignored INT96 column needs no zone.
        self.assertEqual(self.w.compare(ignore_columns=["ts"])["status"], "equal")

    def test_files_with_and_without_int96_on_one_side(self) -> None:
        # Reference: the INT96 file plus an INT64 file (id 2); candidate: one
        # INT64 file with both rows as the engines read them.
        extra = os.path.join(self.w.dir.name, "extra.parquet")
        self.w.con.execute(
            f"COPY (SELECT * REPLACE (2 AS id, TIMESTAMP '2026-01-03 00:00:00' AS ts) "
            f"FROM read_parquet('{INT96_FILE}')) TO '{extra}' (FORMAT PARQUET)"
        )
        self.w.place("ref", "s.t", [INT96_FILE, extra])
        self.w.write(
            "cand",
            "s.t",
            f"SELECT * REPLACE (TIMESTAMP '2026-01-01 07:00:00.123' AS ts) FROM read_parquet('{INT96_FILE}') "
            f"UNION ALL SELECT * FROM read_parquet('{extra}')",
        )
        result = self.w.compare(parquet_time_zone=ZONE)
        self.assertEqual(result["rows"], {"reference": 2, "candidate": 2})
        self.assertEqual(result["reasons"], ["storage_types"], result)


class Schema(unittest.TestCase):
    def setUp(self) -> None:
        self.w = Warehouses()

    def tearDown(self) -> None:
        self.w.dir.cleanup()

    def empty(self, side: str, columns: list[dict[str, str]]) -> None:
        directory = os.path.join(self.w.ref if side == "ref" else self.w.cand, "s", "t")
        os.makedirs(directory)
        with open(os.path.join(directory, compare.METASTORE_FILE), "w") as f:
            json.dump({"dataColumns": columns, "partitionColumns": []}, f)

    def test_column_order_is_compared(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS a, 2 AS b", declared=[("a", "int"), ("b", "int")])
        self.w.write("cand", "s.t", "SELECT 2 AS b, 1 AS a", declared=[("b", "int"), ("a", "int")])
        result = self.w.compare()
        self.assertEqual(result["reasons"], ["column_order"], result)
        self.assertEqual(result["schema"]["column_order"], {"reference": ["a", "b"], "candidate": ["b", "a"]})
        # Without a metastore file on either side, the Parquet order counts.
        self.w.write("ref", "s.t", "SELECT 1 AS a, 2 AS b")
        self.w.write("cand", "s.t", "SELECT 2 AS b, 1 AS a")
        self.assertEqual(self.w.compare()["reasons"], ["column_order"])

    def test_a_metastore_file_on_one_side_only_diverges(self) -> None:
        for with_metastore in ("ref", "cand"):
            with self.subTest(side=with_metastore):
                other = "cand" if with_metastore == "ref" else "ref"
                self.w.write(with_metastore, "s.t", "SELECT 'a' AS status", declared=[("status", "varchar(20)")])
                self.w.write(other, "s.t", "SELECT 'a' AS status")
                result = self.w.compare()
                self.assertEqual(result["status"], "divergent", result)
                self.assertIn("metastore", result["reasons"])

    def test_a_column_declared_on_one_side_only_diverges(self) -> None:
        self.w.write("ref", "s.t", "SELECT 1 AS x", declared=[("x", "integer")])
        self.w.write("cand", "s.t", "SELECT 1 AS x", declared=[("x", "integer"), ("y", "varchar")])
        result = self.w.compare()
        self.assertEqual(result["reasons"], ["columns"], result)
        self.assertEqual(result["schema"]["declared_only_candidate"], ["y"])

    def test_tables_without_data_files_compare_declared_columns(self) -> None:
        self.empty("ref", [{"name": "x", "type": "decimal(18,2)"}])
        self.empty("cand", [{"name": "x", "type": "double"}])
        result = self.w.compare()
        self.assertEqual(result["reasons"], ["declared_types"], result)
        self.w.dir.cleanup()
        self.w = Warehouses()
        self.empty("ref", [{"name": "x", "type": "int"}])
        self.empty("cand", [{"name": "y", "type": "int"}])
        self.assertEqual(self.w.compare()["reasons"], ["columns"])
        self.w.dir.cleanup()
        self.w = Warehouses()
        self.empty("ref", [{"name": "x", "type": "int"}])
        self.empty("cand", [{"name": "x", "type": "int"}])
        self.assertEqual(self.w.compare()["status"], "equal")

    def test_nanosecond_timestamps_keep_every_digit(self) -> None:
        # DuckDB 1.1 cannot build a TIMESTAMP_NS below the microsecond (its
        # casts truncate), so the nanosecond path is checked on the SQL and a
        # microsecond difference on the data.
        self.assertIn("epoch_ns(", compare.canonical_expr("ts", "TIMESTAMP_NS"))
        self.assertNotIn("% 1000", compare.canonical_expr("ts", "TIMESTAMP_NS"))
        self.w.write("ref", "s.t", "SELECT CAST('2026-01-01 10:00:00.123456' AS TIMESTAMP_NS) AS ts")
        self.w.write("cand", "s.t", "SELECT CAST('2026-01-01 10:00:00.123457' AS TIMESTAMP_NS) AS ts")
        self.assertEqual(self.w.compare()["reasons"], ["checksum"])
        # The same instant in two units is the same value; without a
        # metastore file the unit is the declared type, which differs.
        self.w.write("cand", "s.t", "SELECT TIMESTAMP '2026-01-01 10:00:00.123456' AS ts")
        self.assertEqual(self.w.compare()["reasons"], ["declared_types"])


class Configuration(unittest.TestCase):
    def setUp(self) -> None:
        self.w = Warehouses()

    def tearDown(self) -> None:
        self.w.dir.cleanup()

    def test_the_parquet_time_zone_is_checked(self) -> None:
        cfg = config.load(dict(ENV, SHADOW_OUT_DIR="/out", SHADOW_PARQUET_TIME_ZONE=ZONE))
        self.assertEqual(cfg.parquet_time_zone, ZONE)
        with self.assertRaisesRegex(config.ConfigError, "SHADOW_PARQUET_TIME_ZONE"):
            config.load(dict(ENV, SHADOW_OUT_DIR="/out", SHADOW_PARQUET_TIME_ZONE="Mars/Base"))


if __name__ == "__main__":
    unittest.main()
