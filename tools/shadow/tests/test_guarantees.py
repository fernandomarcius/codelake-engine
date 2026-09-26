"""Guarantees of the comparison and of a cycle's verdict that no other test
pins: each test fails when the property is weakened (found by mutating the
harness; the mutation named in each docstring survived the other tests)."""

from __future__ import annotations

import os
import unittest
from typing import Any
from unittest import mock

from codelake_shadow import __main__ as main
from codelake_shadow import compare, config, dbt
from test_compare import Warehouses

ENV = {
    "SHADOW_REFERENCE_URL": "http://reference:8080",
    "SHADOW_CANDIDATE_URL": "http://candidate:8080",
    "SHADOW_REFERENCE_WAREHOUSE": "/reference",
    "SHADOW_CANDIDATE_WAREHOUSE": "/candidate",
    "SHADOW_DBT_PROJECT_DIR": "/project",
    "SHADOW_DBT_PROFILES_DIR": "/project",
    "SHADOW_TABLES": "s.t",
}


class FakeSampler:
    """A sampler that reports a fixed peak, without a thread."""

    def __init__(self, meter: object, interval: float) -> None:
        self.peak = 1024

    def __enter__(self) -> "FakeSampler":
        return self

    def __exit__(self, *exc: object) -> None:
        pass

    def result(self) -> dict[str, Any]:
        return {"meter": None, "peak_bytes": self.peak, "samples": 1, "failed_samples": 0, "interval_ms": 200}


def cycle_with(out_dir: str, table: dict[str, Any], exit_codes=(0, 0), statuses=({}, {})) -> dict[str, Any]:
    """One cycle whose dbt runs and table comparison are given."""
    runs = iter(dbt.DbtRun(code, 0.0, os.devnull, "/nonexistent") for code in exit_codes)
    status = iter(statuses)
    cfg = config.load(dict(ENV, SHADOW_OUT_DIR=out_dir))
    with mock.patch.object(compare, "compare_table", return_value=table), \
         mock.patch.object(main, "cutoff_value", return_value=None), \
         mock.patch.object(dbt, "run", side_effect=lambda *a, **k: next(runs)), \
         mock.patch.object(dbt, "summary", return_value={}), \
         mock.patch.object(dbt, "statuses", side_effect=lambda *a, **k: next(status)), \
         mock.patch.object(main, "Sampler", FakeSampler), \
         mock.patch.object(main, "connection", return_value=mock.MagicMock()):
        return main.run_cycle(cfg, 1, "/project")


class ComparisonGuarantees(unittest.TestCase):
    def setUp(self) -> None:
        self.w = Warehouses()

    def tearDown(self) -> None:
        self.w.dir.cleanup()

    def test_a_sub_millisecond_difference_diverges(self) -> None:
        """Timestamps compared at millisecond precision."""
        self.w.write("ref", "s.t", "SELECT TIMESTAMP '2026-01-01 10:00:00.123' AS ts")
        self.w.write("cand", "s.t", "SELECT TIMESTAMP '2026-01-01 10:00:00.123456' AS ts")
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reasons"], ["checksum"], result)

    def test_duplicates_that_cancel_under_xor_diverge(self) -> None:
        """Row hashes combined with XOR: pairs of duplicates cancel out."""
        self.w.write("ref", "s.t", "SELECT * FROM (VALUES (1), (1), (3), (3)) v(x)")
        self.w.write("cand", "s.t", "SELECT * FROM (VALUES (2), (2), (4), (4)) v(x)")
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["reasons"], ["checksum"], result)

    def test_strings_that_differ_only_by_case_diverge(self) -> None:
        """Strings folded to lower case before hashing."""
        self.w.write("ref", "s.t", "SELECT 'Paid' AS status")
        self.w.write("cand", "s.t", "SELECT 'paid' AS status")
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual([c["column"] for c in result["columns_differing"]], ["status"])

    def test_a_side_without_data_files_is_compared_by_row_count(self) -> None:
        """Row counts not compared when one side has no data file."""
        self.w.write("ref", "s.t", "SELECT 1 AS x")
        os.makedirs(os.path.join(self.w.cand, "s", "t"))
        result = self.w.compare()
        self.assertEqual(result["status"], "divergent", result)
        self.assertEqual(result["rows"], {"reference": 1, "candidate": 0})


class CycleVerdictGuarantees(unittest.TestCase):
    def setUp(self) -> None:
        self.out = Warehouses().dir

    def tearDown(self) -> None:
        self.out.cleanup()

    def test_equal_when_everything_matches(self) -> None:
        cycle = cycle_with(self.out.name, {"table": "s.t", "status": "equal"})
        self.assertEqual(cycle["verdict"], "equal", cycle)
        self.assertEqual(cycle["failures"], [])

    def test_a_missing_table_fails_the_cycle(self) -> None:
        """Only `divergent` tables failing the cycle (missing or error passing)."""
        for status in ("missing_candidate", "missing_reference", "missing_both", "error"):
            cycle = cycle_with(self.out.name, {"table": "s.t", "status": status})
            self.assertEqual(cycle["verdict"], "divergent", status)
            self.assertEqual(cycle["failures"], ["tables"], status)

    def test_a_failed_dbt_run_fails_the_cycle(self) -> None:
        """dbt exit codes ignored."""
        cycle = cycle_with(self.out.name, {"table": "s.t", "status": "equal"}, exit_codes=(0, 1))
        self.assertEqual(cycle["verdict"], "divergent", cycle)
        self.assertEqual(cycle["failures"], ["dbt_exit"])

    def test_a_model_status_difference_fails_the_cycle(self) -> None:
        """Model statuses that differ between the sides ignored."""
        cycle = cycle_with(
            self.out.name,
            {"table": "s.t", "status": "equal"},
            statuses=({"model.p.m": "success"}, {"model.p.m": "error"}),
        )
        self.assertEqual(cycle["verdict"], "divergent", cycle)
        self.assertEqual(cycle["failures"], ["model_statuses"])


if __name__ == "__main__":
    unittest.main()
