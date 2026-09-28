"""Each cycle compares only what it built: full refresh modes and the clock."""

from __future__ import annotations

import datetime as dt
import json
import unittest

from codelake_shadow import config, dbt

ENV = {
    "SHADOW_REFERENCE_URL": "http://reference:8080",
    "SHADOW_CANDIDATE_URL": "http://candidate:8080",
    "SHADOW_REFERENCE_WAREHOUSE": "/reference",
    "SHADOW_CANDIDATE_WAREHOUSE": "/candidate",
    "SHADOW_DBT_PROJECT_DIR": "/project",
    "SHADOW_DBT_PROFILES_DIR": "/project",
    "SHADOW_OUT_DIR": "/out",
}


def load(**extra: str) -> config.Config:
    return config.load(dict(ENV, **extra))


class FullRefresh(unittest.TestCase):
    def test_modes_and_the_older_spelling(self) -> None:
        self.assertEqual(load().full_refresh, "never")
        self.assertEqual(load(SHADOW_FULL_REFRESH_FIRST="true").full_refresh, "first")
        self.assertEqual(load(SHADOW_FULL_REFRESH="always").full_refresh, "always")
        with self.assertRaises(config.ConfigError):
            load(SHADOW_FULL_REFRESH="sometimes")

    def test_always_rebuilds_on_every_cycle(self) -> None:
        cfg = load(SHADOW_FULL_REFRESH="always")
        args = dbt.command(cfg, cfg.reference, "/p", "/w", None, full_refresh=True)
        self.assertIn("--full-refresh", args)


class Clock(unittest.TestCase):
    INSTANT = dt.datetime(2026, 9, 28, 17, 23, 22, 123456, tzinfo=dt.timezone.utc)

    def test_both_sides_get_the_same_instant_in_utc_and_local_time(self) -> None:
        cfg = load(SHADOW_CLOCK_VAR="shadow_now")
        self.assertEqual(
            dbt.clock(cfg, self.INSTANT),
            {"shadow_now": "2026-09-28 17:23:22.123", "shadow_now_local": "2026-09-28 14:23:22.123"},
        )
        variables = json.loads(dbt.dbt_vars(cfg, "2026-09-28 14:20:28", dbt.clock(cfg, self.INSTANT)))
        self.assertEqual(variables["shadow_cutoff"], "2026-09-28 14:20:28")
        self.assertEqual(variables["shadow_now"], "2026-09-28 17:23:22.123")

    def test_no_clock_unless_asked(self) -> None:
        self.assertEqual(dbt.clock(load(), self.INSTANT), {})

    def test_invalid_names_and_zones_are_refused(self) -> None:
        with self.assertRaises(config.ConfigError):
            load(SHADOW_CLOCK_VAR="not-a-name")
        with self.assertRaises(config.ConfigError):
            load(SHADOW_CLOCK_VAR="shadow_now", SHADOW_CLOCK_LOCAL_ZONE="Mars/Olympus")


if __name__ == "__main__":
    unittest.main()
