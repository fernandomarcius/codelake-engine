"""dbt invocation for one side, and what its artifacts say."""

from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import subprocess
import time
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo

from .config import Config, Side

# Never copied from the project: build output, logs, and profiles (which may
# hold credentials; the profiles directory is passed to dbt instead).
_IGNORED = shutil.ignore_patterns("target", "logs", ".git", ".dbt", "profiles.yml", ".user.yml")


def freeze_project(project_dir: str, dest: str) -> str:
    """Copies the project once per invocation, so both sides run the same files."""
    if os.path.exists(dest):
        shutil.rmtree(dest)
    shutil.copytree(project_dir, dest, ignore=_IGNORED)
    return dest


@dataclass
class DbtRun:
    exit_code: int
    seconds: float
    log_path: str
    target_path: str

    def artifact(self, name: str) -> dict[str, Any] | None:
        path = os.path.join(self.target_path, name)
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None


def clock(config: Config, instant: dt.datetime) -> dict[str, str]:
    """The cycle's clock variables (see `Config.clock_var`), empty without one."""
    if not config.clock_var:
        return {}
    fmt = "%Y-%m-%d %H:%M:%S.%f"
    utc = instant.astimezone(dt.timezone.utc)
    local = instant.astimezone(ZoneInfo(config.clock_local_zone))
    return {
        config.clock_var: utc.strftime(fmt)[:-3],
        f"{config.clock_var}_local": local.strftime(fmt)[:-3],
    }


def dbt_vars(config: Config, cutoff: str | None, clock_vars: dict[str, str] | None = None) -> str:
    variables: dict[str, Any] = {}
    if config.extra_vars:
        try:
            extra = json.loads(config.extra_vars)
        except ValueError as err:
            raise ValueError("SHADOW_DBT_VARS must be a JSON object") from err
        if not isinstance(extra, dict):
            raise ValueError("SHADOW_DBT_VARS must be a JSON object")
        variables.update(extra)
    if cutoff is not None:
        variables[config.cutoff_var] = cutoff
    variables.update(clock_vars or {})
    return json.dumps(variables, sort_keys=True)


def command(
    config: Config,
    side: Side,
    project: str,
    work: str,
    cutoff: str | None,
    full_refresh: bool,
    clock_vars: dict[str, str] | None = None,
) -> list[str]:
    args = [
        "dbt", config.dbt_command,
        "--project-dir", project,
        "--profiles-dir", config.profiles_dir,
        "--target", side.target,
        "--target-path", os.path.join(work, "target"),
        "--log-path", os.path.join(work, "logs"),
        "--vars", dbt_vars(config, cutoff, clock_vars),
        "--no-use-colors",
    ]
    if config.select:
        args += ["--select", *config.select.split()]
    if config.exclude:
        args += ["--exclude", *config.exclude.split()]
    if config.threads:
        args += ["--threads", str(config.threads)]
    if full_refresh:
        args.append("--full-refresh")
    return args


def side_env(side: Side) -> dict[str, str]:
    """Variables a profile can read with ``env_var()`` to reach this side."""
    env = dict(os.environ)
    env.update(
        {
            "SHADOW_SIDE": side.name,
            "SHADOW_ENGINE_URL": side.url,
            "SHADOW_ENGINE_HOST": side.host,
            "SHADOW_ENGINE_PORT": str(side.port),
            "SHADOW_ENGINE_HTTP_SCHEME": side.scheme,
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }
    )
    return env


def run(
    config: Config,
    side: Side,
    project: str,
    work: str,
    cutoff: str | None,
    full_refresh: bool,
    clock_vars: dict[str, str] | None = None,
) -> DbtRun:
    os.makedirs(work, exist_ok=True)
    log_path = os.path.join(work, "dbt.log")
    started = time.monotonic()
    with open(log_path, "w", encoding="utf-8") as log:
        done = subprocess.run(
            command(config, side, project, work, cutoff, full_refresh, clock_vars),
            stdout=log,
            stderr=subprocess.STDOUT,
            env=side_env(side),
            cwd=project,
            check=False,
        )
    return DbtRun(done.returncode, time.monotonic() - started, log_path, os.path.join(work, "target"))


def statuses(run_results: dict[str, Any] | None) -> dict[str, str]:
    if not run_results:
        return {}
    return {r["unique_id"]: r.get("status", "") for r in run_results.get("results", [])}


def summary(dbt: DbtRun) -> dict[str, Any]:
    results = statuses(dbt.artifact("run_results.json"))
    counts: dict[str, int] = {}
    for status in results.values():
        counts[status] = counts.get(status, 0) + 1
    return {
        "exit_code": dbt.exit_code,
        "seconds": round(dbt.seconds, 3),
        "nodes": len(results),
        "statuses": counts,
        "log": dbt.log_path,
    }


def output_tables(
    manifest: dict[str, Any] | None,
    results: dict[str, str],
    catalogs: list[str],
) -> tuple[list[tuple[str, str, str]], list[dict[str, str]]]:
    """(schema, table, unique_id) of each materialized model that ran, and the
    models left out with the reason (views, other catalogs)."""
    tables: list[tuple[str, str, str]] = []
    skipped: list[dict[str, str]] = []
    if not manifest:
        return tables, skipped
    wanted = {c.lower() for c in catalogs}
    for uid, node in sorted(manifest.get("nodes", {}).items()):
        if node.get("resource_type") != "model" or uid not in results:
            continue
        materialized = (node.get("config") or {}).get("materialized", "")
        database = (node.get("database") or "").lower()
        name = f"{node.get('schema', '')}.{node.get('alias') or node.get('name')}".lower()
        if materialized not in {"table", "incremental"}:
            skipped.append({"model": name, "reason": f"materialized as {materialized or 'unknown'}"})
            continue
        if database not in wanted:
            skipped.append({"model": name, "reason": f"catalog {database} is not an output catalog"})
            continue
        # As written in the project: the comparison finds the directories
        # without regard to case.
        tables.append((node["schema"], node.get("alias") or node["name"], uid))
    return tables, skipped
