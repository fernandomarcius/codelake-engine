"""Shadow-run configuration, read from ``SHADOW_*`` environment variables.

``scripts/shadow-run.sh`` loads them from a config file (``KEY=value``
lines) and passes them to the harness container. Every variable is
documented in ``docs/shadow-run.md``.
"""

from __future__ import annotations

import dataclasses
import os
from typing import Mapping
from urllib.parse import urlparse

from . import expected

# Differences the candidate declares against the reference engine
# (docs/issues), shipped with the harness. ``SHADOW_EXPECTED_DIFFERENCES``
# names another file, or ``none``.
DEFAULT_EXPECTED = os.path.join(os.path.dirname(os.path.abspath(__file__)), "expected-differences.json")


class ConfigError(ValueError):
    pass


@dataclasses.dataclass
class Side:
    """One engine: where dbt connects, which target, where its output is."""

    name: str
    url: str
    target: str
    warehouse: str
    rss: str
    container_id: str

    @property
    def host(self) -> str:
        return urlparse(self.url).hostname or ""

    @property
    def port(self) -> int:
        parsed = urlparse(self.url)
        return parsed.port or (443 if parsed.scheme == "https" else 80)

    @property
    def scheme(self) -> str:
        return urlparse(self.url).scheme


@dataclasses.dataclass
class Config:
    reference: Side
    candidate: Side
    project_dir: str
    profiles_dir: str
    out_dir: str
    dbt_command: str = "run"
    select: str = ""
    exclude: str = ""
    threads: int = 0
    output_catalogs: list[str] = dataclasses.field(default_factory=lambda: ["hive"])
    tables: list[str] = dataclasses.field(default_factory=list)
    ignore_columns: list[str] = dataclasses.field(default_factory=list)
    cycles: int = 1
    interval_secs: float = 0.0
    cutoff_sql: str = ""
    cutoff_value: str = ""
    cutoff_var: str = "shadow_cutoff"
    extra_vars: str = ""
    full_refresh_first: bool = False
    rss_interval_ms: int = 200
    rss_max_ratio: float | None = None
    sample: int = 10
    show_rows: bool = False
    duckdb_memory: str = "2GB"
    s3_endpoint: str = ""
    s3_region: str = ""
    s3_path_style: bool = False
    expected_differences: str = DEFAULT_EXPECTED
    expected: list[expected.Rule] = dataclasses.field(default_factory=list)


def _bool(value: str, name: str) -> bool:
    lowered = value.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"", "0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{name} must be true or false")


def _int(value: str, name: str, minimum: int) -> int:
    try:
        number = int(value)
    except ValueError as err:
        raise ConfigError(f"{name} must be an integer") from err
    if number < minimum:
        raise ConfigError(f"{name} must be >= {minimum}")
    return number


def _list(value: str) -> list[str]:
    return [item.strip() for item in value.replace("\n", ",").split(",") if item.strip()]


def _url(value: str, name: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ConfigError(f"{name} must be an http(s)://host:port URL")
    if parsed.username or parsed.password:
        raise ConfigError(f"{name} must not carry credentials; put them in the dbt profile")
    return value


def _warehouse(value: str, name: str) -> str:
    if not (value.startswith("/") or value.startswith("s3://")):
        raise ConfigError(f"{name} must be an absolute path or an s3:// URL")
    return value.rstrip("/") or "/"


def expected_path(value: str) -> str:
    """The rule file: the shipped one by default, none with ``none``."""
    if value.strip().lower() == "none":
        return ""
    return value or DEFAULT_EXPECTED


def load(env: Mapping[str, str] | None = None) -> Config:
    """Reads and checks the configuration; reports every problem at once."""
    env = os.environ if env is None else env
    errors: list[str] = []

    def get(name: str, default: str | None = None) -> str:
        value = env.get(name, "")
        if value == "" and default is None:
            errors.append(f"{name} is required")
        return value if value != "" else (default or "")

    def check(fn, *args):
        try:
            return fn(*args)
        except ConfigError as err:
            errors.append(str(err))
            return None

    sides = []
    for side in ("reference", "candidate"):
        prefix = f"SHADOW_{side.upper()}_"
        url = get(prefix + "URL")
        warehouse = get(prefix + "WAREHOUSE")
        sides.append(
            Side(
                name=side,
                url=(check(_url, url, prefix + "URL") or url) if url else url,
                target=get(prefix + "TARGET", side),
                warehouse=(check(_warehouse, warehouse, prefix + "WAREHOUSE") or warehouse)
                if warehouse
                else warehouse,
                rss=get(prefix + "RSS", ""),
                container_id=get(prefix + "CONTAINER_ID", ""),
            )
        )
    reference, candidate = sides
    if reference.warehouse and reference.warehouse == candidate.warehouse:
        errors.append(
            "SHADOW_REFERENCE_WAREHOUSE and SHADOW_CANDIDATE_WAREHOUSE are the same: "
            "each side must write to its own destination"
        )
    if reference.url and reference.url == candidate.url:
        errors.append("SHADOW_REFERENCE_URL and SHADOW_CANDIDATE_URL are the same engine")
    command = get("SHADOW_DBT_COMMAND", "run")
    if command not in {"run", "build"}:
        errors.append("SHADOW_DBT_COMMAND must be run or build")
    cutoff_sql = get("SHADOW_CUTOFF_SQL", "")
    cutoff_value = get("SHADOW_CUTOFF_VALUE", "")
    if cutoff_sql and cutoff_value:
        errors.append("set SHADOW_CUTOFF_SQL or SHADOW_CUTOFF_VALUE, not both")
    ratio = get("SHADOW_RSS_MAX_RATIO", "")
    max_ratio = None
    if ratio:
        try:
            max_ratio = float(ratio)
        except ValueError:
            errors.append("SHADOW_RSS_MAX_RATIO must be a number")
    config = Config(
        reference=reference,
        candidate=candidate,
        project_dir=get("SHADOW_DBT_PROJECT_DIR"),
        profiles_dir=get("SHADOW_DBT_PROFILES_DIR"),
        out_dir=get("SHADOW_OUT_DIR"),
        dbt_command=command,
        select=get("SHADOW_SELECT", ""),
        exclude=get("SHADOW_EXCLUDE", ""),
        threads=check(_int, get("SHADOW_THREADS", "0"), "SHADOW_THREADS", 0) or 0,
        output_catalogs=_list(get("SHADOW_OUTPUT_CATALOGS", "hive")),
        tables=_list(get("SHADOW_TABLES", "")),
        ignore_columns=_list(get("SHADOW_IGNORE_COLUMNS", "")),
        cycles=check(_int, get("SHADOW_CYCLES", "1"), "SHADOW_CYCLES", 1) or 1,
        interval_secs=float(check(_int, get("SHADOW_INTERVAL_SECS", "0"), "SHADOW_INTERVAL_SECS", 0) or 0),
        cutoff_sql=cutoff_sql,
        cutoff_value=cutoff_value,
        cutoff_var=get("SHADOW_CUTOFF_VAR", "shadow_cutoff"),
        extra_vars=get("SHADOW_DBT_VARS", ""),
        full_refresh_first=bool(check(_bool, get("SHADOW_FULL_REFRESH_FIRST", "false"), "SHADOW_FULL_REFRESH_FIRST")),
        rss_interval_ms=check(_int, get("SHADOW_RSS_INTERVAL_MS", "200"), "SHADOW_RSS_INTERVAL_MS", 10) or 200,
        rss_max_ratio=max_ratio,
        sample=check(_int, get("SHADOW_SAMPLE", "10"), "SHADOW_SAMPLE", 0) or 0,
        show_rows=bool(check(_bool, get("SHADOW_SHOW_ROWS", "false"), "SHADOW_SHOW_ROWS")),
        duckdb_memory=get("SHADOW_DUCKDB_MEMORY", "2GB"),
        s3_endpoint=get("SHADOW_S3_ENDPOINT", ""),
        s3_region=get("SHADOW_S3_REGION", ""),
        s3_path_style=bool(check(_bool, get("SHADOW_S3_PATH_STYLE", "false"), "SHADOW_S3_PATH_STYLE")),
    )
    config.expected_differences = expected_path(get("SHADOW_EXPECTED_DIFFERENCES", ""))
    try:
        config.expected = expected.load(config.expected_differences)
    except expected.ExpectedError as err:
        errors.append(str(err))
    if not config.cutoff_var.replace("_", "a").isalnum():
        errors.append("SHADOW_CUTOFF_VAR must be a dbt variable name ([A-Za-z0-9_]+)")
    if errors:
        raise ConfigError("invalid shadow-run configuration:\n  - " + "\n  - ".join(errors))
    return config
