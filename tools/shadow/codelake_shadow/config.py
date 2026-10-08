"""Shadow-run configuration, read from ``SHADOW_*`` environment variables.

``scripts/shadow-run.sh`` loads them from a config file (``KEY=value``
lines) and passes them to the harness container. Every variable is
documented in ``docs/shadow-run.md``.
"""

from __future__ import annotations

import dataclasses
import os
import zoneinfo
from typing import Mapping
from urllib.parse import urlparse

from . import compare, expected

# Differences the candidate declares against the reference engine
# (docs/issues), shipped with the harness as a rule file to opt into:
# ``SHADOW_EXPECTED_DIFFERENCES=default`` uses it, a path names another
# file; unset or ``none``, no rule applies.
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
    metastore_file: str = ".trinoSchema"

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
    # When dbt rebuilds the selected models from scratch: "never", on the
    # "first" cycle of the output directory, or "always" — every cycle, so a
    # model that appends a row per run (a sensor's history) compares only
    # this cycle's rows, whatever earlier cycles did on either side.
    full_refresh: str = "never"
    # dbt variable holding the cycle's clock, the same on both sides: the UTC
    # wall-clock time the cycle started (`YYYY-MM-DD HH:MM:SS.fff`), and in
    # `<var>_local` the same instant in `clock_local_zone`. Empty: no clock.
    clock_var: str = ""
    clock_local_zone: str = "America/Sao_Paulo"
    rss_interval_ms: int = 200
    rss_max_ratio: float | None = None
    sample: int = 10
    show_rows: bool = False
    duckdb_memory: str = "2GB"
    s3_endpoint: str = ""
    s3_region: str = ""
    s3_path_style: bool = False
    expected_differences: str = ""
    expected: list[expected.Rule] = dataclasses.field(default_factory=list)
    parquet_time_zone: str = ""
    double_digits: int = compare.DOUBLE_DIGITS
    real_digits: int = compare.REAL_DIGITS
    float_recheck_limit: int = compare.FLOAT_RECHECK_LIMIT


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


def digits(value: str, name: str, maximum: int) -> int:
    """Significant digits floats are compared to: 0 (exact text) to ``maximum``."""
    number = _int(value, name, 0)
    if number > maximum:
        raise ConfigError(f"{name} must be <= {maximum} (0 compares the exact text)")
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


def time_zone(value: str, name: str) -> str:
    """An IANA zone name (``America/Sao_Paulo``, ``UTC``), or empty."""
    zone = value.strip()
    if not zone:
        return ""
    try:
        zoneinfo.ZoneInfo(zone)
    except (zoneinfo.ZoneInfoNotFoundError, ValueError):
        raise ConfigError(f"{name} must be an IANA time zone name (e.g. America/Sao_Paulo or UTC)") from None
    return zone


def table_names(value: list[str], name: str) -> list[str]:
    """``schema.table`` entries; anything else is refused, never dropped."""
    bad = [t for t in value if len(t.split(".")) != 2 or not all(part.strip() for part in t.split("."))]
    if bad:
        raise ConfigError(f"{name}: expected schema.table entries, got {', '.join(bad)}")
    return value


def expected_path(value: str) -> str:
    """The rule file: none when unset or ``none``, the shipped one with
    ``default``, else the path given."""
    choice = value.strip()
    if choice.lower() in {"", "none"}:
        return ""
    if choice.lower() == "default":
        return DEFAULT_EXPECTED
    return choice


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
                # The reference engine writes Trino's file metastore names;
                # CodeLake its own unless its metastore_names say otherwise.
                metastore_file=get(
                    prefix + "METASTORE_FILE", ".trinoSchema" if side == "reference" else ".codelakeSchema"
                ),
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
    # SHADOW_FULL_REFRESH_FIRST=true is the older spelling of SHADOW_FULL_REFRESH=first.
    legacy_first = bool(check(_bool, get("SHADOW_FULL_REFRESH_FIRST", "false"), "SHADOW_FULL_REFRESH_FIRST"))
    full_refresh = get("SHADOW_FULL_REFRESH", "first" if legacy_first else "never")
    if full_refresh not in {"never", "first", "always"}:
        errors.append("SHADOW_FULL_REFRESH must be never, first or always")
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
        tables=check(table_names, _list(get("SHADOW_TABLES", "")), "SHADOW_TABLES") or [],
        ignore_columns=_list(get("SHADOW_IGNORE_COLUMNS", "")),
        cycles=check(_int, get("SHADOW_CYCLES", "1"), "SHADOW_CYCLES", 1) or 1,
        interval_secs=float(check(_int, get("SHADOW_INTERVAL_SECS", "0"), "SHADOW_INTERVAL_SECS", 0) or 0),
        cutoff_sql=cutoff_sql,
        cutoff_value=cutoff_value,
        cutoff_var=get("SHADOW_CUTOFF_VAR", "shadow_cutoff"),
        extra_vars=get("SHADOW_DBT_VARS", ""),
        full_refresh=full_refresh,
        clock_var=get("SHADOW_CLOCK_VAR", ""),
        clock_local_zone=get("SHADOW_CLOCK_LOCAL_ZONE", "America/Sao_Paulo"),
        rss_interval_ms=check(_int, get("SHADOW_RSS_INTERVAL_MS", "200"), "SHADOW_RSS_INTERVAL_MS", 10) or 200,
        rss_max_ratio=max_ratio,
        sample=check(_int, get("SHADOW_SAMPLE", "10"), "SHADOW_SAMPLE", 0) or 0,
        show_rows=bool(check(_bool, get("SHADOW_SHOW_ROWS", "false"), "SHADOW_SHOW_ROWS")),
        duckdb_memory=get("SHADOW_DUCKDB_MEMORY", "2GB"),
        s3_endpoint=get("SHADOW_S3_ENDPOINT", ""),
        s3_region=get("SHADOW_S3_REGION", ""),
        s3_path_style=bool(check(_bool, get("SHADOW_S3_PATH_STYLE", "false"), "SHADOW_S3_PATH_STYLE")),
    )
    config.parquet_time_zone = check(time_zone, get("SHADOW_PARQUET_TIME_ZONE", ""), "SHADOW_PARQUET_TIME_ZONE") or ""
    for attr, var, maximum in (("double_digits", "SHADOW_DOUBLE_DIGITS", 17), ("real_digits", "SHADOW_REAL_DIGITS", 9)):
        number = check(digits, get(var, str(getattr(config, attr))), var, maximum)
        if number is not None:
            setattr(config, attr, number)
    limit = check(_int, get("SHADOW_FLOAT_RECHECK_LIMIT", str(config.float_recheck_limit)), "SHADOW_FLOAT_RECHECK_LIMIT", 0)
    if limit is not None:
        config.float_recheck_limit = limit
    if max_ratio is not None and not (reference.rss and candidate.rss):
        errors.append(
            "SHADOW_RSS_MAX_RATIO needs both SHADOW_REFERENCE_RSS and SHADOW_CANDIDATE_RSS: "
            "a memory gate without a meter on each side would never fail"
        )
    config.expected_differences = expected_path(get("SHADOW_EXPECTED_DIFFERENCES", ""))
    try:
        config.expected = expected.load(config.expected_differences)
    except expected.ExpectedError as err:
        errors.append(str(err))
    for name, var in (("SHADOW_CUTOFF_VAR", config.cutoff_var), ("SHADOW_CLOCK_VAR", config.clock_var or "x")):
        if not var.replace("_", "a").isalnum():
            errors.append(f"{name} must be a dbt variable name ([A-Za-z0-9_]+)")
    if config.clock_var:
        check(time_zone, config.clock_local_zone, "SHADOW_CLOCK_LOCAL_ZONE")
    if errors:
        raise ConfigError("invalid shadow-run configuration:\n  - " + "\n  - ".join(errors))
    return config
