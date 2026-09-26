"""Peak resident memory of an engine, sampled while its dbt run lasts.

A meter is written ``kind:target``:

- ``pid:<pid>``: the process and its descendants, from ``/proc/<pid>/status``
  (``VmRSS``). The harness container needs ``--pid=host`` to see the engine;
  ``scripts/shadow-run.sh`` adds it when a ``pid:`` meter is configured.
- ``container:<name or id>``: anonymous memory (``anon`` of ``memory.stat``,
  i.e. RSS without the page cache of the files the engine reads) of the
  container's cgroup v2, the same probe the benchmark uses for both engines.
  ``scripts/shadow-run.sh`` resolves the name to an id and mounts the cgroup
  root read-only at ``/hostcg``.
- ``cmd:<shell command>``: a command that prints the current RSS in bytes
  (optionally with a ``k``/``KiB``/``M``/``MiB``/``G``/``GiB`` suffix), for
  engines on another host (``ssh host ps -o rss= -p …``).

A sample that cannot be read is counted, not fatal: the report shows how many
samples succeeded.
"""

from __future__ import annotations

import os
import re
import subprocess
import threading
from dataclasses import dataclass, field

_UNITS = {
    "": 1, "b": 1,
    "k": 1024, "kb": 1024, "kib": 1024,
    "m": 1024**2, "mb": 1024**2, "mib": 1024**2,
    "g": 1024**3, "gb": 1024**3, "gib": 1024**3,
}


def parse_bytes(text: str) -> int:
    """``123``, ``123k``, ``1.5 GiB`` → bytes."""
    match = re.fullmatch(r"\s*([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]*)\s*", text)
    if not match:
        raise ValueError("not a byte count")
    unit = match.group(2).lower()
    if unit not in _UNITS:
        raise ValueError("unknown unit")
    return int(float(match.group(1)) * _UNITS[unit])


def _status_rss(proc: str, pid: int) -> int | None:
    try:
        with open(f"{proc}/{pid}/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return parse_bytes(line.split(":", 1)[1].strip())
    except OSError:
        return None
    return 0


def _children(proc: str) -> dict[int, list[int]]:
    tree: dict[int, list[int]] = {}
    try:
        entries = os.listdir(proc)
    except OSError:
        return tree
    for entry in entries:
        if not entry.isdigit():
            continue
        try:
            with open(f"{proc}/{entry}/stat", encoding="utf-8") as f:
                stat = f.read()
        except OSError:
            continue
        # The command name may hold spaces and parentheses; fields follow the last ')'.
        fields = stat.rsplit(")", 1)[-1].split()
        if len(fields) > 1 and fields[1].isdigit():
            tree.setdefault(int(fields[1]), []).append(int(entry))
    return tree


def process_tree_rss(pid: int, proc: str = "/proc") -> int | None:
    """RSS of ``pid`` plus its descendants; None when ``pid`` is gone."""
    root = _status_rss(proc, pid)
    if root is None:
        return None
    tree = _children(proc)
    total, stack, seen = root, list(tree.get(pid, [])), {pid}
    while stack:
        child = stack.pop()
        if child in seen:
            continue
        seen.add(child)
        total += _status_rss(proc, child) or 0
        stack.extend(tree.get(child, []))
    return total


def find_cgroup(root: str, container_id: str) -> str | None:
    """cgroup v2 directory of a container, for the cgroupfs and systemd drivers."""
    for candidate in (
        os.path.join(root, "docker", container_id),
        os.path.join(root, "system.slice", f"docker-{container_id}.scope"),
    ):
        if os.path.exists(os.path.join(candidate, "memory.stat")):
            return candidate
    return None


def cgroup_anon(directory: str) -> int | None:
    try:
        with open(os.path.join(directory, "memory.stat"), encoding="utf-8") as f:
            for line in f:
                key, _, value = line.partition(" ")
                if key == "anon":
                    return int(value)
    except (OSError, ValueError):
        return None
    return None


class MeterError(ValueError):
    pass


@dataclass
class Meter:
    """One configured meter; ``read()`` returns bytes or None."""

    spec: str
    container_id: str = ""
    cgroup_root: str = "/hostcg"
    proc: str = "/proc"
    timeout: float = 5.0
    _cgroup: str | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        kind, _, target = self.spec.partition(":")
        if kind not in {"pid", "container", "cmd"} or not target:
            raise MeterError("meter must be pid:<pid>, container:<name> or cmd:<command>")
        if kind == "pid" and not target.isdigit():
            raise MeterError("pid: meter needs a numeric pid")
        self.kind, self.target = kind, target

    def describe(self) -> str:
        # Never the command line of a cmd: meter: it may carry a host or a key.
        return "cmd" if self.kind == "cmd" else self.spec

    def read(self) -> int | None:
        if self.kind == "pid":
            return process_tree_rss(int(self.target), self.proc)
        if self.kind == "container":
            if self._cgroup is None:
                self._cgroup = find_cgroup(self.cgroup_root, self.container_id or self.target)
                if self._cgroup is None:
                    return None
            return cgroup_anon(self._cgroup)
        try:
            done = subprocess.run(
                ["sh", "-c", self.target],
                capture_output=True,
                text=True,
                timeout=self.timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired):
            return None
        if done.returncode != 0:
            return None
        try:
            return parse_bytes(done.stdout.strip().splitlines()[-1] if done.stdout.strip() else "")
        except ValueError:
            return None


class Sampler:
    """Samples a meter on a thread until stopped; keeps the peak."""

    def __init__(self, meter: Meter | None, interval: float) -> None:
        self.meter = meter
        self.interval = interval
        self.peak: int | None = None
        self.samples = 0
        self.failed = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _sample(self) -> None:
        assert self.meter is not None
        value = self.meter.read()
        if value is None:
            self.failed += 1
            return
        self.samples += 1
        if self.peak is None or value > self.peak:
            self.peak = value

    def _run(self) -> None:
        while not self._stop.is_set():
            self._sample()
            self._stop.wait(self.interval)

    def __enter__(self) -> "Sampler":
        if self.meter is not None:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        if self._thread is not None:
            self._stop.set()
            self._thread.join()
            # One last sample: a run shorter than the interval still gets two.
            self._sample()

    def result(self) -> dict:
        return {
            "meter": self.meter.describe() if self.meter else None,
            "peak_bytes": self.peak,
            "samples": self.samples,
            "failed_samples": self.failed,
            "interval_ms": int(self.interval * 1000),
        }

