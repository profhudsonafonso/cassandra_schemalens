"""Isolation guard for timed V9 work on the shared Cassandra node.

Same rule as V8-012 (reused/methodology/protocol_012.py: active_bulk_writers, pending_compactions,
require_idle_node, BulkWriterWatch): sample every 15 s, require an idle node (no bulk writer, 0 pending
compactions) for 10 min before a timed measurement, mark a contaminated measurement
INVALID_CONCURRENT_LOAD.  Difference (D-V9-003): the pattern list also covers any interpreter of the
V8 checkout (rubis3_claude) and of the sibling checkout (rubis3), because V9 runs from a third directory.
"""
from __future__ import annotations

import shutil
from pathlib import Path
import subprocess
import threading
from typing import Any

from .common import CONTAINER, now_iso

BULK_WRITER_PATTERNS = (
    "Documents/rubis3/experiments/rubis_cassandra/.venv/bin/python",          # sibling checkout (Codex)
    "Documents/rubis3_claude/experiments/rubis_cassandra/.venv/bin/python",   # V8 checkout
    "schemalens_v9.load",                                                     # V9 bulk loaders
)


# D-V9-012: same exception as V8-012 (reused/methodology/protocol_012.active_bulk_writers). With the sibling's
# PAUSE_JOB marker present, its runner `sequential_baselines_011.py` returns 75 before opening any Cassandra
# session; a bare runner (no child `methodology.baseline_*` loader) is therefore a no-op and is recorded, not counted.
SIBLING_PAUSE_MARKER = Path("/Users/hudsonbatista/Documents/rubis3/experiments/rubis_cassandra/outputs/final_run_v8_completion_011/continuation/PAUSE_JOB")
PAUSED_NOOP_SIGNATURE = "methodology/sequential_baselines_011.py"
IGNORED_PAUSED_NOOPS: list[str] = []


def active_bulk_writers() -> list[str]:
    found = set()
    for pattern in BULK_WRITER_PATTERNS:
        p = subprocess.run(["pgrep", "-fl", pattern], capture_output=True, text=True)
        found.update(line for line in p.stdout.splitlines() if line.strip())
    if SIBLING_PAUSE_MARKER.exists():
        noops = {l for l in found if PAUSED_NOOP_SIGNATURE in l and "methodology.baseline_" not in l}
        if noops and not any("methodology.baseline_" in l for l in found):
            IGNORED_PAUSED_NOOPS.extend(f"{now_iso()} {l}" for l in sorted(noops))
            found -= noops
    return sorted(found)


def pending_compactions() -> int | None:
    p = subprocess.run(["docker", "exec", CONTAINER, "nodetool", "compactionstats"], capture_output=True, text=True, timeout=120)
    for line in p.stdout.splitlines():
        if line.strip().lower().startswith("pending tasks:"):
            try:
                return int(line.split(":", 1)[1].split()[0])
            except ValueError:
                return None
    return None


def node_status() -> dict[str, Any]:
    p = subprocess.run(["docker", "exec", CONTAINER, "nodetool", "status"], capture_output=True, text=True, timeout=120)
    lines = [l for l in p.stdout.splitlines() if l[:2] in ("UN", "DN", "UJ", "UL", "UM", "DJ", "DL", "DM")]
    return {"rc": p.returncode, "nodes": [l.split()[0] + " " + l.split()[1] for l in lines], "up_normal": bool(lines) and all(l.startswith("UN") for l in lines)}


def disk() -> dict[str, Any]:
    u = shutil.disk_usage(str(__import__("pathlib").Path.home()))
    return {"host_free_gb": round(u.free / 1e9, 1), "host_total_gb": round(u.total / 1e9, 1)}


def snapshot() -> dict[str, Any]:
    writers = active_bulk_writers()
    comp = pending_compactions()
    return {"checked_at": now_iso(), "bulk_writers": writers, "pending_compactions": comp, "idle": not writers and comp == 0}


class BulkWriterWatch:
    def __init__(self, interval_s: float = 15.0):
        self.interval_s, self.samples, self.seen = interval_s, 0, []
        self._ign0 = len(IGNORED_PAUSED_NOOPS)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True)

    def _loop(self):
        while not self._stop.is_set():
            w = active_bulk_writers()
            self.samples += 1
            if w:
                self.seen.append({"at": now_iso(), "writers": w})
            self._stop.wait(self.interval_s)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=30)

    def report(self) -> dict[str, Any]:
        return {"samples": self.samples, "interval_s": self.interval_s, "concurrent_bulk_writer_samples": len(self.seen),
                "first_observations": self.seen[:5], "clean": not self.seen,
                "ignored_paused_sibling_runner_noops": {"count": len(IGNORED_PAUSED_NOOPS) - self._ign0, "first": IGNORED_PAUSED_NOOPS[self._ign0:self._ign0 + 5],
                                                        "marker": str(SIBLING_PAUSE_MARKER)}}
