"""Shared paths and helpers for the V9 run. Nothing here writes outside the V9 repository."""
from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any

V9 = Path(__file__).resolve().parents[1]
OLD = Path.home() / "Documents/rubis3_claude/experiments/rubis_cassandra"   # V1–V8, READ-ONLY
OLD_RUN_012 = OLD / "outputs/final_run_v8_completion_012"
OLD_RUN_011 = OLD / "outputs/final_run_v8_completion_011"
RUN_ID = "v9-generation-20261112-001"
RUN = V9 / "runs/v9_generation_001"
STAGES_DIR = RUN / "stages"
STATE = RUN / "STATE.json"
LOG = RUN / "run.log"
MONITOR_DIR = V9 / "monitor"
CONTAINER = "rubis-schemalens-cassandra"
KEYSPACE_PREFIX = "rubis_v9_"
SOURCE_KEYSPACE = "rubis_full"


def now_iso() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


def _guard(path: Path) -> None:
    p = Path(path).resolve()
    if str(p).startswith(str(OLD.resolve())):
        raise PermissionError(f"refusing to write inside the read-only V8 repository: {p}")


def write_json(path: Path, value: Any) -> None:
    """Atomic JSON write (temp file + rename)."""
    _guard(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=path.suffix)
    with os.fdopen(fd, "w") as fh:
        json.dump(value, fh, indent=2, sort_keys=True, default=str, ensure_ascii=False)
        fh.write("\n")
    os.replace(tmp, path)


def write_text(path: Path, text: str) -> None:
    _guard(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".tmp_", suffix=path.suffix)
    with os.fdopen(fd, "w") as fh:
        fh.write(text)
    os.replace(tmp, path)


def read_json(path: Path, default: Any = None) -> Any:
    try:
        return json.loads(Path(path).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def sha256_obj(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def log(msg: str) -> None:
    LOG.parent.mkdir(parents=True, exist_ok=True)
    with open(LOG, "a") as fh:
        fh.write(f"{now_iso()} {msg}\n")


def file_ref(path: Path) -> dict:
    p = Path(path)
    return {"path": str(p), "sha256": sha256_file(p), "bytes": p.stat().st_size}


class Timer:
    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        self.seconds = round(time.time() - self.t0, 3)
