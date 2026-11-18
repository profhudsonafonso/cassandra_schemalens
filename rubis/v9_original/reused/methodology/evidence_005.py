"""Evidence storage, exact value comparison and process ownership for V8-005."""
from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import math
import os
import subprocess
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN_ID = "v8-completion-20261108-005"
RUN = ROOT / "outputs/final_run_v8_completion_005"


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def file_hash(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def code_identity():
    files = sorted([*ROOT.glob("methodology/*.py"), *ROOT.glob("config/*.json")])
    hashes = {str(p.relative_to(ROOT)): file_hash(p) for p in files}
    return {"kind": "content_addressed_source_tree", "files": hashes,
            "sha256": hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(),
            "git_commit": None}


def atomic_json(path, value, *, preserve=True):
    """Preserve every prior observation before replacing a mutable index."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = (json.dumps(value, indent=2, sort_keys=True, default=str, allow_nan=False) + "\n").encode()
    if path.exists():
        previous = path.read_bytes()
        if previous == data:
            return
        if preserve:
            history = path.parent / "history" / (path.name + "." + hashlib.sha256(previous).hexdigest())
            history.parent.mkdir(parents=True, exist_ok=True)
            if not history.exists():
                history.write_bytes(previous)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    with temp.open("wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)


@contextlib.contextmanager
def process_lock(path):
    """OS lock, released after crash; a stale PID never constitutes ownership."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"An active process owns {path}") from exc
        stream.seek(0)
        stream.truncate()
        stream.write(json.dumps({"pid": os.getpid(), "started_at": utcnow()}))
        stream.flush()
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def exact_value(value):
    """Canonicalize numbers by exact rational value without rounding decimals."""
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, (int, float, Decimal, Fraction)):
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("Non-finite logical number")
        if isinstance(value, Decimal) and not value.is_finite():
            raise ValueError("Non-finite logical decimal")
        number = Fraction(value)
        return {"number": [str(number.numerator), str(number.denominator)]}
    if isinstance(value, datetime):
        dt = value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
        return {"timestamp": dt.isoformat(timespec="microseconds")}
    if isinstance(value, date):
        return {"date": value.isoformat()}
    if isinstance(value, dict):
        return {str(k): exact_value(v) for k, v in sorted(value.items(), key=lambda x: str(x[0]))}
    if isinstance(value, (list, tuple)):
        return [exact_value(x) for x in value]
    raise TypeError(f"Unspecified logical serialization: {type(value).__name__}")


def canonical_rows(rows, ordered=True):
    values = [exact_value(r) for r in rows]
    if not ordered:
        values.sort(key=lambda x: json.dumps(x, sort_keys=True, separators=(",", ":")))
    return values


def compare_rows(expected, actual, *, ordered=True):
    expected = canonical_rows(expected, ordered)
    actual = canonical_rows(actual, ordered)
    def digest(value):
        return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"status": "PASS" if expected == actual else "FAIL",
            "expected": expected, "actual": actual, "ordered": ordered,
            "expected_cardinality": len(expected), "actual_cardinality": len(actual),
            "expected_hash": digest(expected), "actual_hash": digest(actual)}


def legacy_write_acceptance(row):
    """A claimed status never overrides missing or contradictory evidence."""
    expected = row.get("expected_state_hash", row.get("expected_after_state_hash"))
    actual = row.get("actual_state_hash", row.get("actual_after_state_hash"))
    if not expected or not actual or expected != actual:
        return "FAIL"
    if not all(row.get(key) is True for key in (
            "logical_equivalence", "physical_projections_match", "retry_equivalent", "recovery_validated")):
        return "PARTIAL"
    if not row.get("before_state_hash") or not row.get("expected_state") or not row.get("actual_state"):
        return "PARTIAL"
    if not row.get("recovery_trace") or not row.get("required_projection_inventory"):
        return "PARTIAL"
    return "PASS"


def command(args, timeout=60):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    return {"command": args, "returncode": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr}


def preserve_prior_outputs():
    previous = [p for p in sorted((ROOT / "outputs").glob("final_run*"))
                if p.is_dir() and p != RUN]
    hashes = {str(p.relative_to(ROOT)): file_hash(p)
              for folder in previous for p in sorted(folder.rglob("*")) if p.is_file()}
    path = RUN / "preflight/PRIOR_OUTPUT_HASHES.json"
    if path.exists():
        original = json.loads(path.read_text())["files"]
        changes = [p for p, digest in original.items() if hashes.get(p) != digest]
        if changes:
            raise RuntimeError(f"Prior outputs changed: {changes[:20]}")
    else:
        atomic_json(path, {"captured_at": utcnow(), "files": hashes})
    return {"status": "PASS", "files": len(hashes), "manifest": str(path.relative_to(ROOT)),
            "sha256": file_hash(path)}
