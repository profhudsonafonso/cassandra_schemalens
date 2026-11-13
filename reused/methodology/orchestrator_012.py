"""Resumable V8-012 orchestrator.

1. wait until the shared Cassandra node has been idle (no sibling-checkout
   interpreter, no pending compactions) for IDLE_WINDOW_S;
2. sync the sibling checkout's full-scale baseline evidence (no SQLite state);
3. run the timed stages (factorial ablation, matched-scale baselines);
4. analyse, compare, verify, write the report.

If a timed stage reports INVALID_CONCURRENT_LOAD or refuses to start because
the node became busy, the orchestrator returns to step 1 and retries.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

from . import protocol_012 as P

SIBLING = Path("/Users/hudsonbatista/Documents/rubis3/experiments/rubis_cassandra")
SIBLING_FULL = SIBLING / "outputs/final_run_v8_completion_011/continuation/full_baselines"
LOCAL_FULL = P.FROZEN_011 / "continuation/full_baselines"
STATE = P.RUN_DIR / "orchestration/STATE.json"
LOG_DIR = P.RUN_DIR / "orchestration"
IDLE_WINDOW_S = 600
POLL_S = 60
MAX_TIMED_ATTEMPTS = 4
PY = [sys.executable, "-m"]

# The baseline loader exists only in the sibling checkout; run it there with its own interpreter.
def sibling_cmd(args: str) -> list[str]:
    return ["/bin/sh", "-c", f"cd {SIBLING} && exec env -u PYTHONPATH .venv/bin/python -m methodology.baseline_continuation_011 {args}"]


TASKS = [
    ("wait_idle", None),
    ("sync_full_baselines", None),
    # full-scale Normalized: frozen-prefix sample on the sibling's reconciled keyspace, then release it
    ("full_sample_normalized", PY + ["methodology.full_baselines_sample_012", "run", "--baseline", "normalized"]),
    ("full_release_normalized", PY + ["methodology.full_baselines_sample_012", "release", "--baseline", "normalized"]),
    ("factorial_protocol", PY + ["methodology.ablation_factorial_012", "run", "--label", "protocol"]),
    ("factorial_analysis", PY + ["methodology.ablation_factorial_012", "analyze", "--label", "protocol"]),
    ("cohort_baselines_protocol", PY + ["methodology.cohort_baselines_012", "--label", "protocol"]),
    ("comparison", PY + ["methodology.comparison_012"]),
    ("acceptance", PY + ["methodology.acceptance_012", "verify"]),
    ("negative_tests", PY + ["methodology.acceptance_012", "negative-tests"]),
    ("report", PY + ["methodology.report_012"]),
    # full-scale Expert: cut by user decision 2026-11-10 (keyspace was only partially loaded); Expert is compared at cohort scale only
    # full-scale SchemaLens: validated derivation -> materialisation (~5-7 h) -> verification -> timed benchmark
    ("full_validate_derivation", PY + ["methodology.full_schemalens_012", "validate-derivation"]),
    ("full_build", PY + ["methodology.full_schemalens_012", "build"]),
    ("full_verify", PY + ["methodology.full_schemalens_012", "verify"]),
    ("full_bench", PY + ["methodology.full_schemalens_012", "bench"]),
    ("comparison_final", PY + ["methodology.comparison_012"]),
    ("acceptance_final", PY + ["methodology.acceptance_012", "verify"]),
    ("report_final", PY + ["methodology.report_012"]),
    # rebench (user request 2026-11-10): NoSE/QPG re-materialised and measured with SchemaLens in ONE idle window
    ("rebench_load_nose", PY + ["methodology.full_rebench_012", "load", "--baseline", "nose_bidding"]),
    ("rebench_load_qpg", PY + ["methodology.full_rebench_012", "load", "--baseline", "qpg_bidding"]),
    ("wait_idle_rebench", None),
    ("rebench_bench", PY + ["methodology.full_rebench_012", "bench"]),
    ("rebench_compare", PY + ["methodology.full_rebench_012", "compare"]),
    ("rebench_release", PY + ["methodology.full_rebench_012", "release"]),
    # cold-cache head-to-head (user request 2026-11-11): page cache + key cache dropped before every repetition
    ("cold_load_nose", PY + ["methodology.full_cold_012", "load", "--baseline", "nose_bidding"]),
    ("cold_load_qpg", PY + ["methodology.full_cold_012", "load", "--baseline", "qpg_bidding"]),
    ("wait_idle_cold", None),
    ("cold_bench", PY + ["methodology.full_cold_012", "bench"]),
    ("cold_compare", PY + ["methodology.full_cold_012", "compare"]),
]
TIMED = {"factorial_protocol", "cohort_baselines_protocol", "full_bench", "full_sample_normalized", "rebench_bench", "cold_bench"}
# acceptance may legitimately end INCOMPLETE; that is a result, not an orchestration failure
NONFATAL = {"acceptance", "acceptance_final"}
# untimed stages that still must not overlap a sibling's timed work
NEEDS_IDLE = {"full_validate_derivation", "full_build", "full_verify", "rebench_load_nose", "rebench_load_qpg", "cold_load_nose", "cold_load_qpg"}


def load_state() -> dict:
    return P.read_json(STATE, {"run_id": P.RUN_ID, "tasks": {}})


def save_state(state: dict) -> None:
    state["observed_at"] = P.now_iso()
    P.write_json(STATE, state)


def log(msg: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with (LOG_DIR / "orchestrator.log").open("a") as f:
        f.write(f"{P.now_iso()} {msg}\n")


def wait_idle(state: dict, key: str = "wait_idle") -> None:
    idle_since = None
    while True:
        writers, comp = P.active_bulk_writers(), P.pending_compactions()
        checkpoint = P.read_json(SIBLING_FULL / "SEQUENTIAL_BASELINE_CHECKPOINT.json", {})
        busy = bool(writers) or comp != 0
        if busy:
            idle_since = None
        elif idle_since is None:
            idle_since = time.time()
        state["tasks"][key] = {"status": "WAITING", "writers": len(writers), "pending_compactions": comp,
                                       "sibling_checkpoint": {k: v.get("status") for k, v in checkpoint.get("baselines", {}).items()},
                                       "idle_for_s": int(time.time() - idle_since) if idle_since else 0}
        save_state(state)
        if idle_since and time.time() - idle_since >= IDLE_WINDOW_S:
            state["tasks"][key]["status"] = "PASS"
            save_state(state)
            log(f"{key}: node idle for {IDLE_WINDOW_S}s; sibling checkpoint={state['tasks'][key]['sibling_checkpoint']}")
            return
        time.sleep(POLL_S)


def sync_full_baselines(state: dict) -> None:
    if not SIBLING_FULL.exists():
        state["tasks"]["sync_full_baselines"] = {"status": "PASS", "note": "sibling evidence absent"}
        return
    LOCAL_FULL.mkdir(parents=True, exist_ok=True)
    cmd = ["rsync", "-a", "--exclude", "*.sqlite3*", "--exclude", "ledger.sqlite", "--exclude", "writer.lock",
           f"{SIBLING_FULL}/", f"{LOCAL_FULL}/"]
    p = subprocess.run(cmd, capture_output=True, text=True)
    state["tasks"]["sync_full_baselines"] = {"status": "PASS" if p.returncode == 0 else "FAIL", "command": cmd,
                                             "returncode": p.returncode, "stderr": p.stderr[-2000:], "at": P.now_iso()}
    save_state(state)
    if p.returncode != 0:
        raise SystemExit("rsync of sibling full-scale evidence failed")


def run_cmd(name: str, cmd: list[str], state: dict) -> int:
    attempt = state["tasks"].get(name, {}).get("attempts", 0) + 1
    log_path = LOG_DIR / f"{name}.attempt{attempt}.log"
    state["tasks"][name] = {"status": "RUNNING", "attempts": attempt, "started_at": P.now_iso(), "log": str(log_path.relative_to(P.ROOT))}
    save_state(state)
    with log_path.open("w") as f:
        env = {**os.environ, "PYTHONPATH": str(P.ROOT)}
        env["PATH"] = env.get("PATH", "/usr/bin:/bin") + ":/Users/hudsonbatista/.docker/bin:/usr/local/bin"
        rc = subprocess.run(cmd, cwd=P.ROOT, stdout=f, stderr=subprocess.STDOUT, env=env).returncode
    state["tasks"][name].update({"returncode": rc, "finished_at": P.now_iso(), "status": "PASS" if rc == 0 else "FAIL"})
    save_state(state)
    log(f"{name} attempt {attempt} rc={rc}")
    return rc


def timed_failure_is_concurrency(name: str) -> bool:
    text = (P.ROOT / load_state()["tasks"][name]["log"]).read_text()
    return "INVALID_CONCURRENT_LOAD" in text or "refusing timed measurement" in text


TRANSIENT_MARKERS = ("NoHostAvailable", "OperationTimedOut", "ReadTimeout", "WriteTimeout", "Unavailable",
                     "ConnectionRefused", "Connection refused", "ConnectionReset", "Connection reset", "ConnectionShutdown",
                     "Cannot connect to the Docker daemon", "Error response from daemon", "BrokenPipe", "timed out")
MAX_ATTEMPTS_ANY = 6          # absolute cap per task, whatever the failure class
MAX_GENERIC_RETRIES = 1       # unknown failures: one retry after a pause (deterministic bugs fail twice and stop)
CONTAINER = "rubis-schemalens-cassandra"


def classify_failure(name: str) -> str:
    text = (P.ROOT / load_state()["tasks"][name]["log"]).read_text(errors="replace")[-20000:]
    if "INVALID_CONCURRENT_LOAD" in text or "refusing timed measurement" in text:
        return "concurrency"
    if any(m in text for m in TRANSIENT_MARKERS):
        return "transient_cassandra"
    return "unknown"


def docker(*args: str, timeout: int = 120) -> subprocess.CompletedProcess:
    env = {**os.environ, "PATH": os.environ.get("PATH", "/usr/bin:/bin") + ":/Users/hudsonbatista/.docker/bin:/usr/local/bin"}
    return subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, env=env)


def ensure_cassandra(max_wait_s: int = 1800) -> bool:
    """Start the container if it stopped and wait until CQL answers. Never restarts a running node."""
    deadline = time.time() + max_wait_s
    while time.time() < deadline:
        try:
            st = docker("inspect", "-f", "{{.State.Running}}", CONTAINER)
            if st.stdout.strip() != "true":
                log(f"recovery: container {CONTAINER} not running ({st.stdout.strip() or st.stderr.strip()[:200]}); starting it")
                docker("start", CONTAINER)
            elif docker("exec", CONTAINER, "cqlsh", "-e", "SELECT release_version FROM system.local", timeout=60).returncode == 0:
                return True
        except Exception as exc:  # docker daemon down / timeout
            log(f"recovery: docker check failed: {exc!r}")
        time.sleep(30)
    return False


def alert(msg: str) -> None:
    P.write_json(LOG_DIR / "ALERT.json", {"at": P.now_iso(), "message": msg})
    try:
        subprocess.run(["osascript", "-e", f'display notification "{msg[:180]}" with title "RUBiS V8-012"'], timeout=10)
    except Exception:
        pass


def wait_for_orphans(state: dict) -> None:
    """A task left RUNNING by a killed orchestrator may still have its child alive; never start a duplicate."""
    for name, cmd in TASKS:
        t = state["tasks"].get(name, {})
        if t.get("status") != "RUNNING" or not cmd:
            continue
        signature = " ".join(cmd[2:]) if cmd[0] != "/bin/sh" else cmd[-1].split("-m ", 1)[-1]
        while subprocess.run(["pgrep", "-f", "--", signature], capture_output=True, text=True).stdout.strip():
            log(f"{name}: child from a previous orchestrator still running; waiting for it")
            time.sleep(60)
        t.update({"status": "FAIL", "returncode": None, "note": "orchestrator restarted while task was running; re-run (stages are resumable/idempotent)",
                  "finished_at": P.now_iso()})
        save_state(state)


def resume() -> int:
    state = load_state()
    wait_for_orphans(state)
    i = 0
    while i < len(TASKS):
        name, cmd = TASKS[i]
        if state["tasks"].get(name, {}).get("status") == "PASS":
            i += 1
            continue
        if name.startswith("wait_idle"):
            wait_idle(state, name); i += 1; continue
        if name == "sync_full_baselines":
            sync_full_baselines(state); i += 1; continue
        if name in NEEDS_IDLE and (P.active_bulk_writers()):
            log(f"{name}: sibling work active; waiting for idle window")
            wait_idle(state)
        rc = run_cmd(name, cmd, state)
        state = load_state()
        if rc == 0 or name in NONFATAL:
            if name in NONFATAL and rc != 0:
                state["tasks"][name]["status"] = "PASS"; state["tasks"][name]["note"] = "verifier ran; see ACCEPTANCE_RESULT.json"
                save_state(state)
            i += 1
            continue
        attempts = state["tasks"][name]["attempts"]
        kind = classify_failure(name)
        state["tasks"][name]["failure_class"] = kind
        save_state(state)
        retries = state["tasks"][name].get("generic_retries", 0)
        if attempts >= MAX_ATTEMPTS_ANY:
            pass
        elif kind == "concurrency" and (name in TIMED or name in NEEDS_IDLE) and attempts < MAX_TIMED_ATTEMPTS:
            log(f"{name}: node became busy; waiting for idle window again")
            state["tasks"]["wait_idle"] = {"status": "PENDING"}
            save_state(state)
            wait_idle(state)
            continue
        elif kind == "transient_cassandra":
            log(f"{name}: transient Cassandra/Docker failure; checking the node before retrying")
            if ensure_cassandra():
                state = load_state()
                state["tasks"]["wait_idle"] = {"status": "PENDING"}
                save_state(state)
                wait_idle(state)
                continue
            alert(f"{name}: Cassandra did not come back within 30 min")
        elif kind == "unknown" and retries < MAX_GENERIC_RETRIES:
            log(f"{name}: unclassified failure; one retry after 120 s")
            state["tasks"][name]["generic_retries"] = retries + 1
            save_state(state)
            time.sleep(120)
            if name in TIMED or name in NEEDS_IDLE:
                state["tasks"]["wait_idle"] = {"status": "PENDING"}
                save_state(state)
                wait_idle(state)
            continue
        log(f"{name} failed ({kind}, attempt {attempts}); stopping")
        alert(f"{name} failed ({kind}) after {attempts} attempt(s); see {state['tasks'][name]['log']}")
        return 1
    log("all tasks finished")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=("resume", "status"))
    a = ap.parse_args()
    if a.command == "status":
        print(json.dumps(load_state(), indent=2, sort_keys=True)); return 0
    return resume()


if __name__ == "__main__":
    raise SystemExit(main())
