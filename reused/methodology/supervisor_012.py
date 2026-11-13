"""Keeps the V8-012 orchestrator running until every task has passed.

The orchestrator already retries concurrency and transient Cassandra failures itself; this supervisor
covers what it cannot: the orchestrator process crashing or stopping on a failure. It restarts it with
exponential back-off (2, 4, 8, 16, 30, 30 ... min) up to MAX_RESTARTS times, then raises an alert.
Run under caffeinate:  make v8-012-supervise
"""
from __future__ import annotations

import os
import subprocess
import sys
import time

from . import protocol_012 as P
from .orchestrator_012 import LOG_DIR, alert, load_state, TASKS

MAX_RESTARTS = 12
STATUS = LOG_DIR / "SUPERVISOR.json"


def all_passed() -> bool:
    tasks = load_state()["tasks"]
    return all(tasks.get(n, {}).get("status") == "PASS" for n, _ in TASKS)


def main() -> int:
    restarts = 0
    history = []
    while True:
        started = P.now_iso()
        P.write_json(STATUS, {"status": "RUNNING", "pid": os.getpid(), "restarts": restarts, "orchestrator_started_at": started, "history": history[-20:]})
        env = {**os.environ, "PYTHONPATH": str(P.ROOT)}
        with (LOG_DIR / "orchestrator.stdout.log").open("a") as f:
            rc = subprocess.run([sys.executable, "-m", "methodology.orchestrator_012", "resume"], cwd=P.ROOT, stdout=f, stderr=subprocess.STDOUT, env=env).returncode
        history.append({"started_at": started, "ended_at": P.now_iso(), "returncode": rc})
        if rc == 0 and all_passed():
            P.write_json(STATUS, {"status": "DONE", "pid": os.getpid(), "restarts": restarts, "finished_at": P.now_iso(), "history": history[-20:]})
            alert("V8-012 terminou: todas as etapas concluídas")
            return 0
        restarts += 1
        if restarts > MAX_RESTARTS:
            P.write_json(STATUS, {"status": "GAVE_UP", "pid": os.getpid(), "restarts": restarts, "at": P.now_iso(), "history": history[-20:]})
            alert(f"supervisor desistiu após {MAX_RESTARTS} reinícios; intervenção necessária")
            return 1
        wait_min = min(30, 2 ** restarts)
        P.write_json(STATUS, {"status": "BACKOFF", "pid": os.getpid(), "restarts": restarts, "next_restart_at": time.time() + 60 * wait_min,
                              "last_returncode": rc, "history": history[-20:]})
        time.sleep(60 * wait_min)


if __name__ == "__main__":
    raise SystemExit(main())
