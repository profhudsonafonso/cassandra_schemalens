"""Deterministic paired RUBiS histories with reconstructable logical inputs."""
from __future__ import annotations

import hashlib
import json
import random
from datetime import datetime, timedelta

from .engine import GROUPS, STATEMENTS, WEIGHTS
from .evidence_005 import ROOT, RUN, atomic_json, code_identity, file_hash, utcnow

WORKLOADS = ("browsing", "bidding", "update10x", "update100x")
CONSTITUENTS = {g: [sid for sid, spec in sorted(STATEMENTS.items()) if spec[0] == g] for g in GROUPS}


def json_hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(",", ":")).encode()).hexdigest()


def histories(workload, repetition, count, domain, source_state_id, *, seed=42005, phase="measured"):
    """Generate inputs only; dependent writes are resolved by application execution.

    The historical sources give group frequencies but not parameter marginals.
    Uniform choices over an explicitly frozen domain are a research assumption.
    IDs are unique across workload, repetition and warm-up/measurement phases.
    """
    wi = WORKLOADS.index(workload)
    if phase not in ("warmup", "measured") or not 0 <= repetition < 10 or not 0 <= count <= 100000:
        raise ValueError("History namespace bounds exceeded")
    for key in ("users", "items", "categories", "regions", "comments"):
        if not domain.get(key):
            raise ValueError(f"Empty input domain {key}")
    namespace = wi*10000000 + repetition*1000000 + (0 if phase == "warmup" else 200000)
    rng = random.Random(seed + namespace)
    weights = [WEIGHTS[group][wi] for group in GROUPS]
    base_time = datetime(2030, 1, 1)
    for index in range(count):
        group = rng.choices(GROUPS, weights=weights, k=1)[0]
        number = namespace + index
        event_id = f"c005:{workload}:r{repetition}:{phase}:{index}"
        source_item = rng.choice(domain["items"])
        source_user = rng.choice(domain["users"])
        params = {"item_id": source_item, "user_id": source_user,
                  "category_id": rng.choice(domain["categories"]),
                  "region_id": rng.choice(domain["regions"]),
                  "comment_id": rng.choice(domain["comments"]),
                  "from_user_id": rng.choice(domain["users"]), "to_user_id": source_user,
                  "since": "2010-01-01T00:00:00", "new_id": 1000000000 + number,
                  "date": (base_time + timedelta(seconds=number)).isoformat(),
                  "end_date": (base_time + timedelta(seconds=number, days=7)).isoformat(),
                  "name": f"c005-item-{number}", "description": f"event {event_id}",
                  "nickname": f"c005-user-{number}", "firstname": "Research", "lastname": str(number),
                  "email": f"c005-{number}@rubis.test", "password": "research-only",
                  "initial_price": rng.randint(10, 1000)/4, "reserve_price": 0.0,
                  "buy_now": 500.0, "quantity": rng.randint(1, 10), "qty": rng.randint(1, 3),
                  "bid": rng.randint(100, 3000)/4, "rating": rng.randint(-5, 5),
                  "comment": f"comment {event_id}"}
        yield {"logical_request_id": event_id, "logical_event_ids": [f"{event_id}:s{sid}" for sid in CONSTITUENTS[group] if STATEMENTS[sid][1] == "write"],
               "request_group": group, "constituent_statement_ids": CONSTITUENTS[group],
               "workload": workload, "repetition": repetition, "phase": phase,
               "parameters": params, "parameter_hash": json_hash(params),
               "source_state_id": source_state_id, "generator_seed": seed,
               "history_namespace": namespace}


def freeze(domain, source_state_id, *, measured=10000, warmup=1000, repetitions=5):
    """Freeze once; mismatch is a new protocol, never a silent overwrite."""
    protocol = {"workloads": list(WORKLOADS), "request_unit": "complete_application_group",
                "groups": GROUPS, "constituents": CONSTITUENTS, "weights": WEIGHTS,
                "repetitions": repetitions, "requests_per_repetition": measured, "warmup": warmup,
                "consistency": "ONE", "concurrency": 1, "seed": 42005,
                "domain": domain, "source_state_id": source_state_id,
                "parameter_distribution": "uniform over frozen source-derived entity ID sets; documented reconstruction",
                "dependency_policy": "execute statements in group order; use current read values for updates; statement31 traverses every returned statement30 comment",
                "timing": "perf_counter_ns around complete group, perf_counter_ns around measured interval; exclude warmup/reset/oracle",
                "correctness": "compare actual complete logical results and all write projections; no metadata-only hashes",
                "reset": "restore isolated experiment-owned logical state including all physical projections before each independent repetition",
                "historical_workload_sha256": file_hash(ROOT/"inputs/historical_sources/nose_workloads_rubis.rb")}
    path = RUN / "protocol/PARAMETERIZED_PROTOCOL.json"
    if path.exists():
        previous = json.loads(path.read_text())
        if previous["protocol"] != protocol:
            raise RuntimeError("Frozen protocol mismatch; retain prior protocol and create a separately identified experiment")
        return previous
    out = {"protocol": protocol, "protocol_sha256": json_hash(protocol), "frozen_at": utcnow(),
           "source_code": code_identity(), "status": "FROZEN_BEFORE_MEASUREMENT"}
    atomic_json(path, out)
    return out
