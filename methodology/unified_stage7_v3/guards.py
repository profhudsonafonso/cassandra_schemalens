"""Scientific firewall for Stage 7/8 V3: no measured performance may be opened by selection code."""
from __future__ import annotations

import builtins
import contextlib
import io
import re
from pathlib import Path

FORBIDDEN = re.compile(r'(raw_latenc|stage9|stage91|benchmark_summary|p95|latency_ms|latenc|E20_benchmark|E21_analysis|'
                       r'E22_|E23_|COST_MODEL\.json|final_run_v8_completion_012|/outputs/v1_run1|query_p95|summaries\.json|'
                       r'variant_summary|measured_space|method_eval|raw_reads)', re.I)
OPENED: list[str] = []


class FirewallViolation(RuntimeError):
    pass


def check_path(p) -> str:
    s = str(p)
    # Source code (.py) is allowed: e.g. the deterministic TPCx event generator lives in stage9/experiment.py.
    # Data/result artefacts (json, csv, gz, pkl, txt, ...) are checked.
    if not s.endswith('.py') and FORBIDDEN.search(s):
        raise FirewallViolation(f'Stage 7/8 V3 attempted to read measured-performance artefact: {s}')
    OPENED.append(s)
    return s


@contextlib.contextmanager
def firewall():
    """Patch open()/Path.read_text/read_bytes so every file read during selection is checked and logged."""
    o_open, o_rt, o_rb, o_ioopen = builtins.open, Path.read_text, Path.read_bytes, io.open

    def g_open(file, *a, **k):
        if isinstance(file, (str, Path)):
            check_path(file)
        return o_open(file, *a, **k)

    def g_rt(self, *a, **k):
        check_path(self); return o_rt(self, *a, **k)

    def g_rb(self, *a, **k):
        check_path(self); return o_rb(self, *a, **k)
    builtins.open, io.open, Path.read_text, Path.read_bytes = g_open, g_open, g_rt, g_rb
    try:
        yield OPENED
    finally:
        builtins.open, io.open, Path.read_text, Path.read_bytes = o_open, o_ioopen, o_rt, o_rb


def assert_no_legacy_cost():
    import sys
    if 'schemalens_v9.cost' in sys.modules:
        raise FirewallViolation('legacy NNLS module schemalens_v9.cost is loaded in a V3 selection process')
