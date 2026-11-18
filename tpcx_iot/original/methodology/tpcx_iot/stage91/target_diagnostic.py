"""Stage 9.1: verify that Cassandra driver and Docker nodetool target the SAME node.

Research single-node Cassandra only. This preflight runs before any --reset
and prevents measurements from silently using a different Cassandra container.
No benchmarks or SchemaLens activation rules are changed.
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from methodology.tpcx_iot.stage9.experiment import CassandraClient

UUID_RE = re.compile(r'(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b')
SAFE_CONTAINER = re.compile(r'[A-Za-z0-9_.-]{1,100}')


def inspect_target(client: Any, container: str, *, output_path: Path) -> dict:
    """Compare the driver's `system.local.host_id` to Docker cqlsh's host_id.

    The result is persisted for both failures and successes. Docker's cqlsh is
    deliberately invoked inside the EXACT container used by nodetool. Do not
    infer matching nodes from Docker health or from the Cassandra version.
    """
    if not SAFE_CONTAINER.fullmatch(container):
        raise ValueError('Invalid Docker container name')
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report: dict = {
        'status': 'DIAGNOSTIC_FAILED',
        'docker_container': container,
        'driver_host_id': None,
        'container_host_id': None,
        'message': '',
        'destructive_actions': False,
        'tpc_compliant': False,
    }

    try:
        row = client.session.execute('SELECT host_id, cluster_name, release_version FROM system.local').one()
        if row is None or row.host_id is None:
            raise RuntimeError('Cassandra driver returned no system.local.host_id')
        report['driver_host_id'] = str(row.host_id).lower()
        report['driver_cluster_name'] = str(row.cluster_name)
        report['driver_release_version'] = str(row.release_version)

        command = ['docker', 'exec', container, 'cqlsh', '127.0.0.1', '9042', '-e',
                   'SELECT host_id FROM system.local;']
        report['docker_command'] = command
        proc = subprocess.run(command, capture_output=True, text=True, timeout=30, check=False)
        report['docker_returncode'] = proc.returncode
        report['docker_stdout_tail'] = (proc.stdout or '')[-2000:]
        report['docker_stderr_tail'] = (proc.stderr or '')[-2000:]
        if proc.returncode != 0:
            raise RuntimeError(f'cqlsh in {container} exited {proc.returncode}; '
                               'inspect docker_stdout_tail and docker_stderr_tail')
        ids = UUID_RE.findall(proc.stdout)
        if len(ids) != 1:
            raise RuntimeError(f'Expected exactly one host_id from Docker cqlsh; got {len(ids)}')
        report['container_host_id'] = ids[0].lower()
        if report['container_host_id'] == report['driver_host_id']:
            report['status'] = 'MATCH'
            report['message'] = 'The Python Cassandra driver and Docker nodetool container address the same Cassandra node.'
        else:
            report['status'] = 'TARGET_MISMATCH'
            report['message'] = ('The Python driver and Docker container target different Cassandra nodes. '
                                 'Check Docker published ports, the CLI --port/--hosts settings, '
                                 'and --nodetool-container BEFORE rerunning the benchmark.')
    except (OSError, subprocess.TimeoutExpired, RuntimeError, AttributeError) as exc:
        report['status'] = 'DIAGNOSTIC_FAILED'
        report['message'] = f'{type(exc).__name__}: {exc}'
    output_path.write_text(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + '\n',
                           encoding='utf-8')
    report['diagnostic_path'] = str(output_path)
    return report


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description='Read-only Cassandra driver vs Docker target comparison')
    ap.add_argument('--hosts', default='127.0.0.1')
    ap.add_argument('--port', type=int, default=9042)
    ap.add_argument('--container', default='schemalens_tpcx_cassandra')
    ap.add_argument('--output', default='outputs/stage91_target_diagnostic.json')
    a = ap.parse_args(argv)
    client = CassandraClient(a.hosts.split(','), a.port)
    try:
        report = inspect_target(client, a.container, output_path=Path(a.output).resolve())
    finally:
        client.close()
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0 if report['status'] == 'MATCH' else 2


if __name__ == '__main__':
    raise SystemExit(main())
