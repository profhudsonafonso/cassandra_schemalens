"""Ambiente Cassandra do protocolo comum v1: container dedicado, preflight,
gate de flush, armazenamento e guarda de isolamento.

Regras de seguranca: so cria um container NOVO com nome proprio (recusa se ja
existir), so le os keyspaces criados pela campanha e nunca executa DROP/TRUNCATE.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import platform
import re
import subprocess
import threading
import time
from pathlib import Path

PROTOCOL_PATH = Path(__file__).with_name('protocol_v1.json')


def protocol():
    return json.loads(PROTOCOL_PATH.read_text(encoding='utf8'))


def sh(cmd, check=True, timeout=600):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if check and r.returncode != 0:
        raise RuntimeError(f'{cmd!r} failed: {r.stderr.strip()[:800]}')
    return r.stdout


def container_exists(name):
    return bool(sh(['docker', 'ps', '-a', '--filter', f'name=^{name}$', '--format', '{{.Names}}']).strip())


def start_container(name, port, proto=None):
    """Create the dedicated, pinned container. Refuses to reuse an existing one."""
    proto = proto or protocol()
    env = proto['environment']
    if container_exists(name):
        raise FileExistsError(f'Container {name} already exists; v1 requires a fresh dedicated node')
    sh(['docker', 'run', '-d', '--name', name,
        '--cpuset-cpus', env['container_cpuset'], '--memory', env['container_memory_limit'],
        '-e', f"MAX_HEAP_SIZE={env['max_heap_size']}", '-e', f"HEAP_NEWSIZE={env['heap_newsize']}",
        '-e', 'CASSANDRA_CLUSTER_NAME=schemalens_v1',
        '-p', f'127.0.0.1:{port}:9042', env['cassandra_image']])
    wait_ready(name, port)


def wait_ready(name, port, timeout=300):
    from cassandra.cluster import Cluster
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            c = Cluster(['127.0.0.1'], port=port, connect_timeout=5)
            s = c.connect()
            s.execute('SELECT release_version FROM system.local')
            c.shutdown()
            # nodetool also has to answer (JMX up)
            nodetool(name, 'status')
            return time.time() - t0
        except Exception:
            time.sleep(3)
    raise TimeoutError(f'Cassandra {name} not ready after {timeout}s')


def restart(name, port):
    """Cold restart: JVM, key/row caches and memtables discarded (data is flushed)."""
    t0 = time.time()
    sh(['docker', 'restart', '-t', '60', name])
    wait_ready(name, port)
    return time.time() - t0


def nodetool(name, *args, timeout=600):
    return sh(['docker', 'exec', name, 'nodetool', *args], timeout=timeout)


def flush(name, keyspaces):
    for ks in keyspaces:
        nodetool(name, 'flush', ks)


def wait_compactions(name, timeout=600):
    t0 = time.time()
    while time.time() - t0 < timeout:
        out = nodetool(name, 'compactionstats')
        m = re.search(r'pending tasks:\s*(\d+)', out)
        if m and int(m.group(1)) == 0 and 'compaction type' not in out.lower():
            return time.time() - t0
        time.sleep(2)
    raise TimeoutError('Compactions did not settle')


def tablestats(name, keyspaces):
    """Parse nodetool tablestats for the given keyspaces -> {(ks, table): dict}."""
    out = nodetool(name, 'tablestats', *keyspaces)
    stats, ks, tb = {}, None, None
    for line in out.splitlines():
        s = line.strip()
        if s.startswith('Keyspace :') or s.startswith('Keyspace:'):
            ks = s.split(':', 1)[1].strip(); tb = None
        elif s.startswith('Table:'):
            tb = s.split(':', 1)[1].strip(); stats[(ks, tb)] = {}
        elif tb and ':' in s:
            k, v = s.split(':', 1)
            stats[(ks, tb)][k.strip()] = v.strip()
    def num(x):
        try:
            return float(x.split()[0])
        except Exception:
            return None
    res = {}
    for key, d in stats.items():
        res[key] = {'sstable_count': int(num(d.get('SSTable count', '0')) or 0),
                    'memtable_cell_count': int(num(d.get('Memtable cell count', '0')) or 0),
                    'memtable_data_size': int(num(d.get('Memtable data size', '0')) or 0),
                    'space_used_live_bytes': int(num(d.get('Space used (live)', '0')) or 0),
                    'space_used_total_bytes': int(num(d.get('Space used (total)', '0')) or 0),
                    'number_of_partitions': int(num(d.get('Number of partitions (estimate)', '0')) or 0),
                    'compacted_partition_mean_bytes': int(num(d.get('Compacted partition mean bytes', '0')) or 0),
                    'compacted_partition_max_bytes': int(num(d.get('Compacted partition maximum bytes', '0')) or 0)}
    return res


def flush_gate(name, keyspaces):
    """Gate: every table has SSTable >= 1 and an empty memtable."""
    st = tablestats(name, keyspaces)
    if not st:
        raise AssertionError('flush gate: no tables found')
    bad = {f'{k[0]}.{k[1]}': v for k, v in st.items()
           if v['sstable_count'] < 1 or v['memtable_cell_count'] != 0}
    return {'passed': not bad, 'tables': len(st), 'violations': bad,
            'detail': {f'{k[0]}.{k[1]}': v for k, v in st.items()}}


def preflight(name, port, extra=None):
    from cassandra.cluster import Cluster
    import cassandra
    c = Cluster(['127.0.0.1'], port=port)
    s = c.connect()
    rel = s.execute('SELECT release_version FROM system.local').one().release_version
    c.shutdown()
    image = sh(['docker', 'inspect', name, '--format', '{{.Config.Image}}']).strip()
    image_id = sh(['docker', 'inspect', name, '--format', '{{.Image}}']).strip()
    hc = json.loads(sh(['docker', 'inspect', name, '--format', '{{json .HostConfig}}']))
    others = sh(['docker', 'ps', '--format', '{{.Names}}\t{{.Image}}\t{{.Status}}']).strip().splitlines()
    stats = sh(['docker', 'stats', '--no-stream', '--format', '{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}'], timeout=120).strip().splitlines()
    mem = Path('/proc/meminfo').read_text().splitlines()[:3]
    return {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'container': name, 'port': port,
            'cassandra_release_version': rel, 'image': image, 'image_id': image_id,
            'cpuset': hc.get('CpusetCpus'), 'memory_limit_bytes': hc.get('Memory'),
            'python': platform.python_version(), 'driver': cassandra.__version__,
            'host': platform.node(), 'kernel': platform.release(), 'cpu_count': os.cpu_count(),
            'client_affinity': sorted(os.sched_getaffinity(0)), 'meminfo': mem,
            'loadavg': os.getloadavg(), 'running_containers': others, 'docker_stats': stats,
            **(extra or {})}


class IsolationGuard:
    """Samples active compactions every `interval` s on a separate connection and
    counts compaction_history rows of the measured keyspaces before/after."""

    def __init__(self, port, keyspaces, interval=15.0):
        self.port, self.keyspaces, self.interval = port, set(keyspaces), interval
        self.samples, self._stop = [], threading.Event()

    def _history(self, session):
        n = 0
        for r in session.execute('SELECT keyspace_name FROM system.compaction_history'):
            n += r.keyspace_name in self.keyspaces
        return n

    def _active(self, session):
        rows = list(session.execute('SELECT keyspace_name, table_name, kind FROM system_views.sstable_tasks'))
        return [f'{r.keyspace_name}.{r.table_name}:{r.kind}' for r in rows]

    def __enter__(self):
        from cassandra.cluster import Cluster
        self.cluster = Cluster(['127.0.0.1'], port=self.port)
        self.session = self.cluster.connect()
        self.hist_before = self._history(self.session)
        self.t0 = time.time()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        return self

    def _run(self):
        while not self._stop.wait(self.interval):
            try:
                act = self._active(self.session)
                self.samples.append({'t': round(time.time() - self.t0, 1), 'active_tasks': act,
                                     'loadavg1': os.getloadavg()[0]})
            except Exception as exc:  # recorded, makes the window invalid
                self.samples.append({'t': round(time.time() - self.t0, 1), 'error': str(exc)})

    def __exit__(self, *exc):
        self._stop.set(); self.thread.join()
        self.samples.append({'t': round(time.time() - self.t0, 1), 'active_tasks': self._active(self.session),
                             'loadavg1': os.getloadavg()[0]})
        self.hist_after = self._history(self.session)
        self.cluster.shutdown()
        return False

    def report(self):
        active = [s for s in self.samples if s.get('active_tasks') or s.get('error')]
        new_hist = self.hist_after - self.hist_before
        return {'passed': not active and new_hist == 0, 'samples': len(self.samples),
                'interval_s': self.interval, 'active_compaction_samples': active,
                'compaction_history_new_rows': new_hist,
                'window_seconds': round(time.time() - self.t0, 1),
                'loadavg1_max': max((s.get('loadavg1', 0) for s in self.samples), default=None)}
