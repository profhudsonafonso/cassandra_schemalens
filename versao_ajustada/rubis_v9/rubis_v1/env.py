"""Ambiente do protocolo v1 para o RUBiS (dois bracos: 4.1.12 e 3.11.11), portatil (Linux/macOS).

Reaproveita versao_ajustada/common/cassandra_env.py sem altera-lo (ele esta no hash dos pre-registros TSDB).
Guarda de isolamento SIMETRICA nas duas versoes (3.11 nao tem system_views): diferenca de
system.compaction_history antes/depois da janela por keyspace. Compactacoes nos keyspaces medidos sao
telemetria (as cargas de atualizacao escrevem); compactacoes em outros keyspaces nao-system invalidam a janela.
Escritores externos sao vigiados pelo BulkWriterWatch da V9.
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import os
import platform
import time
from collections import Counter

import cassandra_env as E
from common_paths import ARMS


def arm_protocol(arm):
    proto = copy.deepcopy(E.protocol())
    if ARMS[arm]['image']:
        proto['environment']['cassandra_image'] = ARMS[arm]['image']
        proto['environment']['cassandra_release_version'] = ARMS[arm]['version']
    return proto


def docker_resources():
    info = json.loads(E.sh(['docker', 'info', '--format', '{{json .}}']))
    return {'ncpu': info.get('NCPU'), 'mem_bytes': info.get('MemTotal'), 'os': info.get('OperatingSystem'),
            'arch': info.get('Architecture'), 'server_version': info.get('ServerVersion')}


def check_resources(proto):
    r = docker_resources()
    need = int(proto['environment']['container_cpuset'].split('-')[-1]) + 1
    problems = []
    if (r['ncpu'] or 0) < need:
        problems.append(f"Docker tem {r['ncpu']} CPUs (precisa >= {need})")
    if (r['mem_bytes'] or 0) < 7 * 2**30:
        problems.append('Docker tem menos de 7 GiB')
    return r, problems


def pin_client(proto):
    if hasattr(os, 'sched_setaffinity'):
        try:
            os.sched_setaffinity(0, set(proto['environment']['client_cpu_affinity']))
            return sorted(os.sched_getaffinity(0))
        except OSError as exc:
            return f'nao aplicado: {exc}'
    return 'nao suportado neste SO'


def preflight(arm, extra=None):
    from cassandra.cluster import Cluster
    import cassandra
    a = ARMS[arm]
    c = Cluster(['127.0.0.1'], port=a['port']); s = c.connect()
    rel = s.execute('SELECT release_version FROM system.local').one()[0]
    c.shutdown()
    hc = json.loads(E.sh(['docker', 'inspect', a['container'], '--format', '{{json .HostConfig}}']))
    return {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'arm': arm, 'container': a['container'], 'port': a['port'],
            'cassandra_release_version': rel,
            'image': E.sh(['docker', 'inspect', a['container'], '--format', '{{.Config.Image}}']).strip(),
            'image_id': E.sh(['docker', 'inspect', a['container'], '--format', '{{.Image}}']).strip(),
            'cpuset': hc.get('CpusetCpus'), 'memory_limit_bytes': hc.get('Memory'),
            'python': platform.python_version(), 'driver': cassandra.__version__, 'host_os': platform.platform(),
            'machine': platform.machine(), 'cpu_count': os.cpu_count(), 'docker': docker_resources(),
            'loadavg': os.getloadavg() if hasattr(os, 'getloadavg') else None,
            'running_containers': E.sh(['docker', 'ps', '--format', '{{.Names}}\t{{.Image}}\t{{.Status}}']).strip().splitlines(),
            **(extra or {})}


def is_running(container):
    return E.sh(['docker', 'inspect', container, '--format', '{{.State.Running}}'], check=False).strip() == 'true'


def stop(container):
    if is_running(container):
        E.sh(['docker', 'stop', '-t', '120', container])


def flush_gate(container, names):
    """Memtable empty everywhere; SSTable >= 1 for every table that holds data (empty tables allowed)."""
    st = E.tablestats(container, names)
    bad = {f'{k[0]}.{k[1]}': v for k, v in st.items()
           if v['memtable_cell_count'] != 0 or (v['sstable_count'] < 1 and v['number_of_partitions'] > 0)}
    empty = sorted(f'{k[0]}.{k[1]}' for k, v in st.items() if v['sstable_count'] == 0 and v['number_of_partitions'] == 0)
    return {'passed': not bad and bool(st), 'tables': len(st), 'violations': bad, 'empty_tables': empty}


def cold_cycle(arm, names, other=None):
    """flush -> (stop the other arm) -> cold restart -> gate. Returns (restart_s, gate)."""
    a = ARMS[arm]
    if other:
        stop(ARMS[other]['container'])
    if is_running(a['container']):
        for ks in names:
            E.nodetool(a['container'], 'flush', ks)
        rest = E.restart(a['container'], a['port'])
    else:
        t0 = time.time()
        E.sh(['docker', 'start', a['container']])
        E.wait_ready(a['container'], a['port'])
        rest = time.time() - t0
    return rest, flush_gate(a['container'], names)


class HistoryGuard:
    def __init__(self, port, keyspaces):
        self.port, self.keyspaces = port, set(keyspaces)

    def _hist(self):
        from cassandra.cluster import Cluster
        c = Cluster(['127.0.0.1'], port=self.port); s = c.connect()
        n = Counter(r.keyspace_name for r in s.execute('SELECT keyspace_name FROM system.compaction_history'))
        c.shutdown()
        return n

    def __enter__(self):
        self.before, self.t0 = self._hist(), time.time()
        return self

    def __exit__(self, *exc):
        self.after = self._hist()
        return False

    def report(self):
        new = {k: self.after[k] - self.before.get(k, 0) for k in self.after if self.after[k] - self.before.get(k, 0) > 0}
        foreign = {k: v for k, v in new.items() if k not in self.keyspaces and not k.startswith('system')}
        return {'passed': not foreign, 'window_seconds': round(time.time() - self.t0, 1),
                'own_keyspace_compactions': {k: v for k, v in new.items() if k in self.keyspaces},
                'foreign_compactions': foreign,
                'rule': 'carga mista: compactacao nos keyspaces medidos e telemetria; fora deles (nao-system) invalida'}
