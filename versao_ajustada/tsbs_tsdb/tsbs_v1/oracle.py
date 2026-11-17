"""Oraculo INDEPENDENTE do TSBS cpu-only (protocolo v1).

Implementa a semantica dos 16 templates do TSBS (cmd/tsbs_generate_queries, devops) diretamente
sobre os eventos-fonte em memoria. NAO importa nada de methodology/tsbs/stage5 (planner/runtime),
stage6 (materializer) nem do executor: e um caminho de codigo separado do que esta sendo medido,
corrigindo a divergencia 'oraculo nao independente' (importante 7 da analise de consistencia).
Formato de saida = formato logico das respostas do executor (bucket_ns, max_*/avg_*, ...).
"""
from __future__ import annotations

from collections import defaultdict

METRICS = ("usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait",
           "usage_irq", "usage_softirq", "usage_steal", "usage_guest", "usage_guest_nice")
NS_MIN = 60_000_000_000
NS_HOUR = 3_600_000_000_000
# query_id -> (kind, metric_count)
SPEC = {'Q01': ('single', 1), 'Q02': ('single', 1), 'Q03': ('single', 1), 'Q04': ('single', 5), 'Q05': ('single', 5),
        'Q06': ('single', 5), 'Q07': ('maxall', 10), 'Q08': ('maxall', 10), 'Q09': ('maxall', 10),
        'Q10': ('double', 1), 'Q11': ('double', 5), 'Q12': ('double', 10), 'Q13': ('high', 10), 'Q14': ('high', 10),
        'Q15': ('lastpoint', 10), 'Q16': ('toprollup', 1)}


class Oracle:
    def __init__(self, events):
        self.by_host = defaultdict(list)
        for e in events:
            self.by_host[e['hostname']].append(e)
        for v in self.by_host.values():
            v.sort(key=lambda e: e['timestamp_ns'])

    def _rows(self, hosts, lo=None, hi=None):
        for h in hosts:
            for e in self.by_host.get(h, ()):
                t = e['timestamp_ns']
                if (lo is None or t >= lo) and (hi is None or t < hi):
                    yield e

    def answer(self, query_id, params):
        kind, mc = SPEC[query_id]
        hosts = params['hostnames']
        ms = METRICS[:mc]
        if kind in ('single', 'maxall'):
            res = NS_MIN if kind == 'single' else NS_HOUR
            agg = {}
            for e in self._rows(hosts, params['start_ns'], params['end_ns']):
                b = (e['timestamp_ns'] // res) * res
                cur = agg.setdefault(b, {m: e[m] for m in ms})
                for m in ms:
                    if e[m] > cur[m]:
                        cur[m] = e[m]
            return [{'bucket_ns': b, **{f'max_{m}': v[m] for m in ms}} for b, v in sorted(agg.items())]
        if kind == 'double':
            s = defaultdict(lambda: [0.0] * mc); n = defaultdict(int)
            for e in self._rows(hosts, params['start_ns'], params['end_ns']):
                k = ((e['timestamp_ns'] // NS_HOUR) * NS_HOUR, e['hostname'])
                acc = s[k]
                for i, m in enumerate(ms):
                    acc[i] += e[m]
                n[k] += 1
            return [{'bucket_ns': b, 'hostname': h, **{f'avg_{m}': s[(b, h)][i] / n[(b, h)] for i, m in enumerate(ms)}}
                    for (b, h) in sorted(s)]
        if kind == 'high':
            out = [e for e in self._rows(hosts, params['start_ns'], params['end_ns']) if e['usage_user'] > 90.0]
            out.sort(key=lambda e: (e['timestamp_ns'], e['hostname']))
            return [{'hostname': e['hostname'], 'timestamp_ns': e['timestamp_ns'], **{m: e[m] for m in METRICS}} for e in out]
        if kind == 'lastpoint':
            out = []
            for h in sorted(hosts):
                rows = self.by_host.get(h)
                if rows:
                    e = rows[-1]
                    out.append({'hostname': h, 'timestamp_ns': e['timestamp_ns'], **{m: e[m] for m in METRICS}})
            return out
        if kind == 'toprollup':
            agg = {}
            for e in self._rows(hosts, None, params['end_ns']):
                b = (e['timestamp_ns'] // NS_MIN) * NS_MIN
                if b not in agg or e['usage_user'] > agg[b]:
                    agg[b] = e['usage_user']
            top = sorted(agg.items(), reverse=True)[:5]
            return [{'bucket_ns': b, 'max_usage_user': v} for b, v in top]
        raise ValueError(query_id)
