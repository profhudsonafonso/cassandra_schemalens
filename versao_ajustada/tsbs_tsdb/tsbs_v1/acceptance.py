"""Passo 5 - aceitacao TSBS: gates recalculados dos dados brutos + testes negativos."""
from __future__ import annotations

import argparse
import copy
import time
from collections import Counter
from pathlib import Path

from common_paths import PREREG, ROOT, code_hashes, dump, dump_x, frozen_hashes, read, read_csv, sha
import cassandra_env as E


def gates(run):
    proto = E.protocol(); reg = read(PREREG); man = read(run / 'manifest.json')
    raw = read_csv(run / 'raw_latencies.csv'); labels = [v['label'] for v in reg['variants']]
    nrep = proto['repetitions']['repetitions']; nr = reg['rounds']
    classes = {r['query_id'] for r in raw}
    cnt = Counter((int(r['rep']), r['variant'], r['query_id']) for r in raw)
    g = {}
    g['G1_completude'] = len(classes) == 32 and set(cnt) == {(rep, v, q) for rep in range(1, nrep + 1) for v in labels for q in classes} and all(c == nr for c in cnt.values())
    g['G2_oraculo_independente_100pct'] = all(r['oracle_ok'] == 'True' for r in raw)
    acc = [x for x in man['repetitions'] if x['isolation_guard']['passed']]
    g['G3_isolamento'] = sorted(x['rep'] for x in acc) == list(range(1, nrep + 1)) and all(
        x['isolation_guard']['compaction_history_new_rows'] == 0 and not x['isolation_guard']['active_compaction_samples'] for x in acc)
    g['G4_gate_flush'] = all(x['flush_gate']['passed'] for x in acc)
    pf = man['preflight']
    g['G5_ambiente'] = (pf['cassandra_release_version'] == proto['environment']['cassandra_release_version']
                        and pf['image'] == proto['environment']['cassandra_image'] and pf['cpuset'] == proto['environment']['container_cpuset'])
    g['G6_congelamento'] = (reg['design_phase']['frozen_sha256'] == frozen_hashes() and reg['code_sha256'] == code_hashes()
                            and sha(ROOT / reg['data']['path']) == reg['data']['sha256'] and sha(ROOT / reg['instances']['path']) == reg['instances']['sha256']
                            and sha(ROOT / reg['data']['raw_generator_file']) == reg['data']['raw_generator_sha256'])
    an = read(run / 'analysis/analysis.json')
    g['G7_piso_de_ruido'] = all(an['noise_floor'][c]['noise_floor_abs_ln'] is not None for c in ('natural', 'aligned'))
    g['G8_firewall'] = reg['created_utc'] < pf['utc'] and reg['firewall']['selection_uses_measured_results'] is False
    pos = Counter((r['variant'], r['position']) for r in raw)
    ratios = [max(pos[(v, str(p))] for p in range(1, len(labels) + 1)) / max(1, min(pos[(v, str(p))] for p in range(1, len(labels) + 1))) for v in labels]
    g['G9_contrabalanco'] = max(ratios) <= 1.6
    g['G10_todas_instancias_cobertas'] = len({r['instance_id'] for r in raw}) == 96
    return g, {'discarded_windows': len(man['repetitions']) - len(acc), 'position_max_min_ratio': max(ratios), 'raw_rows': len(raw)}


def negative_tests(run):
    from oracle import Oracle
    import freeze
    res = {}
    ds = freeze.dataset(); o = Oracle(ds['events'])
    inst = read(ROOT / read(PREREG)['instances']['path'])
    from measure import compare
    q = next(i for i in inst if i['query_id'] == 'Q12' and i['cohort'] == 'natural')
    e = o.answer(q['query_id'], q['params'])
    bad = copy.deepcopy(e); bad[0]['avg_usage_user'] += 1e-3
    res['N1_oraculo_detecta_valor_errado'] = compare(e, e) and not compare(e, bad)
    res['N2_oraculo_detecta_linha_faltante'] = not compare(e, e[:-1])
    q16 = next(i for i in inst if i['query_id'] == 'Q16')
    e16 = o.answer('Q16', q16['params'])
    res['N3_oraculo_detecta_ordem_trocada'] = len(e16) > 1 and not compare(e16, list(reversed(e16)))
    h2 = dict(code_hashes()); h2[next(iter(h2))] = '0' * 64
    res['N4_codigo_alterado_detectado'] = h2 != read(PREREG)['code_sha256']
    from measure import ks_name
    try:
        ks_name('abcdef', 'system_auth'); res['N5_keyspace_inseguro_recusado'] = False
    except ValueError:
        res['N5_keyspace_inseguro_recusado'] = True
    raw = read_csv(run / 'raw_latencies.csv')
    cnt = Counter((r['rep'], r['variant'], r['query_id']) for r in raw[1:])
    res['N6_amostra_faltante_detectada'] = min(cnt.values()) < max(cnt.values())
    gd = E.IsolationGuard(0, ['x']); gd.samples = [{'t': 1, 'active_tasks': ['x.t:compaction']}]; gd.hist_before = gd.hist_after = 0; gd.t0 = time.time()
    res['N7_compactacao_invalida_janela'] = gd.report()['passed'] is False
    orig = E.tablestats
    E.tablestats = lambda n, k: {('k', 't'): {'sstable_count': 1, 'memtable_cell_count': 9}}
    try:
        res['N8_memtable_nao_vazia_reprova'] = E.flush_gate('c', ['k'])['passed'] is False
    finally:
        E.tablestats = orig
    try:
        dump_x(PREREG, {}); res['N9_preregistro_nao_sobrescreve'] = False
    except FileExistsError:
        res['N9_preregistro_nao_sobrescreve'] = True
    try:
        E.start_container(read(PREREG)['container'], 1); res['N10_container_existente_recusado'] = False
    except FileExistsError:
        res['N10_container_existente_recusado'] = True
    return res


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    g, info = gates(run); n = negative_tests(run)
    ok = all(g.values()) and all(n.values())
    dump(run / 'analysis/acceptance.json', {'gates': g, 'info': info, 'negative_tests': n, 'ACCEPTED': ok})
    for k, v in {**g, **n}.items():
        print(f'{k:<40} {"PASS" if v else "FAIL"}')
    print('ACCEPTED' if ok else 'NOT ACCEPTED', info)


if __name__ == '__main__':
    main()
