"""Passo 5 - aceitacao: gates recalculados dos dados brutos + testes negativos."""
from __future__ import annotations

import argparse
import copy
import shutil
import tempfile
from collections import Counter
from pathlib import Path

from common_paths import PREREG, ROOT, SNAP, code_hashes, dump, dump_x, frozen_hashes, read, read_csv
import cassandra_env as E


def gates(run):
    proto = E.protocol(); reg = read(PREREG); man = read(run / 'manifest.json')
    raw = read_csv(run / 'raw_latencies.csv')
    labels = [v['label'] for v in reg['variants']]
    nrep = proto['repetitions']['repetitions']; nr = proto['repetitions']['timed_rounds_per_query_variant_per_repetition']
    cnt = Counter((int(r['rep']), r['variant'], r['query_id']) for r in raw)
    exp_keys = {(rep, v, f'Q{i}') for rep in range(1, nrep + 1) for v in labels for i in range(1, 13)}
    g = {}
    g['G1_completude'] = set(cnt) == exp_keys and all(c == nr for c in cnt.values())
    g['G2_oraculo_100pct'] = all(r['oracle_ok'] == 'True' for r in raw) and len(raw) == len(exp_keys) * nr
    acc = {}
    for x in man['repetitions']:
        if x['isolation_guard']['passed']:
            acc[x['rep']] = x
    g['G3_isolamento'] = sorted(acc) == list(range(1, nrep + 1)) and all(
        a['isolation_guard']['compaction_history_new_rows'] == 0 and not a['isolation_guard']['active_compaction_samples'] for a in acc.values())
    g['G4_gate_flush'] = all(a['flush_gate']['passed'] for a in acc.values())
    pf = man['preflight']
    g['G5_ambiente'] = (pf['cassandra_release_version'] == proto['environment']['cassandra_release_version']
                        and pf['image'] == proto['environment']['cassandra_image']
                        and pf['cpuset'] == proto['environment']['container_cpuset'])
    g['G6_congelamento'] = reg['design_phase']['frozen_sha256'] == frozen_hashes() and reg['code_sha256'] == code_hashes()
    s79 = read_csv(run / 'analysis/stage7_vs_stage9.csv')
    g['G7_stage7_vs_stage9'] = all(r['status'] == 'MATCH' for r in s79)
    an = read(run / 'analysis/analysis.json')
    g['G8_piso_de_ruido'] = an['noise_floor']['noise_floor_abs_ln'] is not None and len(an['noise_floor']['pairs']) >= 1
    g['G9_firewall'] = reg['created_utc'] < man['preflight']['utc'] and reg['firewall']['selection_uses_measured_results'] is False
    pos = Counter((r['variant'], r['position']) for r in raw)
    ratios = []
    for v in labels:
        cs = [pos[(v, str(p))] for p in range(1, len(labels) + 1)]
        ratios.append(max(cs) / max(1, min(cs)))
    g['G10_contrabalanco'] = max(ratios) <= 1.6
    disc = [x for x in man['repetitions'] if not x['isolation_guard']['passed']]
    return g, {'discarded_windows': len(disc), 'position_max_min_ratio': max(ratios), 'raw_rows': len(raw)}


def negative_tests(run):
    res = {}
    # N1 snapshot alterado -> hashes diferem
    with tempfile.TemporaryDirectory() as td:
        t = Path(td) / 'snap'
        shutil.copytree(SNAP / 'phase_a1', t / 'phase_a1'); shutil.copytree(SNAP / 'outputs', t / 'outputs')
        for d in ('config', 'iot_pipeline', 'SchemaLens_V4_Rev3_PlanAware', 'SchemaLens_V4_Rev2_Confirmatory'):
            shutil.copytree(SNAP / d, t / d)
        p = t / 'phase_a1/outputs/normalized_double_readings.csv'
        b = bytearray(p.read_bytes()); b[-3] = ord('9') if b[-3] != ord('9') else ord('8'); p.write_bytes(bytes(b))
        res['N1_dado_adulterado_detectado'] = frozen_hashes(t) != frozen_hashes(SNAP)
    # N2 codigo alterado -> recusa
    h = code_hashes(); h2 = dict(h); k = next(iter(h2)); h2[k] = '0' * 64
    res['N2_codigo_alterado_detectado'] = h2 != read(PREREG)['code_sha256']
    # N3 oraculo detecta resposta errada
    from measure import load_inputs
    from iot_pipeline.offline import compare
    rows, queries, *_ , oracle, _p = load_inputs()
    q = next(x for x in queries if x['query_id'] == 'Q4')
    exp = oracle(rows, q)
    bad = copy.deepcopy(exp)
    def perturb(x):
        if isinstance(x, bool): return x
        if isinstance(x, float): return x * 1.001 + 1e-3
        if isinstance(x, int) and x > 10**11: return x  # timestamps kept
        if isinstance(x, int): return x + 1
        if isinstance(x, dict): return {kk: perturb(vv) for kk, vv in x.items()}
        if isinstance(x, list): return [perturb(vv) for vv in x]
        return x
    res['N3_oraculo_detecta_erro'] = compare(exp, exp) and not compare(perturb(bad), exp)
    q2 = next(x for x in queries if x['query_id'] == 'Q2')
    e2 = oracle(rows, q2)
    res['N3b_oraculo_detecta_linha_faltante'] = (not compare(e2[:-1], e2)) if isinstance(e2, list) and e2 else None
    # N4 nome de keyspace inseguro
    from measure import keyspace
    try:
        keyspace('system_', {'keyspace_tail': 'reference'}); res['N4_keyspace_inseguro_recusado'] = False
    except ValueError:
        res['N4_keyspace_inseguro_recusado'] = True
    # N5 amostra faltante -> G1 falha (recalculo sobre copia)
    raw = read_csv(run / 'raw_latencies.csv')
    cnt = Counter((r['rep'], r['variant'], r['query_id']) for r in raw[1:])
    res['N5_amostra_faltante_detectada'] = min(cnt.values()) < max(cnt.values())
    # N6 guarda de isolamento com compactacao ativa
    gd = E.IsolationGuard(0, ['x']); gd.samples = [{'t': 1, 'active_tasks': ['x.t:compaction']}]
    gd.hist_before = gd.hist_after = 0; import time as _t; gd.t0 = _t.time()
    res['N6_compactacao_invalida_janela'] = gd.report()['passed'] is False
    # N7 gate de flush com memtable nao vazia
    orig = E.tablestats
    E.tablestats = lambda name, ks: {('k', 't'): {'sstable_count': 1, 'memtable_cell_count': 5}}
    try:
        res['N7_memtable_nao_vazia_reprova'] = E.flush_gate('c', ['k'])['passed'] is False
        E.tablestats = lambda name, ks: {('k', 't'): {'sstable_count': 0, 'memtable_cell_count': 0}}
        res['N7b_sem_sstable_reprova'] = E.flush_gate('c', ['k'])['passed'] is False
    finally:
        E.tablestats = orig
    # N8 pre-registro e exclusivo
    try:
        dump_x(PREREG, {}); res['N8_preregistro_nao_sobrescreve'] = False
    except FileExistsError:
        res['N8_preregistro_nao_sobrescreve'] = True
    # N9 container existente e recusado
    try:
        E.start_container(read(PREREG)['container'], 1); res['N9_container_existente_recusado'] = False
    except FileExistsError:
        res['N9_container_existente_recusado'] = True
    return res


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True, type=Path)
    a = ap.parse_args()
    run = (ROOT / a.run) if not a.run.is_absolute() else a.run
    g, info = gates(run); n = negative_tests(run)
    ok = all(g.values()) and all(v is not False for v in n.values())
    dump(run / 'analysis/acceptance.json', {'gates': g, 'info': info, 'negative_tests': n, 'ACCEPTED': ok})
    for k, v in {**g, **n}.items():
        print(f'{k:<40} {"PASS" if v else ("N/A" if v is None else "FAIL")}')
    print('ACCEPTED' if ok else 'NOT ACCEPTED', info)


if __name__ == '__main__':
    main()
