"""Passo 5 - aceitacao TPCx-IoT: gates recalculados dos dados brutos + testes negativos."""
from __future__ import annotations

import argparse
import shutil
import tempfile
import time
from collections import Counter
from pathlib import Path

from common_paths import PREREG, ROOT, SNAP, WORKLOAD, code_hashes, dump, dump_x, frozen_hashes, read, read_csv
import cassandra_env as E


def gates(run):
    proto = E.protocol(); reg = read(PREREG); man = read(run / 'manifest.json')
    raw = read_csv(run / 'raw_reads.csv'); labels = [v['label'] for v in reg['variants']]
    nrep = proto['repetitions']['repetitions']; W = WORKLOAD
    acc = [r for r in man['runs'] if r['isolation_guard']['passed']]
    cnt = Counter((int(r['rep']), r['variant']) for r in raw)
    g = {}
    g['G1_completude'] = (set(cnt) == {(rep, v) for rep in range(1, nrep + 1) for v in labels}
                          and all(c == W['read_operations'] for c in cnt.values())
                          and sorted((r['rep'], r['variant']) for r in acc) == sorted((rep, v) for rep in range(1, nrep + 1) for v in labels))
    g['G2_oraculo_e_auditoria'] = all(r['oracle_ok'] == 'True' for r in raw) and all(r['audit_status'] == 'PASS_ALL_RECORDS_ALL_PHYSICAL_TABLES' for r in acc)
    g['G3_isolamento'] = all(r['isolation_guard']['compaction_history_new_rows'] == 0 and not r['isolation_guard']['active_compaction_samples'] for r in acc)
    g['G4_gate_flush'] = all(r['flush_gate'] for r in acc)
    pf = man['preflight']
    g['G5_ambiente'] = (pf['cassandra_release_version'] == proto['environment']['cassandra_release_version']
                        and pf['image'] == proto['environment']['cassandra_image'] and pf['cpuset'] == proto['environment']['container_cpuset'])
    g['G6_congelamento'] = reg['design_phase']['frozen_sha256'] == frozen_hashes() and reg['code_sha256'] == code_hashes()
    s79 = read_csv(run / 'analysis/stage7_vs_stage9.csv')
    g['G7_inserts_por_evento_stage7'] = all(r['status_inserts'] == 'MATCH' for r in s79)
    an = read(run / 'analysis/analysis.json')
    g['G8_piso_de_ruido'] = an['noise_floor']['noise_floor_abs_ln'] is not None
    g['G9_firewall'] = reg['created_utc'] < pf['utc'] and reg['firewall']['selection_uses_measured_results'] is False
    pos = Counter((r['variant'], r['order_position']) for r in acc)
    meanpos = {v: sum(p for (vv, p), c in pos.items() if vv == v for _ in range(c)) / nrep for v in labels}
    g['G10_contrabalanco'] = max(meanpos.values()) - min(meanpos.values()) <= len(labels) / 2
    g['G11_agenda_identica'] = len({r['schedule_sha256'] for r in acc}) == 1
    g['G12_leituras_concorrentes'] = all(r['overlap_reads'] >= 1 for r in acc)
    return g, {'discarded_attempts': len(man['runs']) - len(acc), 'mean_position_spread': max(meanpos.values()) - min(meanpos.values()),
               'raw_reads': len(raw), 'runs': len(acc)}


def negative_tests(run):
    res = {}
    with tempfile.TemporaryDirectory() as td:
        t = Path(td) / 's'
        shutil.copytree(SNAP, t, ignore=shutil.ignore_patterns('__pycache__', 'stage91*', 'tpcx_iot_stage9'))
        p = t / 'outputs/tpcx_iot_stage8/54_hbench_selected.json'
        p.write_text(p.read_text().replace('"hbench_selection_order": 1', '"hbench_selection_order": 2', 1))
        res['N1_hbench_adulterado_detectado'] = frozen_hashes(t) != frozen_hashes(SNAP)
        import subprocess, sys
        r = subprocess.run([sys.executable, '-c', 'from pathlib import Path\nfrom methodology.tpcx_iot.stage9.experiment import load_frozen\nload_frozen(Path("."))'],
                           cwd=t, env={'PYTHONPATH': str(t), 'PATH': '/usr/bin:/bin'}, capture_output=True, text=True)
        res['N1b_codigo_congelado_recusa_hbench_adulterado'] = r.returncode != 0 and 'ValueError' in r.stderr
    h2 = dict(code_hashes()); h2[next(iter(h2))] = '0' * 64
    res['N2_codigo_alterado_detectado'] = h2 != read(PREREG)['code_sha256']
    from methodology.tpcx_iot.stage91.experiment import expected_rows_from_watermark, make_probe
    pr = make_probe(200, 3, sensors=3)
    e = expected_rows_from_watermark(pr, stations=2)
    bad = [dict(x) for x in e]; bad[0]['sensor_reading'] += 0.1
    res['N3_oraculo_detecta_valor_errado'] = bad != e
    res['N3b_oraculo_detecta_linha_faltante'] = e[:-1] != e and len(e) > 0
    from measure import ks_name
    try:
        ks_name('zzzzzz', 1, 'system; DROP', 1); res['N4_keyspace_inseguro_recusado'] = False
    except ValueError:
        res['N4_keyspace_inseguro_recusado'] = True
    raw = read_csv(run / 'raw_reads.csv')
    cnt = Counter((r['rep'], r['variant']) for r in raw[1:])
    res['N5_leitura_faltante_detectada'] = min(cnt.values()) < WORKLOAD['read_operations']
    gd = E.IsolationGuard(0, ['x']); gd.samples = [{'t': 1, 'active_tasks': ['x.t:compaction']}]; gd.hist_before = gd.hist_after = 0; gd.t0 = time.time()
    res['N6_compactacao_invalida_janela'] = gd.report()['passed'] is False
    orig = E.tablestats
    E.tablestats = lambda n, k: {('k', 't'): {'sstable_count': 1, 'memtable_cell_count': 3}}
    try:
        res['N7_memtable_nao_vazia_reprova'] = E.flush_gate('c', ['k'])['passed'] is False
    finally:
        E.tablestats = orig
    try:
        dump_x(PREREG, {}); res['N8_preregistro_nao_sobrescreve'] = False
    except FileExistsError:
        res['N8_preregistro_nao_sobrescreve'] = True
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
        print(f'{k:<44} {"PASS" if v else "FAIL"}')
    print('ACCEPTED' if ok else 'NOT ACCEPTED', info)


if __name__ == '__main__':
    main()
