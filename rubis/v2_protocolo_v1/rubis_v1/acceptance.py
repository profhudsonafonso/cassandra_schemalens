"""Passo 6 - aceitacao RUBiS v1 (dois bracos): gates recalculados dos dados brutos + testes negativos."""
from __future__ import annotations

import argparse
import gzip
import json
import tempfile
from collections import Counter
from pathlib import Path

from common_paths import code_matches_prereg, ARMS, FROZEN, MEASURED, PREREG, REPS, ROOT, WORKLOADS, code_hashes, dump, dump_x, read, sha, v9_hashes
import cassandra_env as E


def gates(run):
    reg = read(PREREG); man = read(run / 'manifest.json'); deps = reg['deployments_base_order']
    g, info = {}, {}
    complete, errors = True, 0
    for arm in ARMS:
        cnt = Counter()
        for f in sorted((run / arm / 'raw').glob('*.jsonl.gz')):
            with gzip.open(f, 'rt') as fh:
                for line in fh:
                    r = json.loads(line); cnt[(r['deployment_id'], r['workload'], r['repetition'])] += 1; errors += bool(r.get('error'))
        complete &= set(cnt) == {(d, w, r) for d in deps for w in WORKLOADS for r in range(REPS)} and all(v == MEASURED for v in cnt.values())
    valid = [x for x in man['repetitions'] if x['valid']]
    g['G1_completude_dois_bracos'] = complete
    g['G2_sem_erros'] = errors == 0
    agr = man.get('agreement_with_v9_oracle', {})
    g['G3_concordancia_oraculo_V9'] = set(agr) == set(ARMS) and all(set(x) == set(deps) and all(v['status'] == 'PASS' for v in x.values()) for x in agr.values())
    g['G4_restauracao_de_estado'] = all(not d['state_restoration_changed_tables'] for x in valid for d in x['deployments'])
    g['G5_isolamento'] = all(sorted(x['rep'] for x in valid if x['arm'] == arm) == list(range(1, REPS + 1)) for arm in ARMS) and all(
        d['isolation_v9']['clean'] and d['guard_v1']['passed'] for x in valid for d in x['deployments'])
    g['G6_gate_flush_reinicio_frio'] = all(x['gate']['passed'] for x in valid)
    g['G7_ambiente'] = all(man['preflight'][arm]['cassandra_release_version'] == ARMS[arm]['version'] for arm in ARMS) and \
        man['preflight']['v41']['image'] == E.protocol()['environment']['cassandra_image'] and man['preflight']['v311']['image'] == ARMS['v311']['image']
    g['G8_congelamento'] = (code_matches_prereg(reg['code_sha256']) and reg['design_phase']['v9_frozen_and_code_sha256'] == v9_hashes()
                            and sha(FROZEN / 'cohort_source.pkl') == reg['data']['pickle_sha256'] and sha(FROZEN / 'ORACLE_REF.pkl') == reg['oracle']['sha256'])
    g['G9_dados_identicos_nos_dois_nos'] = bool(read(FROZEN / 'TRANSFER_MANIFEST.json').get('IDENTICAL'))
    g['G10_contrabalanco'] = all(x['order'] == (deps if x['rep'] % 2 == 1 else list(reversed(deps))) for x in valid) and all(
        [x['arm'] for x in valid if x['rep'] == r] == (['v41', 'v311'] if r % 2 == 1 else ['v311', 'v41']) for r in range(1, REPS + 1))
    g['G11_materializacao_confere'] = all(all(t.get('match', True) for t in (m.get('tables') or {}).values())
                                          for arm in ARMS for m in man['materialization'][arm].values() if isinstance(m, dict))
    g['G12_firewall'] = reg['created_utc'] < man['started_utc'] and reg['firewall']['selection_uses_measured_results'] is False
    info.update(errors=errors, valid_windows=len(valid), discarded_windows=len(man['repetitions']) - len(valid))
    return g, info


def negative_tests():
    res = {}
    h2 = dict(code_hashes()); h2[next(iter(h2))] = '0' * 64
    res['N1_codigo_alterado_detectado'] = h2 != read(PREREG)['code_sha256']
    v = dict(v9_hashes()); v[next(iter(v))] = '0' * 64
    res['N2_artefato_V9_alterado_detectado'] = v != read(PREREG)['design_phase']['v9_frozen_and_code_sha256']
    from schemalens_v9 import phase6
    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / 'x.jsonl.gz'
        p.write_bytes(gzip.compress(json.dumps({'workload': 'browsing', 'repetition': 0, 'logical_request_id': 'zz',
                                                'statements': [{'statement_id': 1, 'canonical_hash': 'bad'}]}).encode() + b'\n'))
        res['N3_resposta_divergente_reprovada'] = phase6.agreement([{'path': str(p)}], {('browsing', 0, 'zz', 1): 'good'})['status'] == 'FAIL'
    import env as V
    g = V.HistoryGuard(0, ['rubis_v9_h01']); g.before, g.after, g.t0 = Counter(), Counter({'rubis_full': 1}), 0
    res['N4_compactacao_fora_dos_medidos_invalida'] = g.report()['passed'] is False
    g2 = V.HistoryGuard(0, ['rubis_v9_h01']); g2.before, g2.after, g2.t0 = Counter(), Counter({'rubis_v9_h01': 2, 'system': 3}), 0
    res['N5_compactacao_propria_e_system_nao_invalidam'] = g2.report()['passed'] is True
    orig = E.tablestats
    E.tablestats = lambda n, k: {('k', 't'): {'sstable_count': 1, 'memtable_cell_count': 3, 'number_of_partitions': 1}}
    try:
        res['N6_memtable_nao_vazia_reprova'] = V.flush_gate('c', ['k'])['passed'] is False
    finally:
        E.tablestats = orig
    try:
        dump_x(PREREG, {}); res['N7_preregistro_nao_sobrescreve'] = False
    except FileExistsError:
        res['N7_preregistro_nao_sobrescreve'] = True
    return res


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--run', required=True)
    a = ap.parse_args()
    run = (ROOT / a.run) if not Path(a.run).is_absolute() else Path(a.run)
    g, info = gates(run); n = negative_tests()
    ok = all(g.values()) and all(n.values())
    dump(run / 'analysis/acceptance.json', {'gates': g, 'info': info, 'negative_tests': n, 'ACCEPTED': ok})
    for k, v in {**g, **n}.items():
        print(f'{k:<46} {"PASS" if v else "FAIL"}')
    print('ACCEPTED' if ok else 'NOT ACCEPTED', info)


if __name__ == '__main__':
    main()
