"""Passo 1 - congela a fase de projeto e pre-registra a medicao v1 (sem Cassandra).

* confirma que o snapshot frozen_design_B e byte a byte igual a campanha B original;
* confirma o codigo Rev3 auditado e os hashes internos do manifesto Rev3;
* define as variantes medidas: espaco admissivel completo (modo verification) + baselines;
* detecta pares A/A (trabalho identico) para o piso de ruido;
* grava PREREGISTRATION_IOT_V1.json em modo exclusivo, antes de qualquer medicao.
"""
from __future__ import annotations

import datetime as dt
import json

from common_paths import (EXPECTED_REV3_CODE, LABEL_OPT, LABEL_ORIG, ORIGINAL_B, PREREG, REV3, SNAP,
                          CONTAINER, PORT, code_hashes, dump_x, frozen_hashes, read, sha)


def check_snapshot():
    snap, orig = frozen_hashes(SNAP), frozen_hashes(ORIGINAL_B) if ORIGINAL_B.exists() else None
    if orig is not None and snap != orig:
        diff = [k for k in snap if snap[k] != orig.get(k)]
        raise ValueError('Snapshot differs from original campaign B: ' + ', '.join(diff))
    for name, h in EXPECTED_REV3_CODE.items():
        if sha(SNAP / 'SchemaLens_V4_Rev3_PlanAware/rev3' / name) != h:
            raise ValueError('Rev3 code differs from audited hash: ' + name)
    m = read(REV3 / 'stage8/manifest.json')
    for rel, h in m['artifact_sha256'].items():
        if sha(REV3 / rel) != h:
            raise ValueError('Rev3 artifact modified: ' + rel)
    if m['source_sha256']['data'] != sha(SNAP / 'phase_a1/outputs/normalized_double_readings.csv'):
        raise ValueError('data hash differs from Rev3 manifest')
    return snap, orig is not None, m


def route_signature(cover, q):
    r = cover['query_routes'][q]
    return r['physical_plan']


def variants():
    covers = read(REV3 / 'stage8/29_all_compatible_covers.json')
    hb = read(REV3 / 'stage8/28_hbench_v4_rev3_frozen.json')
    rank = {h['deployment_id']: i + 1 for i, h in enumerate(hb)}
    out = [{'label': LABEL_ORIG, 'kind': 'baseline', 'keyspace_tail': 'reference',
            'description': 'Traducao Cassandra do modelo table do IoTDB; Q8 por varredura completa'},
           {'label': LABEL_OPT, 'kind': 'baseline', 'keyspace_tail': 'reference',
            'description': 'Mesma tabela da referencia; Q8 em ordem DESC parando no ultimo nao nulo'}]
    for h in sorted(covers, key=lambda x: (x['preselection_score'], x['deployment_id'])):
        out.append({'label': h['deployment_id'], 'kind': 'cover', 'keyspace_tail': h['deployment_id'].lower(),
                    'candidates': h['candidates'], 'families': h['families'],
                    'canonical_source': h['canonical_source'],
                    'in_hbench': h['deployment_id'] in rank, 'hbench_rank': rank.get(h['deployment_id']),
                    'selection_reason': next((x['selection_reason'] for x in hb if x['deployment_id'] == h['deployment_id']), 'NOT_SELECTED'),
                    'selection_status': h.get('selection_status'),
                    'preselection_score': h['preselection_score'], 'analytic_cost': h['analytic_cost'],
                    'routes': {q: r['plan_id'] for q, r in sorted(h['query_routes'].items())}})
    return out, covers, hb


def aa_pairs(covers):
    """Pairs that execute byte-identical read work: same physical plan on a same-shaped table for all Q."""
    shape = {'ts2_iot_wide_unbucketed': 'WIDE_U', 'ts2_iot_wide': 'WIDE_D', 'ts0_iot_raw': 'NARROW', 'ts4_iot_latest': 'LATEST'}
    sig = {}
    for h in covers:
        sig[h['deployment_id']] = tuple((q, h['query_routes'][q]['physical_plan'], shape[h['query_routes'][q]['candidate_id']])
                                        for q in sorted(h['query_routes']))
    pairs = []
    ids = sorted(sig)
    for i, a in enumerate(ids):
        for b in ids[i + 1:]:
            if sig[a] == sig[b]:
                pairs.append([a, b])
    # Reference table has the same DDL as TS2-U (checked below) and, with the optimized Q8,
    # the same plans (RANGE/DESC_SPARSE on an unbucketed wide table).
    from iot_pipeline.reference_baseline import table_ddl
    specs = {s['candidate_id']: s for s in read(SNAP / 'outputs/stage4/10_candidate_specs.json')}
    norm = lambda d: ''.join(d.replace('iot_reference_table', 'T').replace('ts2_iot_wide_unbucketed', 'T').split()).upper()
    sensors = ['s_0', 's_1']
    same_ddl = norm(table_ddl(sensors)) == norm(specs['ts2_iot_wide_unbucketed']['ddl'])
    u = [h['deployment_id'] for h in covers if h['candidates'] == ['ts2_iot_wide_unbucketed']]
    if same_ddl and u and all(p == ('TS2U_DESC_SPARSE' if q == 'Q8' else 'TS2U_RANGE_SCAN') for q, p, _ in sig[u[0]]):
        pairs.append([u[0], LABEL_OPT])
    return pairs, same_ddl


def main():
    if PREREG.exists():
        raise FileExistsError('Already pre-registered: ' + str(PREREG))
    snap, compared, m = check_snapshot()
    vs, covers, hb = variants()
    pairs, same_ddl = aa_pairs(covers)
    import hashlib
    tag = hashlib.sha256((m['artifact_sha256']['stage8/28_hbench_v4_rev3_frozen.json'] + '|v1|run1').encode()).hexdigest()[:8]
    reg = {
        'status': 'PREREGISTERED_BEFORE_MEASUREMENT',
        'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'benchmark': 'IoT-Benchmark (thulab, commit 62e0dc10) - campanha B Rev3',
        'protocol_file_sha256': code_hashes()['common/protocol_v1.json'],
        'design_phase': {
            'source': 'campanha B Rev3 (DATA_SEED 531101, QUERY_SEED 531107; 9 dispositivos, 54.000 celulas)',
            'snapshot_equals_original_bytes': compared,
            'frozen_sha256': snap,
            'rev3_hbench_sha256': sha(REV3 / 'stage8/28_hbench_v4_rev3_frozen.json'),
            'design_phase_rerun': False,
            'reason': 'Analise de consistencia: fase de projeto ja consistente; so a medicao e refeita sob o protocolo v1'},
        'code_sha256': code_hashes(),
        'variants': vs,
        'admissible_space_size': len(covers), 'hbench_ids_ranked': [h['deployment_id'] for h in hb],
        'aa_pairs': pairs, 'reference_ddl_equals_ts2u': same_ddl,
        'run_tag': tag,
        'keyspace_prefix': f'sl_v1_iot_{tag}_',
        'container': CONTAINER, 'port': PORT,
        'measurement_seed_base': 20261117,
        'decision_metric': 'mean of per-class mean latency (uniform 12-class mix)',
        'hypotheses': {
            'H1': 'TS2-U (rank 1 do seletor) tem a menor media do mix ou empata, pelo piso de ruido, com o melhor medido do espaco admissivel',
            'H2': 'O HBench (3 de 6) contem um deployment a ate 5% do melhor medido (near-best 5%)',
            'H3': 'TS2-U empata com BASELINE_Q8_OPTIMIZED (mesmo DDL e mesmos planos; par A/A)',
            'H4': 'TS2-D e TS0 sao mais lentos que TS2-U na media do mix (fora do piso de ruido)',
            'H5': 'Previsoes do Stage 7 (chamadas e linhas) coincidem com o observado em todas as rotas'},
        'firewall': {'selection_uses_measured_results': False, 'hbench_selection_modified': False},
    }
    dump_x(PREREG, reg)
    print('PRE-REGISTERED', PREREG)
    print('variants:', len(vs), 'admissible covers:', len(covers), 'HBench:', reg['hbench_ids_ranked'])
    print('A/A pairs:', pairs)


if __name__ == '__main__':
    main()
