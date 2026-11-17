"""Passo 1 - congela a fase de projeto do TPCx-IoT e pre-registra a medicao v1 (sem Cassandra)."""
from __future__ import annotations

import datetime as dt
import hashlib

from common_paths import (AA_SUFFIX, BASELINE_ID, CONTAINER, ORIGINAL, PORT, PREREG, SNAP, WORKLOAD, code_hashes,
                          dump_x, frozen_hashes, read, sha)


def check_snapshot():
    snap = frozen_hashes(SNAP)
    compared = ORIGINAL.exists()
    if compared:
        orig = frozen_hashes(ORIGINAL)
        if snap != orig:
            diff = sorted(set(snap) ^ set(orig) | {k for k in snap if snap.get(k) != orig.get(k)})
            raise ValueError('Snapshot differs from repository TPCx-IoT: ' + ', '.join(diff[:10]))
    from methodology.tpcx_iot.stage9.experiment import load_frozen
    hb, _, _, inputs = load_frozen(SNAP)          # frozen code's own integrity checks
    return snap, compared, hb, inputs


def variants(hb):
    complete = read(SNAP / 'outputs/tpcx_iot_stage8/52_complete_compatible_deployments.json')
    pareto = read(SNAP / 'outputs/tpcx_iot_stage8/53_pareto_preselection.json')
    order = {h['deployment_id']: h['hbench_selection_order'] for h in hb}
    out = []
    for h in sorted(complete, key=lambda x: (x['normalized_proxy_score'], x['deployment_id'])):
        did = h['deployment_id']
        out.append({'label': did, 'deployment_id': did, 'kind': 'cover', 'tables': h['tables'],
                    'families': h['families'], 'read_table': h['route']['Q_READ_2_WINDOWS']['candidate_id'],
                    'in_hbench': did in order, 'hbench_rank': order.get(did),
                    'pareto_analytic': did in pareto['pareto_deployment_ids'],
                    'normalized_proxy_score': h['normalized_proxy_score'], 'objectives': h['objectives']})
    rank1 = hb[0]['deployment_id']
    out.append({'label': BASELINE_ID, 'deployment_id': BASELINE_ID, 'kind': 'baseline',
                'tables': [BASELINE_ID], 'description': 'TPCx-IoT logical-record baseline portado ao Cassandra (pesquisa)'})
    out.append({'label': rank1 + AA_SUFFIX, 'deployment_id': rank1, 'kind': 'aa_replica', 'replica_of': rank1,
                'tables': hb[0]['tables']})
    out.append({'label': BASELINE_ID + AA_SUFFIX, 'deployment_id': BASELINE_ID, 'kind': 'aa_replica',
                'replica_of': BASELINE_ID, 'tables': [BASELINE_ID]})
    return out, complete


def main():
    if PREREG.exists():
        raise FileExistsError('Already pre-registered: ' + str(PREREG))
    snap, compared, hb, inputs = check_snapshot()
    vs, complete = variants(hb)
    rank1 = hb[0]['deployment_id']
    tag = hashlib.sha256((inputs['outputs/tpcx_iot_stage8/54_hbench_selected.json'] + '|v1|run1').encode()).hexdigest()[:6]
    reg = {
        'status': 'PREREGISTERED_BEFORE_MEASUREMENT',
        'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'benchmark': 'TPCx-IoT v2.1.2 (adaptacao de pesquisa; nao oficial) - Stages 1-8 congelados',
        'design_phase': {'snapshot_equals_repository_bytes': compared, 'frozen_files': len(snap),
                         'frozen_sha256': snap, 'stage8_inputs_sha256': inputs, 'design_phase_rerun': False},
        'code_sha256': code_hashes(),
        'protocol_file_sha256': code_hashes()['common/protocol_v1.json'],
        'workload': WORKLOAD,
        'variants': vs, 'admissible_space_size': len(complete),
        'hbench_ids_ranked': [h['deployment_id'] for h in hb],
        'aa_pairs': [[rank1, rank1 + AA_SUFFIX], [BASELINE_ID, BASELINE_ID + AA_SUFFIX]],
        'aa_note': 'Nenhum par natural de trabalho identico no espaco TPCx (projecoes diferentes na ingestao); '
                   'replicas explicitas do rank-1 e do baseline em keyspaces distintos',
        'run_tag': tag, 'container': CONTAINER, 'port': PORT, 'measurement_seed_base': 20261117,
        'decision_metric': 'media da latencia de leitura Q_READ_2_WINDOWS sob ingestao concorrente',
        'secondary_metrics': ['p50/p95/p99 de leitura', 'eventos/s de ingestao', 'bytes live apos flush'],
        'hypotheses': {
            'H1': 'O HBench (5 de 9) contem um deployment empatado (piso de ruido) com o melhor medido em media de leitura',
            'H2': 'Deployments que leem de uma projecao TS3 (3 requisicoes CQL) tem media de leitura menor que os que leem de TS0/TS1 (20-30 requisicoes)',
            'H3': 'Deployments de uma tabela (1 insert/evento) ingerem mais eventos/s que os de duas tabelas (2 inserts/evento)',
            'H4': 'O HBench retem 100% da fronteira de Pareto medida (leitura, bytes, tempo de ingestao)',
            'H5': 'Requisicoes CQL por leitura observadas coincidem com read_cql_requests do Stage 7/8 em todos os deployments'},
        'firewall': {'selection_uses_measured_results': False, 'hbench_selection_modified': False},
    }
    dump_x(PREREG, reg)
    print('PRE-REGISTERED', PREREG, 'variants', len(vs), 'HBench', reg['hbench_ids_ranked'])


if __name__ == '__main__':
    main()
