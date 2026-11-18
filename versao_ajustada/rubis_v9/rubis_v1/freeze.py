"""Passo 2 - pre-registro do RUBiS v1 (depois da transferencia verificada, antes de medir)."""
from __future__ import annotations

import datetime as dt

from common_paths import (AA_ID, ARMS, FROZEN, MEASURED, PREREG, REPS, V9_RUN, WARMUP, WORKLOADS, code_hashes,
                          dump_x, read, sha, v9_hashes)


def base_order():
    """V9 interleaving (schemalens_v9.phase6.order) + the A/A replica at the end."""
    H = read(V9_RUN / 'stages/E15_composition/H.json')['H']
    ids = [h['schema_id'] for h in H]
    ext = ['NoSE', 'V8_AUTO_H_norm', 'QPG']
    seq = []
    for i, h in enumerate(ids):
        seq.append(h)
        if i < len(ext):
            seq.append(ext[i])
    return seq + [AA_ID], {h['schema_id']: h['predicted_cost'] for h in H}


def main():
    if PREREG.exists():
        raise FileExistsError('ja pre-registrado')
    tm = read(FROZEN / 'TRANSFER_MANIFEST.json')
    if not tm or not tm.get('IDENTICAL') or set(tm.get('arms', {})) != set(ARMS):
        raise RuntimeError('carregue os dados nos dois nos (import --arm v41 e --arm v311) com IDENTICAL=true antes do pre-registro')
    om = read(FROZEN / 'ORACLE_REF_MANIFEST.json')
    if not om or sha(FROZEN / 'ORACLE_REF.pkl') != om['sha256']:
        raise RuntimeError('rode oracle_ref.py antes do pre-registro')
    order, pred = base_order()
    v9e20 = read(V9_RUN / 'stages/E21_analysis/RESULT.json', {})
    reg = {
        'status': 'PREREGISTERED_BEFORE_MEASUREMENT', 'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'benchmark': 'RUBiS (SchemaLens V9, run v9-generation-20261112-001) - E20 refeito neste servidor, dados regenerados, Cassandra 4.1.12 e 3.11.11',
        'design_phase': {'v9_frozen_and_code_sha256': v9_hashes(), 'hbench_frozen_sha256': read(V9_RUN / 'stages/E16_hbench_frozen/HBENCH_FROZEN.json')['content_sha256'],
                         'design_phase_rerun': False},
        'data': {'origin': tm.get('origin'), 'manifest_sha256': sha(FROZEN / 'TRANSFER_MANIFEST.json'), 'pickle_sha256': tm['pickle_sha256'],
                 'cohort_counts': tm.get('cohort_counts'), 'v9_cohort_counts_for_reference': tm.get('v9_cohort_counts_for_reference'),
                 'identical_on_both_nodes': True},
        'oracle': {'file': 'frozen/ORACLE_REF.pkl', 'sha256': om['sha256'], 'entries': om['entries'], 'source': om['oracle']},
        'arms': {k: {'container': v['container'], 'port': v['port'], 'version': v['version'],
                     'image': v['image'] or 'protocol_v1 (cassandra@sha256:25f6a4d5...)'} for k, v in ARMS.items()},
        'arm_order_rule': 'ABBA: repeticoes impares v41 -> v311, pares v311 -> v41; o outro no fica parado',
        'code_sha256': code_hashes(), 'protocol_file_sha256': code_hashes()['common/protocol_v1.json'],
        'deployments_base_order': order, 'order_rule': 'repeticoes impares: ordem base; pares: ordem invertida (contrabalanco)',
        'request_protocol': {'workloads': list(WORKLOADS), 'repetitions': REPS, 'warmup': WARMUP, 'measured': MEASURED,
                             'histories': 'reused.methodology.workload_005.histories (sementes por repeticao, identicas a V9)',
                             'state_reset': 'adapter.reset_state antes e depois de cada (deployment, carga, repeticao)'},
        'v1_additions': ['container dedicado 4.1.12 por digest', 'flush + reinicio a frio + gate antes de cada repeticao',
                         'ordem intercalada por repeticao e contrabalancada', 'replica A/A H01_AA (piso de ruido)',
                         'p50/p95/p99 + IC bootstrap estratificado + IC t', 'armazenamento por tablestats',
                         'guarda de isolamento para carga mista + BulkWriterWatch da V9'],
        'aa_pairs': [['H01', AA_ID]], 'identical_work_pairs_v9': [['H01', 'H02', 'browsing']],
        'predicted_cost': pred,
        'decision_metric': 'latencia media por requisicao em cada carga (metrica da V9 e do protocolo v1)',
        'hypotheses': {
            'H1': 'Em 4.1.12, H01 e mais rapido que NoSE e QPG em browsing, bidding e update10x (replica a conclusao da V9 em dados regenerados)',
            'H2': 'A versao nao muda as conclusoes: em cada carga, tau >= 0,7 entre as medias de 3.11.11 e 4.1.12 dos 9 deployments, e os vereditos de H01 contra NoSE/QPG sao os mesmos nas duas versoes',
            'H3': 'Dentro de H (4.1.12), a ordem prevista pelo modelo de custo concorda com a medida (tau >= 0,6 em cada carga)',
            'H4': 'O Top-1 previsto dentro de H e o melhor medido (ou empata pelo piso de ruido) em pelo menos 3 das 4 cargas (4.1.12)',
            'H5': 'Todas as respostas de todos os deployments, nas duas versoes, concordam com o oraculo independente da V9'},
        'firewall': {'selection_uses_measured_results': False, 'hbench_selection_modified': False},
        'v9_e21_status': v9e20.get('status'),
    }
    dump_x(PREREG, reg)
    print('PRE-REGISTERED', PREREG, order)


if __name__ == '__main__':
    main()
