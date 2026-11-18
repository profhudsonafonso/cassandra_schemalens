"""Passo 1 - fase de projeto congelada + dados do gerador oficial + instancias + pre-registro (sem Cassandra).

* Stages 1-8 regenerados deterministicamente a partir do codigo do repositorio (as saidas nao eram
  versionadas); os hashes de specs/plans/writes/deployments conferem com o manifesto da execucao anterior.
* Espaco admissivel completo reconstruido com _make_deployment/_pareto do proprio consolidador.
* Dados: tsbs_generate_data oficial (commit fixado), formato cassandra (ver gen_data.sh).
* Instancias: generate_instances do Stage 9 (coortes natural e aligned), gravadas antes de medir.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import itertools
from pathlib import Path

from common_paths import (AA_SUFFIX, BASELINE_ID, CONTAINER, DATA_DIR, GENERATOR, INSTANCES, INTERVAL_S, PORT, PREREG,
                          ROOT, SNAP, TSBS_COMMIT, code_hashes, dump, dump_x, frozen_hashes, read, sha)

PREVIOUS_UPSTREAM = {  # outputs/tsbs_stage9_live_run1/36_stage9_manifest.json (execucao anterior, 1/7 deployments)
    'deployments': '0fa7128806786aa73942eb5669965a8c72704f62bd54a29c19ae219d2c49413f',
    'specs': '881156d80f2a9d4ec11d625b8e038180486ad90f591c45e6c4843a1e1547abd7',
    'plans': 'd7c155e28099be5ec1a03cbc952e24f1145c05fad21561e1bb914e243054cd0f',
    'writes': '41a6d88e69cf434d21324376735acc4d2eca3e2adf1b9227de77284dd13bac87'}
RAW_FILE = DATA_DIR / 'tsbs_cpu_only_cassandra.txt.gz'          # bytes do gerador oficial (series_bigint)
DATA_FILE = DATA_DIR / 'tsbs_cpu_only_cassandra_double.txt.gz'  # adaptado (bigint -> double, sem perda)
INST_FILE = DATA_DIR / 'query_instances.json'
VERIFY_EXTRA = 'pareto_not_selected'   # verification extension (see build_registration)


def design_objects():
    from methodology.tsbs.stage9.contracts import read_inputs
    inp = read_inputs(SNAP)
    for k, h in PREVIOUS_UPSTREAM.items():
        if inp['sha256'][k] != h:
            raise ValueError('Regenerated design differs from the previous execution: ' + k)
    return {d['deployment_id']: d for d in admissible()}, inp['plans'], inp['writes']


def admissible():
    from methodology.tsbs.stage8 import consolidator as C
    raw = C.load_inputs(SNAP); state = C.validate(raw); pol = raw['policy']
    ids = sorted(state['specs']); mand = pol['canonical_source_candidate_id']; opt = [x for x in ids if x != mand]
    out = []
    for prof in pol['profiles']:
        deps = []
        for n in range(min(len(opt), pol['max_tables_per_deployment'] - 1) + 1):
            for combo in itertools.combinations(opt, n):
                d = C._make_deployment(tuple(sorted((mand,) + combo)), prof, state, pol)
                if d:
                    deps.append(d)
        front = C._pareto(deps)
        front.sort(key=lambda d: (d['weighted_proxy_score'], d['table_count'], d['candidate_ids']))
        fids = {d['deployment_id'] for d in front}
        rank = {d['deployment_id']: i + 1 for i, d in enumerate(front[:pol['top_k']])}
        for d in deps:
            d['pareto_analytic'] = d['deployment_id'] in fids; d['hbench_rank'] = rank.get(d['deployment_id'])
        out.extend(deps)
    return out


def adapt_types():
    """The pinned official generator emits series_bigint lines (integer cpu values); the frozen Stage 9
    loader accepts only series_double. Lossless adapter: same tags/series/time, value as float."""
    import gzip
    if DATA_FILE.exists():
        return
    n = 0
    with gzip.open(RAW_FILE, 'rt') as src, gzip.open(DATA_FILE, 'wt', compresslevel=6) as dst:
        for line in src:
            p = line.rstrip('\n').split(',')
            if p[0] != 'series_bigint' or p[1] != 'cpu':
                raise ValueError('unexpected generator line type: ' + p[0])
            v = int(p[-1]); p[0] = 'series_double'; p[-1] = repr(float(v))
            if float(p[-1]) != v:
                raise ValueError('lossy conversion')
            dst.write(','.join(p) + '\n'); n += 1
    return n


def dataset():
    from methodology.tsbs.stage9.dataset import load_cpu_dataset
    adapt_types()
    return load_cpu_dataset(DATA_FILE, max_observations=50000)


def make_instances(ds):
    from methodology.tsbs.stage9.instances import generate_instances
    out = []
    for coh in ('natural', 'aligned'):
        for i in generate_instances(ds, interval_seconds=INTERVAL_S, per_template=INSTANCES['per_template'],
                                    seed=INSTANCES['seed'], alignment_cohort=coh):
            i['cohort'] = coh; i['instance_key'] = f"{coh[0].upper()}_{i['instance_id']}"
            out.append(i)
    return out


def build_registration(smoke=False):
    snap = frozen_hashes()
    deps, plans, writes = design_objects()
    hb = read(SNAP / 'outputs/tsbs_stage8/28_hbench_selected_deployments.json')
    hb_ids = [d['deployment_id'] for d in hb]
    if not set(hb_ids) <= set(deps):
        raise AssertionError('HBench not contained in reconstructed admissible space')
    ds = dataset()
    if not INST_FILE.exists():
        dump(INST_FILE, make_instances(ds))
    variants = []
    extra = [d for d in deps.values() if d['pareto_analytic'] and d['deployment_id'] not in hb_ids]
    for d in sorted(deps.values(), key=lambda d: (d['profile'], d['weighted_proxy_score'])):
        if d['deployment_id'] in hb_ids or d in extra:
            variants.append({'label': d['deployment_id'], 'deployment_id': d['deployment_id'], 'kind': 'cover',
                             'profile': d['profile'], 'candidates': d['candidate_ids'],
                             'in_hbench': d['deployment_id'] in hb_ids, 'hbench_rank': d['hbench_rank'],
                             'pareto_analytic': d['pareto_analytic'], 'score': d['weighted_proxy_score'],
                             'normalized_proxies': d['normalized_proxies']})
    variants.append({'label': BASELINE_ID, 'deployment_id': BASELINE_ID, 'kind': 'baseline', 'candidates': ['series_bigint'],
                     'description': 'Esquema Cassandra original do TSBS para estes dados: series_bigint (series_id, timestamp_ns, value bigint), sem COMPACT STORAGE'})
    r1 = next(d['deployment_id'] for d in hb if d['profile'] == 'conservative' and d['rank'] == 1)
    variants.append({'label': r1 + AA_SUFFIX, 'deployment_id': r1, 'kind': 'aa_replica', 'replica_of': r1, 'candidates': deps[r1]['candidate_ids']})
    tag = hashlib.sha256((sha(SNAP / 'outputs/tsbs_stage8/28_hbench_selected_deployments.json') + '|v1|run1').encode()).hexdigest()[:6]
    counts = {p: sum(d['profile'] == p for d in deps.values()) for p in ('conservative', 'conditional_aligned')}
    return {
        'status': 'SMOKE' if smoke else 'PREREGISTERED_BEFORE_MEASUREMENT',
        'created_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
        'benchmark': 'TSBS cpu-only (devops), gerador oficial Go commit ' + TSBS_COMMIT,
        'design_phase': {'frozen_sha256': snap, 'previous_execution_upstream_sha256_match': True,
                         'note': 'Stages 1-8 regenerados deterministicamente do repositorio (saidas nao versionadas)',
                         'admissible_counts': counts, 'hbench': hb_ids},
        'data': {'path': str(DATA_FILE.relative_to(ROOT)), 'sha256': sha(DATA_FILE), 'raw_generator_file': str(RAW_FILE.relative_to(ROOT)), 'raw_generator_sha256': sha(RAW_FILE), 'type_adapter': 'series_bigint -> series_double (valores inteiros, conversao sem perda verificada linha a linha)', 'generator': GENERATOR,
                 'tsbs_commit': TSBS_COMMIT, 'observations': ds['observations'], 'metric_cells': ds['metric_cells'],
                 'hosts': len(ds['hostnames']), 'start_ns': ds['start_ns'], 'last_ns': ds['last_ns']},
        'instances': {'path': str(INST_FILE.relative_to(ROOT)), 'sha256': sha(INST_FILE), **INSTANCES,
                      'cohorts': ['natural', 'aligned'],
                      'note': 'primaria: natural (janelas TSBS arbitrarias); secundaria: aligned (premissa do perfil conditional_aligned)'},
        'interval_s': INTERVAL_S,
        'code_sha256': code_hashes(), 'protocol_file_sha256': code_hashes()['common/protocol_v1.json'],
        'variants': variants, 'hbench_ids_ranked': hb_ids,
        'verification_scope': ('Espaco admissivel = 2 (conservative) + 24 (conditional_aligned) = 26 > 12: pelo protocolo v1 '
                               'mede-se o HBench (7) + baseline; acrescentam-se os deployments da fronteira de Pareto analitica '
                               'nao selecionados (top-k cortou) para avaliar o corte. Os 16 dominados nao sao medidos.'),
        'aa_pairs': [[r1, r1 + AA_SUFFIX]],
        'run_tag': tag, 'container': CONTAINER, 'port': PORT, 'measurement_seed_base': 20261117,
        'warmup': 2, 'rounds': 12,
        'protocol_deviation': ('DESVIO DECLARADO do protocolo v1 (10 aquecimentos + 60 rodadas por repeticao): no TSBS cada rodada '
                               'executa 384 consultas (32 classes x 12 variantes) e leva ~29 s no smoke (cpu-max-all-32-24, high-cpu-all e '
                               'double-groupby-all fazem centenas de requisicoes). 60 rodadas x 5 repeticoes levariam ~3,5 h; usam-se 2 '
                               'aquecimentos + 12 rodadas por repeticao = 60 amostras por classe e variante no total (5 repeticoes a frio mantidas).'),
        'rounds_note': 'por (classe de consulta = template x coorte, variante, repeticao); 32 classes x 12 rodadas x 5 rep = 60 amostras por classe e variante',
        'decision_metric': 'media do mix uniforme das 16 classes da coorte natural (media das medias por template)',
        'hypotheses': {
            'H1': 'Todos os 7 deployments do HBench respondem corretamente (oraculo independente) a todas as instancias das duas coortes',
            'H2': 'O melhor deployment medido (coorte natural) pertence ao HBench ou empata com um membro do HBench (piso de ruido)',
            'H3': 'Na coorte aligned, deployments conditional_aligned com rollups (TS5/TS6) sao mais rapidos que o conservative rank-1',
            'H4': 'Na coorte natural, rotas condicionais caem em fallback TS0 e o ganho de H3 desaparece ou diminui',
            'H5': 'O conservative rank-1 (TS0+TS2) e mais rapido que o baseline TSBS series_bigint na coorte natural'},
        'firewall': {'selection_uses_measured_results': False, 'hbench_selection_modified': False},
    }


def main():
    if PREREG.exists():
        raise FileExistsError('Already pre-registered')
    reg = build_registration()
    dump_x(PREREG, reg)
    print('PRE-REGISTERED', len(reg['variants']), 'variants', reg['design_phase']['admissible_counts'])
    for v in reg['variants']:
        print(v['label'], v.get('profile'), v.get('hbench_rank'), v.get('candidates'))


if __name__ == '__main__':
    main()
