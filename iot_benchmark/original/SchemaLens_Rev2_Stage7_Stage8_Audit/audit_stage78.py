#!/usr/bin/env python3
"""Read-only audit for SchemaLens Cassandra V4 Rev2 Stage 7 / Stage 8.

It compares the immutable, PRE-BENCHMARK prediction files to the frozen
confirmatory Cassandra measurements without fitting or modifying anything.
Python standard library only. No Cassandra connection and no destructive calls.
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

REV2 = Path('outputs/v4_rev2_prebenchmark')
REQUIRED_RESULTS = ('manifest.json','raw_latencies.csv','query_p95.csv',
                    'orders.csv','load.csv','aggregated.csv')
P95_TOLERANCE = 1e-6


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def readjson(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def readcsv(path):
    with Path(path).open(newline='', encoding='utf-8-sig') as f:
        return list(csv.DictReader(f))


def writecsv(path, rows, fields):
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore')
        w.writeheader();w.writerows(rows)


def writejson(path,obj):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,indent=2,ensure_ascii=False,sort_keys=True)+'\n',encoding='utf-8')


def near(a,b,atol=1e-6,rtol=1e-7):
    return math.isclose(float(a),float(b),abs_tol=atol,rel_tol=rtol)


def percentile_nearest_rank(values,p):
    if not values:raise ValueError('Empty sample')
    v=sorted(values)
    return v[max(0,math.ceil(p/100*len(v))-1)]


def require(cond,msg):
    if not cond:raise ValueError(msg)


def audit_observed(results):
    manifest=readjson(results/'manifest.json')
    require(manifest.get('status')=='CONFIRMATORY_CASSANDRA_COMPLETE',
            'Unexpected confirmatory run status')
    require(manifest.get('correctness_checked_before_timing') is True,
            'Correctness was not attested before latency measurement')
    raw=readcsv(results/'raw_latencies.csv')
    qs=readcsv(results/'query_p95.csv')
    orders=readcsv(results/'orders.csv')
    loads=readcsv(results/'load.csv')
    agg=readcsv(results/'aggregated.csv')
    expected_labels=set(manifest['selected_ids']) | {'BASELINE_ORIGINAL','BASELINE_Q8_OPTIMIZED'}
    samples=defaultdict(list);byorder={};errors=[]
    for row in raw:
        key=(row['query_id'],row['variant'])
        require(row['variant'] in expected_labels,'Unfrozen or unknown measured variant: '+row['variant'])
        for fld in ('latency_ms','rep','position'):
            require(row[fld]!='','Missing raw field '+fld)
        require(math.isfinite(float(row['latency_ms'])) and float(row['latency_ms'])>=0,
                'Non-finite or negative latency')
        require(int(row['rep'])>=1,'Nonpositive rep')
        samples[key].append(row)
        orderkey=(row['query_id'],int(row['rep']))
        byorder.setdefault(orderkey,{})
        require(int(row['position']) not in byorder[orderkey],'Duplicate position')
        byorder[orderkey][int(row['position'])]=row['variant']
    require(len(raw)==int(manifest['records']),'Raw record count mismatch')
    require(len(samples)==len(expected_labels)*int(manifest['query_classes']),
            'Incomplete query/variant coverage')
    for key,group in samples.items():
        require(len(group)==int(manifest['rounds']),f'Expected {manifest["rounds"]} rounds for {key}')
        require({int(x['rep']) for x in group}==set(range(1,int(manifest['rounds'])+1)),
                f'Missing repetitions for {key}')
    for o in orders:
        key=(o['query_id'],int(o['rep']))
        predicted=o['variants_order'].split('|')
        observed=[v for _,v in sorted(byorder.get(key,{}).items())]
        require(observed==predicted,f'Execution-order mismatch at {key}: {observed} != {predicted}')
        require(len(predicted)==len(expected_labels) and set(predicted)==expected_labels,
                f'Incomplete execution order at {key}')
    require(len(orders)==int(manifest['query_classes'])*int(manifest['rounds']),
            'Unexpected orders count')
    qsummary=[]
    q95={(r['query_id'],r['variant']):r for r in qs}
    require(set(q95)==set(samples),'query_p95 contains missing or duplicate/mismatched pairs')
    for (qid,variant), group in sorted(samples.items()):
        observed_latency=[float(x['latency_ms']) for x in group]
        p95=percentile_nearest_rank(observed_latency,95)
        p99=percentile_nearest_rank(observed_latency,99)
        mean=statistics.mean(observed_latency)
        r=q95[qid,variant]
        require(r.get('result_correctness')=='PASS_BEFORE_TIMING',
                f'Result correctness status missing for {qid} {variant}')
        for key,value in [('p95_ms',p95),('p99_ms',p99),('mean_ms',mean)]:
            require(near(value,r[key],atol=P95_TOLERANCE),f'Percentile mismatch for {qid} {variant} {key}')
        def counts(k):
            numeric=[int(float(x[k])) for x in group if x.get(k) not in (None,'')]
            if len(numeric)!=len(group):return ('','',False)
            return (str(statistics.mode(numeric)),str(min(numeric))+'..'+str(max(numeric)),len(set(numeric))==1)
        rcount,rrange,rstable=counts('requests')
        ncount,nrange,nstable=counts('iterated_rows')
        qsummary.append({'query_id':qid,'variant':variant,'p95_ms':round(p95,9),
          'p99_ms':round(p99,9),'mean_ms':round(mean,9),'median_ms':round(statistics.median(observed_latency),9),
          'requests_observed':rcount,'requests_range':rrange,'requests_stable':rstable,
          'rows_observed':ncount,'rows_range':nrange,'rows_stable':nstable,
          'samples':len(group),'route':group[0]['route']})
    byvariant=defaultdict(list)
    for x in qsummary:byvariant[x['variant']].append(x)
    aggregate=[]
    registered={x['variant']:x for x in agg}
    require(set(registered)==expected_labels,'Aggregated variant labels differ from frozen selection')
    for variant,parts in sorted(byvariant.items()):
        m=statistics.mean(x['p95_ms'] for x in parts)
        m_no8=statistics.mean(x['p95_ms'] for x in parts if x['query_id']!='Q8')
        m_no5=statistics.mean(x['p95_ms'] for x in parts if x['query_id']!='Q5')
        require(near(m,registered[variant]['mean_12_query_p95_ms'],atol=P95_TOLERANCE),
                f'Aggregated mean p95 mismatch for {variant}')
        require(near(m_no8,registered[variant]['mean_11_query_p95_excluding_q8_ms'],atol=P95_TOLERANCE),
                f'Aggregated excluding Q8 mismatch for {variant}')
        aggregate.append({'variant':variant,'mean_per_query_p95_ms':round(m,9),
                          'mean_p95_excluding_q8_ms':round(m_no8,9),
                          'mean_p95_excluding_q5_ms':round(m_no5,9)})
    require({x['variant'] for x in loads}==expected_labels-{'BASELINE_Q8_OPTIMIZED'},
            'Load files must contain baseline physical table once and selected configurations')
    return manifest,raw,qsummary,aggregate,loads,{'raw_rows':len(raw),'orders':len(orders),'query_summaries':len(qsummary)}


def dominates(a,b):
    ka=('read','storage','write')
    return all(a[k]<=b[k]+1e-8 for k in ka) and any(a[k]<b[k]-1e-8 for k in ka)


def audit_frozen(exp,observed_manifest,addon=None):
    root=exp/REV2
    meta=readjson(root/'stage8/manifest.json')
    require(meta.get('status')=='FROZEN_V4_REV2_PREBENCH','Rev2 prebenchmark is not frozen')
    require(meta.get('independent_from_original_v3') is True,'Not marked independent from old V3')
    require(meta.get('no_measured_latency_used') is True and meta.get('no_Stage9_results_used') is True,
            'Prebenchmark lacks measured-latency exclusion claims')
    require(meta.get('no_Cassandra_used') is True,
            'Prebenchmark has no explicit no-Cassandra marker')
    require(meta.get('parent_v3_hbench_sha256')==observed_manifest.get('parent_v3_sha256'),
            'Historical V3 frozen hash differs from confirmatory manifest')
    checked=0
    # Validate immutable Rev2 parent inputs and the original V3 snapshot when available.
    original_parent=meta.get('parent_v3_files_sha256',{})
    for rel,digest in original_parent.items():
        path=exp/rel
        require(path.is_file() and sha(path)==digest,'Parent V3 freeze changed: '+rel)
        checked+=1
    for key,rel in {'candidate_specs':'outputs/stage4/10_candidate_specs.json',
                    'activation':'phase_a1/outputs/activation_ts_only.json',
                    'queries':'phase_a1/outputs/query_instances_pilot.json',
                    'data':'phase_a1/outputs/normalized_double_readings.csv',
                    'policy':'config/pipeline_policy.json'}.items():
        original=meta.get('inputs_sha256',{}).get(key)
        require(original and sha(exp/rel)==original,'Rev2 selection input changed: '+key)
        checked+=1
    if addon is not None:
        src=Path(addon).resolve()
        for key,rel in {'prebenchmark':'v4_rev2/prebenchmark.py',
                        'cost':'v4_rev2/cost.py',
                        'v4_auto_selector':'v4_auto/selector.py'}.items():
            expected=meta.get('code_sha256',{}).get(key)
            require(expected and sha(src/rel)==expected,'Frozen selector/source code hash changed: '+rel)
            checked+=1
        for key,rel in {'benchmark':'v4_rev2/benchmark.py',
                        'runtime_helpers':'v4_rev2/runtime_helpers.py'}.items():
            expected=observed_manifest.get('runtime_code_sha256',{}).get(key)
            require(expected and sha(src/rel)==expected,'Measured benchmark/runtime code hash mismatch: '+rel)
            checked+=1
    for rel,digest in meta['artifact_sha256'].items():
        path=root/rel
        require(path.is_file() and sha(path)==digest,'Rev2 frozen artifact hash mismatch: '+rel)
        checked+=1
    frozen_path=root/'stage8/28_hbench_v4_rev2_frozen.json'
    require(sha(frozen_path)==observed_manifest['frozen_HBench_sha256'],
            'Measured HBench SHA does not match the frozen Rev2 selection')
    require(sha(root/'stage8/manifest.json')==observed_manifest['frozen_manifest_sha256'],
            'Measured prebenchmark manifest SHA does not match immutable freeze')
    require(meta['dataset_query_hashes']['data']==observed_manifest['dataset_sha256'] and
            meta['dataset_query_hashes']['queries']==observed_manifest['queries_sha256'],
            'Measured dataset or query hash changed after selection')
    require(meta['java_capture']['sha256']==observed_manifest['new_generator_capture_sha256'],
            'Native Java query capture hash mismatch')
    selected=readjson(frozen_path)
    covers=readjson(root/'stage8/29_all_compatible_covers.json')
    readplans=readjson(root/'stage7/23_read_proxies_rev2.json')
    writeplans=readjson(root/'stage7/22_write_proxies.json')
    storeplans=readjson(root/'stage7/21_storage_proxies.json')
    plans=readjson(root/'stage5/15_cross_query_coverage.json')
    explanations=readjson(root/'stage8/32_selection_explanations_rev2.json')
    partitions=readjson(root/'stage7/24_partition_proxies_rev2.json')
    policy=readjson(root/'stage7/25_rev2_cost_policy.json')
    require(len(selected)==meta['selected_count'] and len(covers)==meta['cover_count'],
            'Frozen selected/compatible count mismatch')
    ids=[x['deployment_id'] for x in selected]
    require(len(ids)==len(set(ids)) and ids==meta['selected_ids'],
            'Frozen selection IDs duplicate or disagree with manifest')
    require(set(ids)==set(observed_manifest['selected_ids']),
            'Measured selected IDs differ from the frozen selection')
    cid={c['deployment_id']:c for c in covers}
    require(all(x in cid for x in ids),'Frozen selected design not in enumerated covers')
    rc={(r['query_id'],r['candidate_id']):r for r in readplans}
    pc={(p['query_id'],p['candidate_id']):p for p in plans}
    wc={x['candidate_id']:float(x['physical_writes_proxy']) for x in writeplans}
    sc={x['candidate_id']:float(x['estimated_bytes_proxy']) for x in storeplans}
    exp_by={x['deployment_id']:x for x in explanations}
    annotations=[];allchecks=[]
    accepted={cid for h in selected for cid in h['candidates']}
    for p in partitions:
        if p['candidate_id'] in accepted:
            require(p.get('hard_guard_pass') is True,
                    'Stage8 selected candidate failing frozen hard partition guard: '+p['candidate_id'])
    front={c['deployment_id'] for c in covers if not any(dominates(other['analytic_cost'],c['analytic_cost']) for other in covers if other is not c)}
    # Scores must reproduce the declared preselect weights and raw analytical costs.
    base_policy=readjson(exp/'config/pipeline_policy.json')
    weights=base_policy['preselect_weights']
    minima={key:min(float(c['analytic_cost'][key]) for c in covers)
            for key in ('read','storage','write')}
    for c in covers:
        score=round(sum(float(weights[key])*float(c['analytic_cost'][key])/
              max(1e-9,minima[key]) for key in ('read','storage','write')),8)
        require(near(score,c['preselection_score'],atol=1e-8),
                'Stage8 normalized preselection score mismatch '+c['deployment_id'])
    require(set(exp_by)==set(ids),'Stage8 explanations do not cover exactly selected designs')
    for h in selected:
        did=h['deployment_id'];available=cid[did]
        require(set(h['candidates'])==set(available['candidates']),f'Selected candidates modified {did}')
        require(h['query_routes']==available['query_routes'],f'Selected query route modified {did}')
        expected_reason='PARETO_FRONT' if did in front else 'STRUCTURAL_CANONICAL_DIVERSITY_SAFEGUARD'
        require(h['selection_reason']==expected_reason,f'Inconsistent Pareto/diversity label {did}')
        require(did in exp_by,f'Missing explanation {did}')
        require(exp_by[did]['selection_reason']==expected_reason,f'Explanation label mismatch {did}')
        ex_by_q={x['query_id']:x for x in exp_by[did]['query_explanations']}
        require(set(ex_by_q)==set(h['query_routes']),f'Stage8 per-query explanation coverage mismatch {did}')
        require(len(h['query_routes'])==int(observed_manifest['query_classes']),
                f'Selected deployment lacks some query routes {did}')
        read_cost=[]
        for qid,route in h['query_routes'].items():
            key=(qid,route['candidate_id'])
            require(key in rc and key in pc,f'Missing Stage7 or coverage plan {did}/{qid}')
            require(pc[key]['route_status']=='DIRECT',f'Frozen route not semantically covered {did}/{qid}')
            require(pc[key]['plan_id']==route['plan_id'],f'Plan ID mismatch {did}/{qid}')
            require(ex_by_q[qid]['candidate_id']==route['candidate_id'] and
                    ex_by_q[qid]['plan_cost_features']==rc[key],
                    f'Stage8 explanation features differ from Stage7 {did}/{qid}')
            read_cost.append(float(rc[key]['analytic_read_proxy']))
            annotations.append({'deployment_id':did,'query_id':qid,'candidate_id':route['candidate_id'],
                                'family':route['family'],'cross_query_reuse':pc[key].get('cross_query_reuse',''),
                                'stage7_plan_id':route['plan_id'],**rc[key]})
        estimated={'read':statistics.mean(read_cost),
                   'storage':sum(sc[x] for x in h['candidates']),
                   'write':sum(wc[x] for x in h['candidates'])}
        for k in ('read','storage','write'):
            require(near(h['analytic_cost'][k],estimated[k],atol=1e-5),f'Stage8 aggregation mismatch {did}/{k}')
        allchecks.append({'deployment_id':did,'canonical_source':h['canonical_source'],
                          'families':'+'.join(h['families']),
                          'selection_reason':expected_reason,
                          'analytic_read_cost':h['analytic_cost']['read'],
                          'analytic_write_cost':h['analytic_cost']['write'],
                          'analytic_storage_cost':h['analytic_cost']['storage'],
                          'preselection_score':h['preselection_score'],
                          'on_pareto_front':did in front})
    require(len(selected)<=meta['top_k'],'Stage8 selected more than frozen top-k')
    require(sum(x in front for x in cid)==meta['pareto_count'],'Pareto count disagrees with manifest')
    return {'manifest':meta,'selected':selected,'covers':covers,'readplans':rc,'writeplans':wc,
            'storeplans':sc,'annotations':annotations,'deployments':allchecks,
            'partition_info':partitions,'policy':policy,'checked_hash_files':checked}


def compare_plan_counts(frozen,observed_summary):
    obs={(x['query_id'],x['variant']):x for x in observed_summary}
    out=[];families=Counter()
    for a in frozen['annotations']:
        did=a['deployment_id'];qid=a['query_id'];x=obs[(qid,did)]
        pred_calls=a['request_count_proxy'];pred_rows=a['physical_rows_proxy']
        act_calls=x['requests_observed'];act_rows=x['rows_observed']
        if act_calls=='' or act_rows=='':
            status='NOT_INSTRUMENTED';families['not_instrumented']+=1
        elif not (x['requests_stable'] and x['rows_stable']):
            status='VARIABLE_OBSERVED_COUNTS';families['variable']+=1
        elif int(round(float(pred_calls)))!=int(act_calls) or int(round(float(pred_rows)))!=int(act_rows):
            status='MISMATCH';families['mismatch']+=1
        else:
            status='MATCH';families['match']+=1
        out.append({'deployment_id':did,'query_id':qid,'family':a['family'],
            'candidate_id':a['candidate_id'],'cross_query_reuse':a['cross_query_reuse'],
            'pred_requests':pred_calls,'observed_requests':act_calls,
            'delta_requests':(int(act_calls)-float(pred_calls)) if act_calls else '',
            'pred_physical_rows':pred_rows,'observed_iterated_rows':act_rows,
            'delta_rows':(int(act_rows)-float(pred_rows)) if act_rows else '',
            'pred_extra_bucket_fanout':a['extra_bucket_fanout_proxy'],
            'pred_bytes':a['returned_bytes_proxy'],
            'pred_read_cost_units':a['analytic_read_proxy'],
            'status':status})
    return out,dict(families)


def compare_writes(frozen,loads):
    out=[];bylabel={x['variant']:x for x in loads}
    for h in frozen['selected']:
        did=h['deployment_id'];actual=int(bylabel[did]['physical_write_operations'])
        pred=sum(frozen['writeplans'][cid] for cid in h['candidates'])
        out.append({'deployment_id':did,'pred_write_operations':pred,
                    'observed_write_operations':actual,'delta':actual-pred,
                    'status':'MATCH' if near(pred,actual) else 'MISMATCH',
                    'load_seconds_observed':bylabel[did]['load_seconds'],
                    'note':'physical write operations, NOT write throughput'})
    return out


def make_report(summary,full):
    obs=summary['observed']
    m=summary['measured_manifest'];ag=summary['aggregates']
    # Order presents baseline optimized, selected by observed ranking, original.
    ags=sorted(ag,key=lambda r:r['mean_per_query_p95_ms'])
    lines=['# SchemaLens Rev2 — Auditoria dos Stages 7 e 8',
           '',f'**Estado:** {"AUDITORIA COMPLETA (artefatos do Stage7/8 disponíveis)" if full else "AUDITORIA PARCIAL (sem o congelamento Stage7/8)"}',
           '',f'- Medições brutas verificadas: **{obs["raw_rows"]}**.',
           f'- Ordens de execução verificadas: **{obs["orders"]}**.',
           f'- p95/p99 e médias por consulta recalculados: **{obs["query_summaries"]}**.',
           '- Unidade da latência: **milissegundos ponta a ponta no cliente** (driver + processamento Python).',
           '- Agregado: **média não ponderada de p95 por consulta**, NÃO p95 da mistura do workload.',
           '- Q1 tem zero resultados segundo o manifesto; não interpreta como carga útil positiva.',
           '', '## Agregados observados', '',
           '| Variante | Média dos p95 Q1–Q12 (ms) | Sem Q8 (ms) | Sem Q5 (ms) |',
           '|---|---:|---:|---:|']
    for r in ags:lines.append(f"| `{r['variant']}` | {r['mean_per_query_p95_ms']:.4f} | {r['mean_p95_excluding_q8_ms']:.4f} | {r['mean_p95_excluding_q5_ms']:.4f} |")
    lines+=['','## Plano físico: previsão versus observação','']
    if not full:
        lines += ['**PENDENTE:** para calcular a concordância das estimativas do Stage 7, execute novamente este auditor com `--experiment` apontando ao experimento confirmatório que contém `outputs/v4_rev2_prebenchmark/`. Os CSVs enviados registram as operações reais, mas não incluem os proxies por plano congelados.',
                  '', 'Em especial, o executor TS0 (`TS0_FROZEN_ROUTE`) registrou campos `requests` e `iterated_rows` vazios; esse instrumento precisa ser acrescentado em uma campanha *nova* para comparar suas estimativas de acesso.',
                  '']
    else:
        counts=summary['plan_status_counts']
        lines += [f"- Rotas com reutilização cruzada documentada: **{summary['cross_query_reuse_selected_routes']}**.",
                  f"- Rotas `MATCH`: **{counts.get('match',0)}**.",
                  f"- Rotas `MISMATCH`: **{counts.get('mismatch',0)}**.",
                  f"- Rotas sem telemetria: **{counts.get('not_instrumented',0)}**.",
                  f"- Rotas com contagens variáveis: **{counts.get('variable',0)}**.",
                  '- `returned_bytes_proxy`, `extra_bucket_fanout_proxy` e `partition_pressure_proxy` **não são medições diretas** de Cassandra nesta campanha.',
                  '- O relatório **não** ajusta coeficientes ou altera o HBench após observar latências.',
                  '', '### Configurações congeladas, seus custos e justificativas', '',
                  '| ID | Fonte canônica | Seleção | Read proxy | Write proxy | Storage proxy |',
                  '|---|---|---|---:|---:|---:|']
        for d in summary['deployments']:
            lines.append(f"| `{d['deployment_id']}` | `{d['canonical_source']}` | {d['selection_reason']} | {float(d['analytic_read_cost']):.3f} | {float(d['analytic_write_cost']):.0f} | {float(d['analytic_storage_cost']):.0f} |")
        lines += ['',f"- Arquivos do congelamento verificados por SHA-256: **{summary['checked_hash_files']}**.",
                 '- O algoritmo de Pareto e a soma dos custos dos planos foram recomputados; falhas de integridade interrompem a auditoria.',
                 '']
    lines+=['## Limites e decisões metodológicas','',
            '1. **Nenhum resultado daqui deve ser retroalimentado no mesmo HBench confirmatório.** Alterar proxies exige uma nova revisão e outro holdout.',
            '2. A comparação de `requests` e `iterated_rows` é frente às operações registradas pelo executor Python; não são contadores internos de *server-side read* do Cassandra.',
            '3. Os bytes transferidos e tamanhos reais de SSTables/partições devem ser avaliados numa etapa separada, sem alterar este congelamento.',
            '4. TS4 pressupõe *batch snapshot*; atualizações concorrentes não foram avaliadas.',
            '5. Este procedimento audita a consistência dos arquivos de seleção e do benchmark; não é reprodução oficial de SQL/IoTDB ou mix de frequências.',
            '', '## Próxima etapa depois desta auditoria', '',
            'Instrumentar TS0 para contar requisições/linhas, medir armazenamento real por tabela e tamanho/partições no Cassandra, repetir em uma nova campanha e comparar erros dos proxies por classe de consulta.',
            '']
    return '\n'.join(lines)


def run(results,experiment,out,addon=None):
    results=Path(results).resolve();out=Path(out).resolve()
    require(out!=results and not out.is_relative_to(results),
            'Audit output must be outside the measured results')
    if experiment is not None:
        require(not out.is_relative_to(Path(experiment).resolve()),
                'Audit output must be outside the immutable frozen experiment')
    for name in REQUIRED_RESULTS:require((results/name).is_file(),'Missing result file: '+str(results/name))
    require(not out.exists(),'Audit output already exists; refusing overwrite: '+str(out))
    manifest,raw,qs,agg,loads,observation=audit_observed(results)
    frozen=None;comparisons=[];writes=[];status={}
    if experiment is not None:
        experiment=Path(experiment).resolve()
        frozen=audit_frozen(experiment,manifest,addon)
        comparisons,status=compare_plan_counts(frozen,qs)
        writes=compare_writes(frozen,loads)
    out.mkdir(parents=True,exist_ok=False)
    # Save source hashes only. Never copy frozen inputs into audit output.
    sum_obj={'status':'AUDIT_COMPLETE' if frozen is not None else 'AUDIT_PARTIAL_NEEDS_FROZEN_STAGE7_8',
             'observed':observation,'measured_manifest':manifest,
             'aggregates':agg,'results_sha256':{f:sha(results/f) for f in REQUIRED_RESULTS},
             'stage7_8_experiment':str(experiment) if experiment else None,
             'plan_status_counts':status,
             'deployments':frozen['deployments'] if frozen else [],
             'cross_query_reuse_selected_routes':sum(a['cross_query_reuse'] is True
                   for a in frozen['annotations']) if frozen else None,
             'checked_hash_files':frozen['checked_hash_files'] if frozen else 0,
             'frozen_code_hashes_verified':bool(addon) if frozen else False,
             'source_code_version':'STAGE78_AUDIT_READONLY_V1'}
    writejson(out/'audit_summary.json',sum_obj)
    writecsv(out/'observed_by_query.csv',qs,['query_id','variant','p95_ms','p99_ms','mean_ms','median_ms',
         'requests_observed','requests_range','requests_stable','rows_observed','rows_range','rows_stable','samples','route'])
    writecsv(out/'observed_aggregates.csv',agg,['variant','mean_per_query_p95_ms','mean_p95_excluding_q8_ms','mean_p95_excluding_q5_ms'])
    if frozen:
        writecsv(out/'stage7_predictions_vs_observations.csv',comparisons,
          ['deployment_id','query_id','family','candidate_id','cross_query_reuse','pred_requests',
           'observed_requests','delta_requests','pred_physical_rows','observed_iterated_rows','delta_rows',
           'pred_extra_bucket_fanout','pred_bytes','pred_read_cost_units','status'])
        writecsv(out/'stage7_writes_vs_observed.csv',writes,
          ['deployment_id','pred_write_operations','observed_write_operations','delta','status','load_seconds_observed','note'])
        writecsv(out/'stage8_selection_audit.csv',frozen['deployments'],
          ['deployment_id','canonical_source','families','selection_reason','analytic_read_cost',
           'analytic_write_cost','analytic_storage_cost','preselection_score','on_pareto_front'])
        # Individual mistakes are auditable, but remain in a separate POST-measurement directory.
        writejson(out/'frozen_stage7_policy_snapshot.json',frozen['policy'])
    (out/'RELATORIO_AUDITORIA_PT.md').write_text(make_report(sum_obj,frozen is not None),encoding='utf-8')
    print('AUDIT:',sum_obj['status'])
    print('Recomputed p95/p99:',len(qs),'from',len(raw),'latencies')
    if frozen:print('Stage7 plan comparisons:',len(comparisons), 'status:',status)
    print('Report:',out/'RELATORIO_AUDITORIA_PT.md')
    return sum_obj


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--results',type=Path,required=True,help='Folder containing manifest.json + 5 confirmatory CSV files')
    p.add_argument('--experiment',type=Path,help='Original Rev2 frozen experiment directory, including outputs/v4_rev2_prebenchmark')
    p.add_argument('--out',type=Path,required=True,help='New, empty output path (never writes into the frozen experiment)')
    p.add_argument('--addon',type=Path,help='Rev2 addon folder; validates frozen code SHA256 (strongly recommended for full audit)')
    a=p.parse_args()
    try:run(a.results,a.experiment,a.out,a.addon)
    except Exception as e:
        print('AUDIT FAILED:',e,file=sys.stderr);sys.exit(2)


if __name__=='__main__':main()
