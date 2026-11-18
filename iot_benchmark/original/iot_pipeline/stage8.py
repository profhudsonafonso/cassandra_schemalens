"""Stage 8: compatible workload covers, Pareto preselection, freeze before any live run."""
from .shared import *
import itertools
import hashlib

def dominated(a,b):
    keys=('read','storage','write')
    return all(b["analytic_cost"][k]<=a["analytic_cost"][k]+1e-9 for k in keys) and any(b["analytic_cost"][k]<a["analytic_cost"][k]-1e-9 for k in keys)

def run():
    p=require_policy();specs=read(path4(4,'10_candidate_specs.json'))
    plans=read(path4(5,'15_query_plans.json')); costs=read(path4(7,'23_read_proxies.json'))
    footprint=read(path4(7,'21_storage_proxies.json'));writecost=read(path4(7,'22_write_proxies.json'))
    queries=read(inputs()['queries'])
    if len(queries)!=12 or {q['query_id'] for q in queries}!={f'Q{i}' for i in range(1,13)}:
        raise ValueError('This controlled prototype requires all twelve A1 operation classes')
    byf={s['family']:s for s in specs};byid={s['candidate_id']:s for s in specs}
    if 'TS0' not in byf:raise ValueError('Missing canonical source TS0')
    opt=[s for s in specs if s['family']!='TS0']
    if len(opt)>15: raise ValueError('Bounded enumeration guard exceeded')
    plan_cost={x['plan_id']:x for x in costs}
    fps={x['candidate_id']:x for x in footprint};wc={x['candidate_id']:x for x in writecost}
    base=next(s for s in specs if s['family']=='TS0');allcovers=[]
    for k in range(len(opt)+1):
        for chosen in itertools.combinations(opt,k):
            cids={base['candidate_id'],*(s['candidate_id'] for s in chosen)}
            if len(cids)>p['max_candidates_per_deployment']:continue
            # Same TS2 access family, alternative physical representations.
            groups=[s.get('exclusive_group') for s in chosen if s.get('exclusive_group')]
            if len(groups)!=len(set(groups)):continue
            routes={};measure=[]
            for q in queries:
                available=[x for x in plans if x['query_id']==q['query_id'] and x['candidate_id'] in cids
                           and x['route_status'] in ('DIRECT','CONDITIONAL')]
                if not available:break
                def analytic(z):
                    cc=plan_cost[z['plan_id']]
                    return p['read_proxy_weights']['request_count']*cc['request_count_proxy']+p['read_proxy_weights']['logical_rows']*cc['logical_rows_proxy']
                best=min(available,key=lambda z:(analytic(z),z['candidate_id']))
                routes[q['query_id']]={'candidate_id':best['candidate_id'],'family':best['family'],
                                       'plan_id':best['plan_id'],'route_status':best['route_status'],
                                       'runtime_guard':best['guard']}
                measure.append(analytic(best))
            if len(routes)!=len(queries):continue
            sizes=sum(fps[c]['estimated_bytes_proxy'] for c in cids)
            touched=sum(wc[c]['write_keys_proxy'] for c in cids)
            cost={'read':sum(measure)/len(measure),'storage':sizes,'write':touched}
            signature=p['experiment_revision']+'|'+('|'.join(sorted(cids)));hid='H_'+hashlib.sha256(signature.encode()).hexdigest()[:12]
            allcovers.append({'deployment_id':hid,'candidates':sorted(cids),'families':sorted(byid[c]['family'] for c in cids),
                              'query_routes':routes,'analytic_cost':cost,'canonical_signature':signature,
                              'semantically_complete':True,'validated_live':False})
    if not allcovers:raise ValueError('No complete compatible cover: cannot benchmark!')
    # Separate physical identity is encoded by exact table id (no accidental scope fusion).
    pareto=[h for h in allcovers if not any(dominated(h,k) for k in allcovers if k is not h)]
    mins={key:min(h['analytic_cost'][key] for h in allcovers) for key in ('read','storage','write')}
    def score(h):
        return sum(p['preselect_weights'][k]*h['analytic_cost'][k]/max(1e-9,mins[k]) for k in ('read','storage','write'))
    # Report the simplest baseline as diagnostic while keeping only Pareto-ready choices.
    for h in allcovers:h['preselection_score']=round(score(h),8)
    pareto=sorted(pareto,key=lambda h:(h['preselection_score'],len(h['candidates']),h['deployment_id']))
    selected=pareto[:p['top_k']]
    if len({x['deployment_id'] for x in selected})!=len(selected):raise ValueError('Duplicate deployment id')
    output=path4(8,'28_hbench_frozen.json');write(output,selected)
    write(path4(8,'29_all_valid_covers.json'),allcovers)
    routes=[{'deployment_id':h['deployment_id'],'query_id':qid,**v} for h in selected for qid,v in h['query_routes'].items()]
    csv_write(path4(8,'30_hbench_summary.csv'),[{'deployment_id':h['deployment_id'],
             'families':'|'.join(h['families']),'candidate_count':len(h['candidates']),
             'score':h['preselection_score'],'read_proxy':h['analytic_cost']['read'],
             'storage_proxy':h['analytic_cost']['storage'],'write_proxy':h['analytic_cost']['write']} for h in selected],
             ['deployment_id','families','candidate_count','score','read_proxy','storage_proxy','write_proxy'])
    csv_write(path4(8,'31_selected_query_routes.csv'),routes,
              ['deployment_id','query_id','candidate_id','family','plan_id','route_status','runtime_guard'])
    manifest_file=path4(8,'32_frozen_manifest.json')
    proof=manifest(8,{'specs':path4(4,'10_candidate_specs.json'), 'plans':path4(5,'15_query_plans.json'),
             'write_plans':path4(6,'18_write_plans.json'),'read_costs':path4(7,'23_read_proxies.json'),
             'storage_costs':path4(7,'21_storage_proxies.json'),'write_costs':path4(7,'22_write_proxies.json'),
             'queries':inputs()['queries'],'data':inputs()['native'],'policy':POLICY,
             'stage4_source':ROOT/'iot_pipeline/stage4.py',
             'stage5_source':ROOT/'iot_pipeline/stage5.py',
             'stage6_source':ROOT/'iot_pipeline/stage6.py',
             'stage7_source':ROOT/'iot_pipeline/stage7.py',
             'stage8_source':ROOT/'iot_pipeline/stage8.py',
             'stage9_source':ROOT/'iot_pipeline/stage9.py',
             'offline_oracle_source':ROOT/'iot_pipeline/offline.py',
             'a1_bridge_source':ROOT/'phase_a1/core/bridge.py',
             'a1_runner_source':ROOT/'phase_a1/tools/run_a1.py',
             'official_java_object_import_source':ROOT/'phase_a1/tools/official_query_capture.py',
             'official_java_capture_example_source':ROOT/'java_bridge/cn/edu/tsinghua/iot/benchmark/tools/SchemaLensQueryCapture.java',
             'ts_activation_rules':ROOT/'phase_a1/vendor/tsdb_base/config/tsdb_activation_rules.json',
             'ts_modifier_rules':ROOT/'phase_a1/vendor/tsdb_base/config/temporal_modifier_rules.json'},
             'FROZEN_PREBENCH_SELECTION',{'number_of_compatible_covers':len(allcovers),
             'number_on_pareto_front':len(pareto),'number_selected':len(selected),
             'chosen_ids':[x['deployment_id'] for x in selected],
             'frozen_hbench_sha256':sha(output),'candidate_costs_analytical_only':True,
             'official_iot_query_sequence_verified':False,'official_java_sampler_wired_as_stage8_input':False,'selection_is_not_observed_performance':True})
    write(manifest_file,proof)
    return selected
