"""DKE Algorithms 4 and 5 adapted to the frozen TPCx-IoT Cassandra workload.

Candidate canonicalisation, conditional cross-query coverage, compatible exact
cover enumeration, multiobjective Pareto preselection, and reproducible HBench.
NEVER opens a measured benchmark, executes Cassandra, or claims live correctness.
"""
from __future__ import annotations

import csv
import hashlib
import itertools
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from methodology.tpcx_iot.stage4.candidates import ROOT, read_json, write_json, sha256
from methodology.tpcx_iot.stage7.estimator import (
    _verify_stage6, validate_policy as validate_stage7_policy, evaluate_candidate,
)

VERSION = 'tpcx-iot-cassandra-stage8-v1'
OUTNAMES = (
    '50_canonical_shared_candidates.json', '51_validated_conditional_coverage.json',
    '52_complete_compatible_deployments.json', '53_pareto_preselection.json',
    '54_hbench_selected.json', '55_hbench_query_routes.json',
    '56_hbench_summary.csv', '57_stage9_execution_plan.csv',
    '58_stage8_manifest.json', '59_excluded_combinations.json',
)
AXES = ('read_cql_requests', 'replicated_logical_storage',
        'insert_statements_per_event', 'maximum_partition_bytes',
        'physical_partition_count')


def canonical_dump(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def digest(obj: Any) -> str:
    return hashlib.sha256(canonical_dump(obj).encode('utf-8')).hexdigest()


def checked_policy(p: dict[str, Any]) -> None:
    fixed = {
        'policy_version': VERSION,
        'research_only': True,
        'source_profile': 'sensor-name-only/raw_read',
        'selection_uses_measured_results': False,
        'allow_conditional_offline_coverage': True,
        'require_live_coverage_for_measured_claims': True,
        'required_control_candidate': 'ts0_series_raw',
        'max_one_canonical_source': True,
        'max_one_projection_per_materialization': True,
        'require_projection_canonical_source': True,
        'partition_warning_policy': 'REPORT_ONLY_NOT_HARD_EXCLUSION',
        'preselection_mode': 'PARETO_THEN_CONTROL_AND_PHYSICAL_VARIANT_DIVERSITY',
    }
    for key, expected in fixed.items():
        if p.get(key) != expected:
            raise ValueError(f'Unsafe Stage8 policy {key}: expected {expected!r}')
    if type(p.get('top_k')) is not int or not 1 <= p['top_k'] <= 12:
        raise ValueError('top_k must be an integer from 1 to 12')
    if type(p.get('exact_enumeration_limit_candidates')) is not int or not 5 <= p['exact_enumeration_limit_candidates'] <= 16:
        raise ValueError('Exact enumeration limit inconsistent with controlled catalogue')
    weights = p.get('normalized_score_weights')
    if not isinstance(weights, dict) or set(weights) != set(AXES):
        raise ValueError('Wrong score dimensions')
    if any(type(v) not in (int, float) or v < 0 or v > 1 for v in weights.values()):
        raise ValueError('Weights must be finite non-negative fractions')
    if abs(sum(weights.values()) - 1) > 1e-10:
        raise ValueError('Predeclared weights must sum to one')
    if not isinstance(p.get('notes'), list) or len(p['notes']) < 1:
        raise ValueError('Policy explanation required')


def _stage7_validated(root: Path) -> tuple[list[dict], list[dict], list[dict], dict[str, dict], dict[str, str]]:
    specs, reads, writes, earlier_hashes = _verify_stage6(root)
    base = root / 'outputs/tpcx_iot_stage7'
    cost_policy_file = root / 'config/tpcx_stage7_cost_policy.json'
    stage7_policy = read_json(cost_policy_file)
    validate_stage7_policy(stage7_policy)
    manifest = read_json(base / '46_cost_manifest.json')
    if (manifest.get('stage') != 7 or manifest.get('candidate_count') != 5 or
        manifest.get('source_hashes') != earlier_hashes or
        manifest.get('cost_policy_sha256') != sha256(cost_policy_file) or
        manifest.get('benchmark_results_used') is not False or
        manifest.get('scenario_is_measured') is not False or
        manifest.get('measured_latency_or_throughput_reported') is not False or
        manifest.get('hbench_selected') is not False or
        manifest.get('live_cassandra_executed') is not False):
        raise ValueError('Stage7 manifest/provenance mismatch')
    if read_json(base/'40_scenario_evidence.json')['scenario'] != stage7_policy:
        raise ValueError('Stage7 scenario evidence drift')
    rr = {x['candidate_id']: x for x in reads}
    ww = {x['candidate_id']: x for x in writes}
    if len(rr) != len(specs) or len(ww) != len(specs):
        raise ValueError('Read/write plans incomplete')
    expected = {s['candidate_id']: evaluate_candidate(s, rr[s['candidate_id']], ww[s['candidate_id']], stage7_policy)
                for s in specs}
    for filename, segment, segment_family in (
        ('41_physical_footprint_proxies.json','footprint', True),
        ('42_read_cost_proxies.json','read', True),
        ('43_write_cost_proxies.json','write', True),
        ('44_feasibility_diagnostics.json','feasibility', True),
    ):
        recorded = read_json(base/filename)
        computed = [{'candidate_id': cid, 'family': c['family'], **c[segment]}
                    for cid, c in sorted(expected.items())]
        if recorded != computed:
            raise ValueError(f'Stage7 proxy mismatch: {filename}')
    # CSV is an auxiliary display file, not a trusted source of estimates.
    with (base/'45_candidate_cost_matrix.csv').open(newline='',encoding='utf-8') as f:
        csvrows=list(csv.DictReader(f))
    if len(csvrows) != len(specs) or set(x['candidate_id'] for x in csvrows) != set(expected):
        raise ValueError('Stage7 CSV membership changed')
    hashes={**earlier_hashes,'stage7_cost_policy_sha256':sha256(cost_policy_file)}
    for name in ('40_scenario_evidence.json','41_physical_footprint_proxies.json',
                 '42_read_cost_proxies.json','43_write_cost_proxies.json',
                 '44_feasibility_diagnostics.json','45_candidate_cost_matrix.csv',
                 '46_cost_manifest.json'):
        hashes[f'stage7_{name}_sha256']=sha256(base/name)
    return specs, reads, writes, expected, hashes


def _physical_signature(spec: dict) -> str:
    """Canonical structural identity, including partitioning, timestamp bucket and maintenance.

    Table label is excluded: physically equivalent structures may use different names.
    Collision is never inferred from sharing a family or the Reading conceptual root.
    """
    return digest({
        'columns': spec['columns'], 'pk': spec['partition_key'],
        'ck': spec['clustering_key'], 'ck_order': spec['clustering_order'],
        'bucket_seconds':spec['bucket_seconds'], 'query_filter_mode':spec['query_filter_mode'],
        'read_cql_shape':spec['read_cql_template'].replace(spec['table'], '<TABLE>'),
        'write_cql_shape':spec['write_cql_template'].replace(spec['table'], '<TABLE>'),
        'preprocessing':spec['preprocessing'],
        'maintenance':spec['maintenance_requirements'],
    })


def _consolidate(specs: list[dict], reads: list[dict], writes: list[dict], root: Path) -> tuple[list[dict], list[dict]]:
    origins = read_json(root/'outputs/tpcx_iot_stage4/11_query_candidate_map.json')
    by_origins = {}
    for row in origins:
        for ids in row['candidate_ids_by_family'].values():
            for cid in ids:
                by_origins.setdefault(cid,set()).add(row['query_id'])
    read_index = {p['candidate_id']:p for p in reads}
    write_index = {p['candidate_id']:p for p in writes}
    rcoverage = {(x['query_id'],x['candidate_id']):x for x in read_json(root/'outputs/tpcx_iot_stage5/21_read_coverage.json')}
    wcoverage = {x['candidate_id']:x for x in read_json(root/'outputs/tpcx_iot_stage6/31_write_coverage.json')}
    canonical: dict[str,dict] = {}
    coverage: list[dict] = []
    for spec in sorted(specs,key=lambda x:x['candidate_id']):
        cid = spec['candidate_id']; sign = _physical_signature(spec)
        rp = read_index[cid]; wp = write_index[cid]
        rc = rcoverage.get(('Q_READ_2_WINDOWS',cid)); wc=wcoverage.get(cid)
        if not rc or not wc or rc.get('read_coverage')!='CONDITIONAL_OFFLINE_ONLY' or not rc.get('offline_equivalence_fixture_passed'):
            raise ValueError(f'Incomplete conditional read coverage for {cid}')
        if wc.get('write_coverage_status')!='CONDITIONAL_OFFLINE_ONLY' or not wc.get('offline_write_fixture_passed'):
            raise ValueError(f'Incomplete conditional write coverage for {cid}')
        if rc.get('cassandra_executed') or wc.get('live_write_coverage_verified'):
            raise ValueError('Unexpected Stage9 live status in pre-benchmark data')
        if not rp.get('read_cql') or not wp.get('write_cql'):
            raise ValueError('Missing query or write plan')
        if wp.get('role') not in ('canonical_series_table','materialized_sensor_projection'):
            raise ValueError('Unknown table role')
        if sign not in canonical:
            canonical[sign]={
                'shared_candidate_id': 'c_'+sign[:12], 'signature':sign,
                'source_candidate_ids':[cid], 'representative_candidate_id':cid,
                'family':spec['family'], 'materialization_scope':{
                    'owner_entity':'Reading', 'partition_key':spec['partition_key'],
                    'bucket_seconds':spec['bucket_seconds'], 'table_role':wp['role'],
                },
                'origin_queries':sorted(by_origins.get(cid,set())),
                'conditional_coverage':['Q_INGEST','Q_READ_2_WINDOWS'],
                'live_coverage_verified':False,
            }
        else:
            existing=canonical[sign]
            existing['source_candidate_ids'].append(cid)
            existing['origin_queries']=sorted(set(existing['origin_queries'])|by_origins.get(cid,set()))
        coverage.append({
            'candidate_id':cid,'shared_candidate_id':canonical[sign]['shared_candidate_id'],
            'origin_queries':sorted(by_origins.get(cid,set())),
            'read_query_id':'Q_READ_2_WINDOWS',
            'read_plan_id':rp['plan_id'], 'read_status':'CONDITIONAL_OFFLINE_ONLY',
            'read_preconditions':rp['read_preconditions'],
            'write_query_id':'Q_INGEST', 'write_plan_id':wp['plan_id'],
            'write_status':'CONDITIONAL_OFFLINE_ONLY',
            'write_maintenance_role':wp['role'],
            'cross_query_validated_conditional_coverage':['Q_INGEST','Q_READ_2_WINDOWS'],
            'operational_coverage_proven':False,
        })
    return sorted(canonical.values(),key=lambda x:x['shared_candidate_id']),coverage


def _admissible(table_ids: tuple[str,...], specs: dict[str,dict], writes: dict[str,dict], p:dict) -> tuple[bool,str, str|None]:
    can=[cid for cid in table_ids if writes[cid]['role']=='canonical_series_table']
    proj=[cid for cid in table_ids if writes[cid]['role']=='materialized_sensor_projection']
    if not can: return False,'missing_canonical_source',None
    if len(can)!=1: return False,'multiple_canonical_sources',None
    if len(proj)>1: return False,'multiple_alternate_projections',None
    if any(specs[cid]['family']=='TS3' for cid in can):
        return False,'invalid_source_role',None
    # Validate each named physical table is distinct and independently writable.
    if len({specs[cid]['table'] for cid in table_ids}) != len(table_ids):
        return False,'duplicate_physical_table',None
    if len(table_ids) != len(can)+len(proj):
        return False,'unsupported_table_role',None
    return True,'admissible_conditional_cover',can[0]


def _score(deployment:dict, denominator:dict[str,float], policy:dict) -> float:
    weights=policy['normalized_score_weights']
    return sum(weights[a]*(deployment['objectives'][a]/denominator[a]) for a in AXES)


def _dominates(a:dict,b:dict)->bool:
    av=a['objectives'];bv=b['objectives']
    return all(av[k] <= bv[k] for k in AXES) and any(av[k] < bv[k] for k in AXES)


def _pareto(rows:list[dict])->list[dict]:
    return [r for r in rows if not any(x is not r and _dominates(x,r) for x in rows)]


def _deployment(table_ids:tuple[str,...], canonical_id:str, specs:dict[str,dict],
                reads:dict[str,dict], writes:dict[str,dict], costs:dict[str,dict]) ->dict:
    # Read path is not chosen using measured latency. CQL fanout is a declared structural proxy.
    best_read=min(table_ids,key=lambda cid:(costs[cid]['read']['total_cql_requests_per_logical_read'],cid))
    read_cost=costs[best_read]['read']
    storage=sum(costs[cid]['footprint']['rf_adjusted_logical_bytes_proxy'] for cid in table_ids)
    partitions=sum(costs[cid]['footprint']['estimated_physical_partitions'] for cid in table_ids)
    partition_max=max(costs[cid]['footprint']['max_partition_bytes_proxy'] for cid in table_ids)
    warnings=sorted({w for cid in table_ids for w in costs[cid]['feasibility']['warnings']})
    objectives={
        'read_cql_requests':read_cost['total_cql_requests_per_logical_read'],
        'replicated_logical_storage':storage,
        'insert_statements_per_event':len(table_ids),
        'maximum_partition_bytes':partition_max,
        'physical_partition_count':partitions,
    }
    did = 'h_'+digest(list(table_ids))[:12]
    return {
        'deployment_id':did,'tables':list(table_ids),
        'families':sorted({specs[cid]['family'] for cid in table_ids}),
        'canonical_source':canonical_id,
        'materialized_projections':[cid for cid in table_ids if cid!=canonical_id],
        'conditional_query_coverage':['Q_INGEST','Q_READ_2_WINDOWS'],
        'route':{'Q_READ_2_WINDOWS':{
                    'candidate_id':best_read, 'plan_id':reads[best_read]['plan_id'],
                    'read_cql':reads[best_read]['read_cql'],
                    'status':'CONDITIONAL_OFFLINE_ONLY',
                    'requirements':reads[best_read]['read_preconditions'],
                 },
                 'Q_INGEST':{
                    'source_write_plan_id':writes[canonical_id]['plan_id'],
                    'all_required_write_plans':[writes[cid]['plan_id'] for cid in table_ids],
                    'status':'CONDITIONAL_OFFLINE_ONLY',
                    'requirements':['no_partial_commit_silently_accepted',
                                    'durable_replay_and_reconciliation_required_for_TS3',
                                    'stable_event_id_and_full_payload'],
                 }},
        'objectives':objectives,
        'warnings':warnings,
        'deployment_completeness_status':'CONDITIONALLY_COMPLETE_NOT_LIVE_VALIDATED',
        'atomicity_verified':False,'durable_repair_verified':False,
        'benchmark_used_for_selection':False,
    }


def _select(pareto_rows:list[dict], p:dict)->list[dict]:
    """Control + set-cover style physical-variant diversity, Pareto-constrained.

    Greedy novelty is determined by physical table ids, with a stable normalized
    structural cost tie-breaker. The output bound remains strictly enforced.
    """
    k=p['top_k']
    baseline=[r for r in pareto_rows if r['tables']==[p['required_control_candidate']]]
    if len(baseline)!=1:
        raise ValueError('Mandatory baseline is absent from Pareto set: audit objective definitions')
    selected=[baseline[0]]
    seen=set(selected[0]['tables'])
    other=sorted((r for r in pareto_rows if r not in selected),key=lambda x:(x['normalized_proxy_score'],x['deployment_id']))
    while other and len(selected)<k:
        max_novelty=max(len(set(x['tables'])-seen) for x in other)
        if not max_novelty: break
        pick=min((r for r in other if len(set(r['tables'])-seen)==max_novelty),
                 key=lambda x:(x['normalized_proxy_score'],x['deployment_id']))
        selected.append(pick);seen.update(pick['tables']);other.remove(pick)
    for remaining in other:
        if len(selected)>=k:break
        selected.append(remaining)
    return selected


def generate(root:Path=ROOT, output_dir:Path|None=None, policy_file:Path|None=None)->dict[str,Any]:
    root=root.resolve();policy_file=(policy_file or root/'config/tpcx_stage8_consolidation_policy.json').resolve()
    output_dir=(output_dir or root/'outputs/tpcx_iot_stage8').resolve()
    if (output_dir==root or output_dir==root/'outputs' or
        output_dir.is_relative_to(root/'inputs') or output_dir.is_relative_to(root/'config') or
        any(output_dir.is_relative_to(root/'outputs'/f'tpcx_iot_stage{x}') for x in ('1_3','4','5','6','7'))):
        raise ValueError('Stage8 cannot overwrite source artifacts')
    p=read_json(policy_file); checked_policy(p)
    specs, rplans, wplans, costs, source_hashes=_stage7_validated(root)
    if len(specs)>p['exact_enumeration_limit_candidates']:
        raise ValueError('Exact search limit exceeded: never silently truncate')
    sp={x['candidate_id']:x for x in specs}; rp={x['candidate_id']:x for x in rplans}; wp={x['candidate_id']:x for x in wplans}
    canon,coverage=_consolidate(specs,rplans,wplans,root)
    ids=tuple(sorted(sp)); exhaustive_count=(1<<len(ids))-1
    combos=[]; exclusions=[]
    for size in range(1,len(ids)+1):
        for subset in itertools.combinations(ids,size):
            valid,reason,canonical_id=_admissible(subset,sp,wp,p)
            if not valid:
                exclusions.append({'candidate_ids':list(subset),'reason':reason})
                continue
            combos.append(_deployment(subset,canonical_id,sp,rp,wp,costs))
    if exhaustive_count != len(combos)+len(exclusions):
        raise AssertionError('Exact enumeration accounting error')
    if not combos:
        raise ValueError('No conditionally complete workload cover')
    # Scores normalize by the value for the mandatory singleton TS0 baseline,
    # not by an observed winner or outcome-specific min/max.
    controls=[r for r in combos if r['tables']==[p['required_control_candidate']]]
    if len(controls)!=1: raise ValueError('Cannot find baseline for stable normalization')
    denom={key:float(value) for key,value in controls[0]['objectives'].items()}
    if any(v<=0 for v in denom.values()):raise ValueError('Zero control normalizer')
    for row in combos:
        row['normalized_proxy_score']=round(_score(row,denom,p),12)
    front=sorted(_pareto(combos),key=lambda r:(r['normalized_proxy_score'],r['deployment_id']))
    dominated=sorted({r['deployment_id'] for r in combos}-{r['deployment_id'] for r in front})
    chosen=_select(front,p)
    ranks={r['deployment_id']:i+1 for i,r in enumerate(chosen)}
    for r in chosen:r['hbench_selection_order']=ranks[r['deployment_id']]
    hbench_ids={x['deployment_id'] for x in chosen}
    records=[]
    for r in chosen:
        for qid,route in sorted(r['route'].items()):
            records.append({'deployment_id':r['deployment_id'],'selection_order':r['hbench_selection_order'],
                            'query_id':qid,'selected_read_candidate':route.get('candidate_id'),
                            'read_plan_id':route.get('plan_id'),
                            'required_write_plan_ids':route.get('all_required_write_plans',[]),
                            'status':route['status'], 'operational_coverage_proven':False})
    execution=[]
    for r in chosen:
        execution.append({
            'deployment_id':r['deployment_id'], 'candidate_ids':';'.join(r['tables']),
            'cql_ddl_paths':';'.join(f'outputs/tpcx_iot_stage4/cql/schemas/{cid}.cql' for cid in r['tables']),
            'read_candidate_id':r['route']['Q_READ_2_WINDOWS']['candidate_id'],
            'read_plan_id':r['route']['Q_READ_2_WINDOWS']['plan_id'],
            'write_plan_ids':';'.join(r['route']['Q_INGEST']['all_required_write_plans']),
            'stage9_validation_gate':'BLOCKED_PENDING_OFFICIAL_KIT_AND_LIVE_CASSANDRA_VALIDATION',
        })
    output_dir.mkdir(parents=True,exist_ok=True)
    write_json(output_dir/OUTNAMES[0],canon)
    write_json(output_dir/OUTNAMES[1],coverage)
    write_json(output_dir/OUTNAMES[2],sorted(combos,key=lambda x:x['deployment_id']))
    write_json(output_dir/OUTNAMES[3],{
        'objective_axes':list(AXES), 'normalizer':'frozen_standalone_TS0_structural_proxy',
        'control_objectives':controls[0]['objectives'],
        'pareto_deployment_ids':[r['deployment_id'] for r in front],
        'dominated_deployment_ids':dominated,
        'exact_enumeration':True,'latency_not_used':True,
        'preselected_only_conditional_coverage':True,
        'pareto_rows':front,
    })
    write_json(output_dir/OUTNAMES[4],chosen)
    write_json(output_dir/OUTNAMES[5],records)
    def write_csv(filename:str, rows:list[dict], fields:list[str]):
        with (output_dir/filename).open('w',encoding='utf-8',newline='') as f:
            writer=csv.DictWriter(f,fieldnames=fields);writer.writeheader();writer.writerows(rows)
    write_csv(OUTNAMES[6],[{
        'selection_order':r['hbench_selection_order'], 'deployment_id':r['deployment_id'],
        'tables':';'.join(r['tables']),'read_plan_candidate':r['route']['Q_READ_2_WINDOWS']['candidate_id'],
        **r['objectives'],'normalized_proxy_score':r['normalized_proxy_score'],
        'coverage':'CONDITIONAL_OFFLINE_ONLY','real_benchmark_executed':'false',
    } for r in chosen], ['selection_order','deployment_id','tables','read_plan_candidate',*AXES,
                          'normalized_proxy_score','coverage','real_benchmark_executed'])
    write_csv(OUTNAMES[7],execution,['deployment_id','candidate_ids','cql_ddl_paths',
        'read_candidate_id','read_plan_id','write_plan_ids','stage9_validation_gate'])
    write_json(output_dir/OUTNAMES[9],{
        'enumerated_nonempty_subsets':exhaustive_count,
        'excluded_as_incompatible':exclusions,
        'excluded_as_dominated':dominated,
        'pareto_not_selected_under_k':sorted({x['deployment_id'] for x in front}-hbench_ids),
    })
    manifest={
        'stage':8,'engine':'Apache Cassandra',
        'benchmark':'TPCx-IoT 2.1.2 adapted research profile (kit predicate unverified)',
        'active_candidate_count':len(specs),'unique_canonical_candidate_count':len(canon),
        'query_originated_specification_count':sum(len(x['candidate_ids_by_family'][f])
            for x in read_json(root/'outputs/tpcx_iot_stage4/11_query_candidate_map.json')
            for f in x['candidate_ids_by_family']),
        'exact_nonempty_subsets_enumerated':exhaustive_count,
        'conditional_complete_covers':len(combos),'pareto_covers':len(front),
        'hbench_count':len(chosen),'top_k':p['top_k'],
        'hbench_ids':[r['deployment_id'] for r in chosen],
        'control_baseline_included':True,
        'selection_scores_are_hypothetical_structural_proxies':True,
        'query_frequency_weights_available':False,
        'reduction_denominator_warning':'Do not treat 31 combinatorial subsets as 31 deployable baseline alternatives; only 9 conditional valid covers.',
        'selection_uses_measured_results':False,
        'official_tpcx_iot_kit_verified':False,
        'live_cassandra_executed':False,
        'operational_coverage_proven':False,
        'distributed_atomicity_proven':False,
        'durable_recovery_proven':False,
        'read_and_write_coverage_status':'CONDITIONAL_OFFLINE_ONLY',
        'partition_warnings_not_hard_exclusions':True,
        'stage3_reexecuted':False,'activation_rules_changed':False,
        'source_hashes':source_hashes,
        'consolidation_policy_sha256':sha256(policy_file),
        'files':list(OUTNAMES),
        'next':'Stage9: verify official driver, execute CQL and live write/read correctness, then measure complete HBench configurations.',
    }
    write_json(output_dir/OUTNAMES[8],manifest)
    return manifest
