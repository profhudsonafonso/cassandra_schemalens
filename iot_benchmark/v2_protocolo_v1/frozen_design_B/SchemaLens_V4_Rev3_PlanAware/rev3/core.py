"""Rev3 offline plan enumeration, cost model, and schema+plan selection.

No Cassandra connections and no measured latency inputs. All physical candidate
activation/semantic coverage and materialization estimates come from the frozen
Rev2 parent. Rev3 considers executable query plans as a separate dimension.
"""
from __future__ import annotations
from collections import defaultdict
import hashlib
import json
from pathlib import Path
from v4_auto import selector as A
from v4_rev2.cost import REV2_POLICY

REVISION = 'SCHEMALENS_CASSANDRA_REV3_PLAN_AWARE_V1'
OUTPUT = 'outputs/v4_rev3_planaware_prebenchmark'
PLAN_MODES = frozenset({'TS0_RANGE_SCAN', 'TS0_FULL_SCAN', 'TS0_DESC_LIMIT1',
  'TS2U_RANGE_SCAN', 'TS2U_DESC_SPARSE', 'TS2U_FULL_SCAN',
  'TS2D_BUCKET_SCAN', 'TS2D_DESC_SPARSE', 'TS2D_FULL_SCAN', 'TS4_SNAPSHOT_LOOKUP'})
DAY = 86400000


def stable_id(query, candidate, mode):
    return f'{query}__{candidate}__{mode}'


def enumerated_modes(candidate, query):
    cid = candidate['candidate_id']; fam = candidate['family']
    qid = query['query_id']
    if fam == 'TS0':
        return ['TS0_FULL_SCAN', 'TS0_DESC_LIMIT1'] if qid == 'Q8' else ['TS0_RANGE_SCAN']
    if fam == 'TS2':
        root = 'TS2D' if cid == 'ts2_iot_wide' else 'TS2U'
        return [root + '_FULL_SCAN', root + '_DESC_SPARSE'] if qid == 'Q8' else [root + ('_BUCKET_SCAN' if root == 'TS2D' else '_RANGE_SCAN')]
    if fam == 'TS4' and qid == 'Q8':
        return ['TS4_SNAPSHOT_LOOKUP']
    return []


def validated_modes(candidate, query, base_coverage):
    """Independent activation and cross-query coverage, inherited from Rev2.

    Never silently treat an activation-only candidate as correct: the frozen
    Rev2 coverage must be DIRECT before its plan alternatives can be enumerated.
    """
    if base_coverage.get('route_status') != 'DIRECT':
        return []
    modes = enumerated_modes(candidate, query)
    cid = candidate['candidate_id']
    if 'TS0_DESC_LIMIT1' in modes:
        ddl = candidate.get('ddl', '').upper().replace(' ', '')
        if 'CLUSTERINGORDERBY(TIMESTAMP_MSDESC)' not in ddl:
            # This plan is safe only if descending clustering is proven by DDL.
            modes.remove('TS0_DESC_LIMIT1')
    return modes


def ts0_full_q8_rows(query, rows):
    wanted = {(d, s) for d in query['devices'] for s in query['sensors']}
    return sum((r['device_id'], r['sensor_name']) in wanted for r in rows)


def ts2_full_q8_rows(query, rows):
    devices = set(query['devices'])
    return len({(r['device_id'], r['timestamp']) for r in rows if r['device_id'] in devices})


def ts2_full_q8_requests(query, rows, bucket):
    if not bucket:
        return len(query['devices'])
    # Rev2 physical TS2-D executor loops over every populated global day
    # for each requested device, including a day where a device has no data.
    days = {r['timestamp'] // DAY for r in rows}
    return len(query['devices']) * len(days)


def plan_features(mode, query, candidate, rows, parent_proxy):
    """Override only the plan-specific features observed to be wrong in Rev2.

    This is not a calibration to measured latency. TS0 FULL_SCAN row count is
    derived from logical source cardinality; optimized Q8 counts one latest row
    per populated sensor partition. Cost coefficients stay Rev2 fixed.
    """
    features = {k:parent_proxy[k] for k in (
        'request_count_proxy', 'physical_rows_proxy', 'bucket_count_proxy',
        'logical_operand_count', 'extra_bucket_fanout_proxy', 'returned_bytes_proxy',
        'max_partition_bytes_proxy', 'partition_pressure_proxy')}
    qid = query['query_id']
    if qid == 'Q8' and mode == 'TS0_FULL_SCAN':
        features['physical_rows_proxy'] = ts0_full_q8_rows(query, rows)
        features['returned_bytes_proxy'] = features['physical_rows_proxy'] * REV2_POLICY['ts0_cell_bytes_proxy']
    elif qid == 'Q8' and mode == 'TS0_DESC_LIMIT1':
        observed = {(r['device_id'], r['sensor_name']) for r in rows}
        features['physical_rows_proxy'] = sum((d,s) in observed for d in query['devices'] for s in query['sensors'])
        features['returned_bytes_proxy'] = features['physical_rows_proxy'] * REV2_POLICY['ts0_cell_bytes_proxy']
    elif qid == 'Q8' and mode.endswith('_FULL_SCAN'):
        bucket = mode.startswith('TS2D_')
        features['request_count_proxy'] = ts2_full_q8_requests(query, rows, bucket)
        features['physical_rows_proxy'] = ts2_full_q8_rows(query, rows)
        projected = len(query['sensors'])
        features['returned_bytes_proxy'] = features['physical_rows_proxy'] * (
            REV2_POLICY['row_header_bytes_proxy'] + REV2_POLICY['metric_double_bytes_proxy'] * projected)
        features['bucket_count_proxy'] = features['request_count_proxy'] if bucket else 0
        features['extra_bucket_fanout_proxy'] = max(0,features['bucket_count_proxy']-len(query['devices']))
    elif mode == 'TS0_RANGE_SCAN' and qid == 'Q8':
        raise ValueError('Unsupported Q8 TS0 plan contract')
    if any(features[k] < 0 for k in ['request_count_proxy','physical_rows_proxy','returned_bytes_proxy']):
        raise ValueError('Negative physical feature')
    return features


def analytic_cost(features, policy=REV2_POLICY):
    return round(policy['request_cost_units']*features['request_count_proxy']
        + policy['physical_row_cost_units']*features['physical_rows_proxy']
        + policy['returned_kib_cost_units']*features['returned_bytes_proxy']/1024
        + policy['extra_bucket_coordination_units']*features['extra_bucket_fanout_proxy']
        + policy['partition_pressure_cost_units']*features['partition_pressure_proxy'], 8)


def make_plan_catalog(candidates, queries, coverage, readproxies, rows, policy=REV2_POLICY):
    by_cid={x['candidate_id']:x for x in candidates}
    by_q={q['query_id']:q for q in queries}
    bycov={(x['query_id'],x['candidate_id']):x for x in coverage}
    byread={(x['query_id'],x['candidate_id']):x for x in readproxies}
    records=[]
    for (qid,cid),cov in sorted(bycov.items()):
        if cid not in by_cid or qid not in by_q or (qid,cid) not in byread:
            continue
        candidate=by_cid[cid];q=by_q[qid]
        for mode in validated_modes(candidate,q,cov):
            features=plan_features(mode,q,candidate,rows,byread[qid,cid])
            plan={'plan_id':stable_id(qid,cid,mode),'query_id':qid,'candidate_id':cid,
                  'family':candidate['family'],'physical_plan':mode,
                  'route_status':'DIRECT','semantic_verification':'INHERITED_REV2_LOGICAL_ORACLE',
                  'activated_for_query':cov.get('activated_for_query'),
                  'cross_query_reuse':cov.get('cross_query_reuse'),
                  'executor_contract':mode,'runtime_verified':False,
                  'preselection_only':True, **features,
                  'analytic_read_proxy':analytic_cost(features,policy)}
            if mode=='TS0_DESC_LIMIT1':
                plan['preconditions']=['TS0 clustering timestamp DESC',
                    'non-null observation rows from original loader',
                    'full-scan fallback if unexpected NULL is encountered']
            elif 'DESC_SPARSE' in mode:
                plan['preconditions']=['timestamp DESC',
                    'find last non-null separately for each projected sensor']
            records.append(plan)
    if not records: raise ValueError('No executable plans generated')
    ids=[p['plan_id'] for p in records]
    if len(set(ids))!=len(ids):raise ValueError('Duplicate plan IDs')
    return records


def select_schema_and_plan(candidates, queries, coverage, catalog, storage, writes, policy):
    """Stage8 selects cheapest executable plan per route, then Pareto + diversity.

    Uses existing Rev2 Stage8 search-covers logic, but passes the selected
    concrete executor plan ID and its cost (not the activation-only route ID).
    """
    best={}
    for p in catalog:
        key=(p['query_id'],p['candidate_id'])
        if key not in best or (p['analytic_read_proxy'],p['plan_id']) < (best[key]['analytic_read_proxy'],best[key]['plan_id']):
            best[key]=p
    choice_plans=[];choice_cost=[]
    by_cid={x['candidate_id']:x for x in candidates}
    for (qid,cid),p in sorted(best.items()):
        choice_plans.append({'query_id':qid,'candidate_id':cid,'family':by_cid[cid]['family'],
                             'plan_id':p['plan_id'],'route_status':'DIRECT',
                             'method':p['physical_plan']+'; executable plan from explicit catalog'})
        choice_cost.append(p)
    allcovers,selected,pareto=A.search_covers(candidates,choice_plans,storage,writes,choice_cost,queries,policy)
    for h in allcovers:
        for qid,route in h['query_routes'].items():
            plan=next(x for x in choice_cost if x['plan_id']==route['plan_id'])
            route['physical_plan']=plan['physical_plan']
            route['executor_contract']=plan['executor_contract']
            route['read_proxy']=plan['analytic_read_proxy']
        signature=json.dumps({'revision':REVISION,'candidates':h['candidates'],
            'routes':{q:r['plan_id'] for q,r in h['query_routes'].items()}},sort_keys=True)
        h['deployment_id']='R3H_'+hashlib.sha256(signature.encode()).hexdigest()[:12]
        h['selection_policy_revision']=REVISION
        h['measured_performance_used']=False
        h['selection_scope']='EXPLORATORY_POST_E5_PLAN_AWARE_SELECTION'
    if len(set(h['deployment_id'] for h in allcovers))!=len(allcovers):raise AssertionError('ID collision')
    return allcovers,selected,pareto
