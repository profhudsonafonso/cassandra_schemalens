"""V4 auto physical candidate/coverage/plan/cost/selection (OFFLINE ONLY).

Never reads any Stage9 or diagnosis results, no Cassandra import, no performance data.
All preselection coefficients come from the V3 frozen policy, not observed latencies.
"""
from __future__ import annotations
from collections import Counter, defaultdict
from dataclasses import dataclass
from itertools import combinations
import csv
import hashlib
import json
import math
from pathlib import Path

DAY=86_400_000
CANONICAL=('ts0_iot_raw','ts2_iot_wide_unbucketed','ts2_iot_wide')
LATEST='ts4_iot_latest'
EXPECTED_FROZEN='2c4f46c70fc272046b42d1c7a8e6fc7c20c4ca9d8b63cef707ff7f89fa3fa6eb'
STAGES={'stage4':'10_candidate_specs.json','stage5':'15_query_plans.json',
        'stage6':'18_write_plans.json','stage7':'23_read_proxies.json',
        'stage8':'28_hbench_frozen.json'}
METHODOLOGY='V4_AUTO_CANONICAL_CROSS_QUERY_REV1'


def jread(path):return json.loads(Path(path).read_text(encoding='utf-8'))

def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def dump(path,obj):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj,sort_keys=True,indent=2,ensure_ascii=False,allow_nan=False)+'\n',encoding='utf-8')

def csv_out(path,rows,columns):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=columns,extrasaction='ignore')
        w.writeheader();w.writerows(rows)

def verify_old(exp):
    """Read frozen inputs ONLY; reject altered V3 files and unverified freeze."""
    exp=Path(exp).resolve()
    f=exp/'outputs/stage8/28_hbench_frozen.json'
    if digest(f)!=EXPECTED_FROZEN:raise ValueError('Wrong V3 HBench SHA-256; refusing V4 auto')
    frozen=jread(exp/'outputs/stage8/32_frozen_manifest.json')
    if frozen.get('frozen_hbench_sha256')!=EXPECTED_FROZEN:raise ValueError('Invalid Stage8 provenance')
    known=frozen['inputs_sha256']
    for rel,val in known.items():
        path=exp/rel
        if not path.is_file() or digest(path)!=val:raise ValueError(f'V3 stage input modified: {rel}')
    tracked=set(known)|{'outputs/stage8/28_hbench_frozen.json','outputs/stage8/32_frozen_manifest.json',
                        'outputs/stage8/30_hbench_summary.csv'}
    return {str(x):digest(exp/x) for x in sorted(tracked)}


def verified_native(exp,qs):
    origins=[q.get('parameter_origin','') for q in qs]
    if len(qs)!=12 or {q.get('query_id') for q in qs}!={f'Q{i}' for i in range(1,13)}:
        raise ValueError('Native experiment requires exactly one object for each Q1-Q12')
    if any('sample from official GenerateQueryWorkLoad Java API' not in x for x in origins):
        raise ValueError('Illustrative/unaudited query objects forbidden in V4 native selection')
    hashes={q.get('capture_sha256') for q in qs}
    if len(hashes)!=1 or not next(iter(hashes)) or len(next(iter(hashes)))!=64:
        raise ValueError('Native capture provenance missing or mixed')
    candidates=list(Path(exp).parent.glob('native_queries_q12fixed_*.jsonl'))
    matched=[str(p) for p in candidates if digest(p)==next(iter(hashes))]
    if len(matched)!=1:raise ValueError('Java capture file not uniquely matched by SHA-256')
    return {'path':matched[0],'sha256':next(iter(hashes))}


def candidate_catalog(specs,activation,rows,policy):
    """Stage4 V4: canonical role choice is new; physical designs from evidenced V3 A1."""
    activated={f for item in activation for f in item['ts_families']}
    eligible=[];rejected=[]
    perdev=Counter((r['device_id'],r['timestamp']) for r in rows)
    maxwide=max(Counter(k[0] for k in perdev).values(),default=0)
    byid={s['candidate_id']:s for s in specs}
    if len(byid)!=len(specs):raise ValueError('Duplicate V3 physical candidates')
    for cid in (*CANONICAL,LATEST):
        s=byid.get(cid)
        if not s:
            rejected.append({'candidate_id':cid,'reason':'not instantiated by activated family and Stage4 rules'})
            continue
        if s['family'] not in activated:
            rejected.append({'candidate_id':cid,'reason':'physical candidate not justified by activation'})
            continue
        reason=''
        if cid=='ts2_iot_wide_unbucketed':
            limit=s.get('max_rows_per_device_guard')
            if not isinstance(limit,int) or maxwide>limit:
                reason=f'estimated rows/device {maxwide} violates original guard {limit}'
        if reason:
            rejected.append({'candidate_id':cid,'reason':reason});continue
        eligible.append({**s,'v4_role':'optional_latest' if cid==LATEST else 'eligible_canonical',
                         'v4_evidence': 'reused V3 activated candidate; no new TS family',
                         'v4_estimated_max_rows_per_device':maxwide})
    if not any(s['candidate_id'] in CANONICAL for s in eligible):
        raise ValueError('No physically admissible canonical source')
    return eligible,rejected


def wide_state(rows):
    result=defaultdict(dict)
    for r in rows:result[(r['device_id'],r['timestamp'])][r['sensor_name']]=r['value_double']
    return result


def physical_reads(rows,qs,eligible,oracle,logical_compare,requests_for,activation=None):
    """Stage5: verify per-query *cross* coverage, not only activated Q-family pairs.

    Build each logical projection from candidate data, and validate against
    original oracle; Q12 uses independent child operand intervals.
    """
    wide=wide_state(rows)
    reconstructed=[{'device_id':d,'timestamp':t,'sensor_name':s,'value_double':v}
                   for (d,t),metrics in wide.items() for s,v in metrics.items()]
    if Counter((r['device_id'],r['sensor_name'],r['timestamp'],r['value_double']) for r in reconstructed)!=\
       Counter((r['device_id'],r['sensor_name'],r['timestamp'],r['value_double']) for r in rows):
        raise ValueError('TS2 cannot reconstruct the complete canonical dataset')
    latest={}
    for r in rows:
        key=r['device_id'],r['sensor_name']
        if key not in latest or r['timestamp']>latest[key]['timestamp']:latest[key]=r
    activated_by_q={a['query_id']:set(a['ts_families']) for a in (activation or []) if 'query_id' in a}
    plans=[]
    for cand in eligible:
        cid,fam=cand['candidate_id'],cand['family']
        for q in qs:
            qid=q['query_id']
            if cid==LATEST and qid!='Q8':continue
            if cid==LATEST:
                records=[latest[d,s] for d in q['devices'] for s in q['sensors'] if (d,s) in latest]
                method='precomputed_latest_snapshot; concurrent updates not supported'
            else:
                found={}
                for d,sensors,start,end in requests_for(q):
                    for r in (reconstructed if fam=='TS2' else rows):
                        if r['device_id']!=d or r['sensor_name'] not in sensors:continue
                        if start is not None and not start<=r['timestamp']<end:continue
                        found[(r['device_id'],r['sensor_name'],r['timestamp'])]=r
                records=list(found.values())
                method='canonical point reconstruction' if fam=='TS2' else 'direct metric-point lookup'
                if qid=='Q8' and fam=='TS2':method='descending sparse-null-aware latest scan'
            expected=oracle(rows,q)
            got=oracle(records,q)
            ok=logical_compare(got,expected)
            is_cross=qid in activated_by_q and fam not in activated_by_q[qid]
            plans.append({'plan_id':qid+'__'+cid,'query_id':qid,'candidate_id':cid,
                          'family':fam,'route_status':'DIRECT' if ok else 'INELIGIBLE',
                          'verification':'IN_MEMORY_ORACLE_ONLY','method':method,
                          'activation_provenance':'materialized from Stage A1; query route validated independently',
                          'cross_query_reuse':is_cross,
                          'activated_for_query':(fam in activated_by_q[qid]) if qid in activated_by_q else None,
                          'runtime_cql_verified':False,
                          'sql_semantics_verified_official':False})
            if not ok and cid in CANONICAL:raise AssertionError(f'Failed canonical read equivalence {cid}/{qid}')
    return plans


def derive_analytic(rows,queries,eligible,plans,policy,requests_for):
    """Stage6 and Stage7: physical counts and plan-aware proxies; no measured latency."""
    byid={s['candidate_id']:s for s in eligible}
    perdevdays=defaultdict(set);ts=defaultdict(set);data_by_key=defaultdict(set)
    for r in rows:
        d=r['device_id'];t=r['timestamp'];s=r['sensor_name']
        perdevdays[d].add(t//DAY);ts[d].add(t);data_by_key[(d,t)].add(s)
    sensors={r['sensor_name'] for r in rows}
    metric_count=len(rows);wide_count=len(data_by_key);latest_count=len({(r['device_id'],r['sensor_name']) for r in rows})
    nbytes={}
    write_plans=[];storage=[]
    for s in eligible:
        cid=s['candidate_id'];fam=s['family']
        count=latest_count if fam=='TS4' else wide_count if fam=='TS2' else metric_count
        avgbytes=64 if fam in ('TS0','TS4') else 56+16*len(sensors)
        nbytes[cid]=count*avgbytes
        storage.append({'candidate_id':cid,'estimated_rows':count,'estimated_bytes_proxy':nbytes[cid],
                        'units':'analytical uncompressed bytes proxy, not disk bytes'})
        write_plans.append({'candidate_id':cid,'physical_writes_proxy':count,
                            'canonical_candidate':cid in CANONICAL,
                            'incremental_maintenance':'UNVALIDATED_FOR_CONCURRENT_UPDATES' if fam=='TS4' else 'BATCH_SNAPSHOT_ONLY',
                            'source_of_truth':'self' if cid in CANONICAL else 'chosen canonical source',
                            'guard':s.get('guard','')})
    read_cost=[]
    for plan in plans:
        if plan['route_status']=='INELIGIBLE':continue
        q=next(x for x in queries if x['query_id']==plan['query_id'])
        cid=plan['candidate_id'];family=plan['family'];bucket=byid[cid].get('partition_variant')=='daily'
        if family=='TS4':
            requests=len(q['devices'])*len(q['sensors']);physical_rows=requests
        elif q['query_id']=='Q8':
            # Latest-value scan is monotonic by DESC clustering, one per device for TS2.
            requests=0;physical_rows=0
            for d in q['devices']:
                allts=sorted(ts[d],reverse=True)
                missing=set(q['sensors'])
                visited=0
                for t in allts:
                    visited+=1
                    missing.difference_update(data_by_key[d,t])
                    if not missing:break
                if family=='TS0':
                    requests+=len(q['sensors']);physical_rows+=len(q['sensors'])
                else:
                    physical_rows+=visited
                    requests+=1 if not bucket else len({t//DAY for t in allts[:visited]})
        else:
            requests=0;physical_rows=0
            for d,projection,start,end in requests_for(q):
                matches=[t for t in ts[d] if start is None or start<=t<end]
                if family=='TS0':
                    requests+=len(projection)
                    for t in matches:physical_rows+=len(data_by_key[d,t].intersection(projection))
                else:
                    if bucket:
                        days=perdevdays[d] if start is None else set(range(start//DAY,(end-1)//DAY+1))
                        requests+=len(days)
                    else:requests+=1
                    physical_rows+=len(matches)
        w=policy['read_proxy_weights']
        proxy=w['request_count']*requests+w['logical_rows']*physical_rows
        read_cost.append({'plan_id':plan['plan_id'],'query_id':q['query_id'],'candidate_id':cid,
                          'request_count_proxy':requests,'physical_rows_proxy':physical_rows,
                          'analytic_read_proxy':round(proxy,6),'rule':'original V3 policy coefficients applied to physical CQL plan counts'})
    return write_plans,storage,read_cost


def is_dominated(a,b):
    return all(b['analytic_cost'][key]<=a['analytic_cost'][key]+1e-8 for key in ('read','storage','write')) and any(
        b['analytic_cost'][key]<a['analytic_cost'][key]-1e-8 for key in ('read','storage','write'))


def search_covers(eligible,plans,storage,writes,read_cost,queries,policy):
    """Stage8: enumerate *all* canonical choices x optional TS4; Pareto + diverse roots."""
    byid={s['candidate_id']:s for s in eligible}
    canonical=[x for x in CANONICAL if x in byid]
    latest_allowed=LATEST in byid
    good={(p['query_id'],p['candidate_id']):p for p in plans if p['route_status']=='DIRECT'}
    rc={(x['query_id'],x['candidate_id']):x for x in read_cost}
    sc={x['candidate_id']:x['estimated_bytes_proxy'] for x in storage}
    wc={x['candidate_id']:x['physical_writes_proxy'] for x in writes}
    allcovers=[]
    for source in canonical:
        for latest_enabled in ([False,True] if latest_allowed else [False]):
            chosen=[source]+([LATEST] if latest_enabled else [])
            routes={};values=[]
            for q in queries:
                qid=q['query_id'];options=[cid for cid in chosen if (qid,cid) in good]
                if not options:break
                choice=min(options,key=lambda cid:(rc[qid,cid]['analytic_read_proxy'],cid))
                pp=good[qid,choice]
                routes[qid]={'candidate_id':choice,'family':byid[choice]['family'],
                             'plan_id':pp['plan_id'],'route_status':pp['route_status'],
                             'reason':pp['method']}
                values.append(rc[qid,choice]['analytic_read_proxy'])
            if len(routes)!=len(queries):continue
            costs={'read':sum(values)/len(values),'storage':sum(sc[c] for c in chosen),
                   'write':sum(wc[c] for c in chosen)}
            signature=METHODOLOGY+'|'+','.join(chosen)+'|'+digest_bytes(json.dumps(routes,sort_keys=True).encode())
            did='V4H_'+hashlib.sha256(signature.encode()).hexdigest()[:12]
            allcovers.append({'deployment_id':did,'canonical_source':source,'candidates':chosen,
                              'families':[byid[x]['family'] for x in chosen],
                              'query_routes':routes,'analytic_cost':costs,
                              'selection_status':'NOT_YET_RANKED',
                              'semantically_complete_offline':True,
                              'measured_performance_used':False})
    if not allcovers:raise ValueError('No full native V4 cover')
    mins={k:min(h['analytic_cost'][k] for h in allcovers) for k in ('read','storage','write')}
    w=policy['preselect_weights']
    for h in allcovers:
        h['preselection_score']=round(sum(w[k]*h['analytic_cost'][k]/max(1e-9,mins[k]) for k in mins),8)
    pareto=[h for h in allcovers if not any(is_dominated(h,k) for k in allcovers if k is not h)]
    rank=lambda h:(h['preselection_score'],len(h['candidates']),h['deployment_id'])
    chosen=sorted(pareto,key=rank)[:policy['top_k']]
    # Conservative, explicitly declared structural-diversity safeguard.
    # It preserves an alternative canonical representation even if optimistic
    # proxies accidentally dominate it. Such additional designs are labeled.
    for src in canonical:
        if len(chosen)>=policy['top_k']:break
        if any(x['canonical_source']==src for x in chosen):continue
        pool=sorted((x for x in allcovers if x['canonical_source']==src),key=rank)
        if pool:
            extra=pool[0];chosen.append(extra)
    chosen=sorted(chosen,key=rank)
    pareto_ids={h['deployment_id'] for h in pareto}
    for h in allcovers:h['selection_status']='PARETO' if h['deployment_id'] in pareto_ids else 'DOMINATED_ANALYTICALLY'
    for h in chosen:h['selection_reason']='PARETO_FRONT' if h['deployment_id'] in pareto_ids else 'STRUCTURAL_CANONICAL_DIVERSITY_SAFEGUARD'
    return allcovers,chosen,pareto


def digest_bytes(content):return hashlib.sha256(content).hexdigest()
