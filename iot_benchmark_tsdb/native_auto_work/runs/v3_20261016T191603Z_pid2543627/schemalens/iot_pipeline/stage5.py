"""Stage 5: conditional executable-read contracts. Never infer coverage from activation alone."""
from .shared import *

RAW={'Q1','Q2','Q3','Q4','Q5','Q6','Q7','Q8','Q9','Q10','Q11','Q12'}
BUCKETED={'Q2','Q3','Q9','Q10','Q12'}
WIDE={'Q1','Q2','Q3','Q9','Q10','Q12'}

def route_family(q, family,p):
    qid=q['query_id']
    if family=='TS0':return ('DIRECT','read canonical raw points, client aggregate/set/direction; all partitions scanned when needed')
    if family=='TS1' and qid in BUCKETED:return ('DIRECT','enumerate UTC day buckets for every queried device/sensor')
    if family=='TS2' and qid in WIDE:return ('DIRECT','enumerate device/day buckets, project requested columns with sparse NULL semantics')
    if family=='TS4' and qid=='Q8':return ('CONDITIONAL','Q8 uses global latest from TableStrategy last/last_by; exact null and sensor-row semantics require official verification')
    if family=='TS5' and qid in {'Q7','Q11'}:
        g=q.get('group_by_ms');start=q.get('start_ms');end=q.get('end_ms_exclusive')
        if g==p['rollup_window_ms'] and start%g==0 and end%g==0:
            return ('CONDITIONAL','complete aligned rollup windows; client must validate function and ordered output')
        return ('INELIGIBLE','window boundaries unaligned: TS5 rollup cannot answer exactly without raw partial-window reads')
    return ('INELIGIBLE','no exact physical read path for this query/family in current adapter')

def run():
    p=require_policy();specs=read(path4(4,'10_candidate_specs.json'))
    queries=read(inputs()['queries']);amap=read(path4(4,'11_activated_candidate_map.json'))
    byid={s['candidate_id']:s for s in specs};byq={q['query_id']:q for q in queries};plans=[]
    for row in amap:
        cid=row['candidate_id'];qid=row['query_id'];
        if cid is None:continue
        s=byid[cid];status,reason=route_family(byq[qid],s['family'],p)
        plans.append({'plan_id':f'{qid}__{cid}','query_id':qid,'candidate_id':cid,'family':s['family'],
                      'route_status':status,'guard':reason+'; '+s['guard'],'partition_variant':s.get('partition_variant'),
                      'physical_query_executed':False,'sql_semantics_verified_official':False})
    out=path4(5,'15_query_plans.json');write(out,plans)
    csv_write(path4(5,'16_coverage_matrix.csv'),plans,['plan_id','query_id','candidate_id','family','route_status','guard'])
    coverage={q:{x['candidate_id'] for x in plans if x['query_id']==q and x['route_status']!='INELIGIBLE'} for q in byq}
    if any(not x for x in coverage.values()):raise ValueError(f'Uncovered query classes: {coverage}')
    write(path4(5,'17_manifest.json'),manifest(5,{'stage4_specs':path4(4,'10_candidate_specs.json'), 'stage4_mapping':path4(4,'11_activated_candidate_map.json'),**inputs()},
                   'STRUCTURAL_COVERAGE_ONLY',{'classes':len(byq),'plans':len(plans),
                       'eligible_plan_links':sum(x['route_status']!='INELIGIBLE' for x in plans),
                       'conditional_plans':sum(x['route_status']=='CONDITIONAL' for x in plans)}))
    return plans
