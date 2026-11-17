"""Stage 7: prebenchmark analytic proxies only (never trained on measured latency)."""
from .shared import *
from collections import defaultdict
from datetime import date,datetime,timezone

def run():
    p=require_policy();sp=read(path4(4,'10_candidate_specs.json'));plans=read(path4(5,'15_query_plans.json'))
    query={q['query_id']:q for q in read(inputs()['queries'])};rows=read_rows(inputs()['native'])
    sensors={r['sensor_name'] for r in rows};groups=defaultdict(list)
    for r in rows:groups[(r['device_id'],r['timestamp']//86400000,r['timestamp'])].append(r)
    storage=[];write_proxies=[]
    n=len(rows)
    for s in sp:
        fam=s['family']; distinct=len(groups) if fam=='TS2' else len(set((r['device_id'],r['sensor_name']) for r in rows)) if fam=='TS4' else len(set((r['device_id'],r['sensor_name'],r['timestamp']//p['rollup_window_ms']) for r in rows)) if fam=='TS5' else n
        avgbytes={'TS0':64,'TS1':72,'TS2':56+16*len(sensors),'TS4':64,'TS5':88}[fam]
        storage.append({'candidate_id':s['candidate_id'],'family':fam,'estimated_rows':distinct,'estimated_bytes_proxy':distinct*avgbytes,
                        'estimate_kind':'uncompressed logical byte proxy, NOT Cassandra disk storage'})
        write_proxies.append({'candidate_id':s['candidate_id'],'family':fam,'write_keys_proxy':distinct,
                      'estimated_rebuild_pressure':float(n) if fam in {'TS4','TS5'} else 0,
                      'estimate_kind':'batch materialization/write-key proxy; no measured I/O'})
    reads=[]
    for plan in plans:
        q=query[plan['query_id']];cid=plan['candidate_id'];fam=plan['family']
        if plan['route_status']=='INELIGIBLE':continue
        request_sensors=len(q['sensors']);requests=len(q['devices'])*request_sensors
        if fam=='TS2':requests=(len(q['devices']) if next(x for x in sp if x['candidate_id']==cid).get('partition_variant')=='unbucketed'
                                      else len(q['devices'])*len(range(q['start_ms']//86400000,(q['end_ms_exclusive']-1)//86400000+1)))
        if fam=='TS1':requests*=max(1,(q['end_ms_exclusive']-q['start_ms']+86399999)//86400000)
        if fam=='TS4':requests=len(q['devices'])*request_sensors
        if fam=='TS5':requests=len(q['devices'])*request_sensors
        if q['query_id'] in {'Q5','Q8'}:relevant=n
        else:relevant=sum(1 for r in rows if r['device_id'] in q['devices'] and r['sensor_name'] in q['sensors'] and (q['query_id']=='Q1' and r['timestamp']==q['point_ms'] or q['query_id']!='Q1' and q['start_ms']<=r['timestamp']<q['end_ms_exclusive']))
        if q['query_id']=='Q12':requests*=2
        estimated_rows=max(1,relevant)*p['read_family_factors'][fam]
        reads.append({'plan_id':plan['plan_id'],'query_id':plan['query_id'],'candidate_id':cid,
                      'request_count_proxy':requests,'logical_rows_proxy':round(estimated_rows,4),
                      'route_status':plan['route_status'],
                      'partition_variant':next(x for x in sp if x['candidate_id']==cid).get('partition_variant'),
                      'estimate_kind':'policy-driven, not observed latency'})
    write_json=path4(7,'22_write_proxies.json')
    write(path4(7,'21_storage_proxies.json'),storage);write(write_json,write_proxies)
    write(path4(7,'23_read_proxies.json'),reads)
    write(path4(7,'27_manifest.json'),manifest(7,{
        'physical':path4(4,'10_candidate_specs.json'),'coverage':path4(5,'15_query_plans.json'),
        'writes':path4(6,'18_write_plans.json'),'input':inputs()['native'],'policy':POLICY},
        'ANALYTIC_PROXIES_ONLY',{'estimated_physical_candidates':len(storage),'read_proxies':len(reads),
             'no_runtime_or_latency_observations_used':True}))
    return storage,write_proxies,reads
