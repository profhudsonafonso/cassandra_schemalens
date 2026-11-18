"""Stage 6: table-specific insert and maintenance plans. Aggregates rebuilt from canonical observations."""
from .shared import *

def run():
    specs=read(path4(4,'10_candidate_specs.json')); p=require_policy()
    ops={
      'TS0':('INSERT INTO ts0_iot_raw (device_id,sensor_name,timestamp_ms,value_double) VALUES (?,?,?,?)','one CQL row per non-null measurement'),
      'TS1':('INSERT INTO ts1_iot_bucket (device_id,sensor_name,bucket_day,timestamp_ms,value_double) VALUES (?,?,?,?,?)','one CQL row per non-null measurement and UTC day'),
      'TS2':('INSERT INTO ts2_iot_wide (device_id,bucket_day,timestamp_ms,columns...) VALUES (?,?,?,...)','one CQL row per device/day/timestamp; sparse columns skipped'),
      'TS4':('INSERT INTO ts4_iot_latest (device_id,sensor_name,latest_timestamp_ms,value_double) VALUES (?,?,?,?)','latest materialized after snapshot canonical load; out-of-order and replay handled by deterministic rebuild'),
      'TS5':('INSERT INTO ts5_iot_rollup (device_id,sensor_name,bucket_day,window_start_ms,value_sum,sample_count,minimum,maximum) VALUES (?,?,?,?,?,?,?,?)','rebuild sum/count/min/max from canonical observations; no unsafe counter deltas')
    }
    plans=[{'candidate_id':s['candidate_id'],'family':s['family'],'insert_template':(
            'INSERT INTO ts2_iot_wide_unbucketed (device_id,timestamp_ms,columns...) VALUES (?,?,...)'
            if s['family']=='TS2' and s.get('partition_variant')=='unbucketed' else ops[s['family']][0]),
            'maintenance':ops[s['family']][1],'partition_variant':s.get('partition_variant'),'source_of_truth':'canonical TS0',
            'incremental_write_support':s['family'] in ('TS0','TS1','TS2'),
            'live_write_status':'BATCH_LOAD_IMPLEMENTED; concurrent correction/update protocol not implemented'} for s in specs]
    write(path4(6,'18_write_plans.json'),plans)
    links=[]
    for q in read(path4(5,'15_query_plans.json')):
        if q['route_status']!='INELIGIBLE':links.append({'plan_id':q['plan_id'],'write_plan':q['candidate_id'],'query_id':q['query_id']})
    write(path4(6,'19_write_read_links.json'),links)
    write(path4(6,'20_manifest.json'),manifest(6,{'stage5':path4(5,'15_query_plans.json'),'stage4':path4(4,'10_candidate_specs.json'),'policy':POLICY},
          'WRITE_PLANS_GENERATED_NOT_LIVE_TESTED',{'write_plans':len(plans),'compatible_links':len(links)}))
    return plans
