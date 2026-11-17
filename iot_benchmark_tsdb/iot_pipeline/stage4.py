"""Stage 4: evidence-activated physical candidates, with auditable TS2 partition variants.

Stage4 Rev2 introduces no new logical family. A wide TS2 may have either a bounded
UTC-day partition or an unbucketed device partition. Both follow the ORIGINAL TS2
activation. The unbucketed variant has an explicit, predeclared size guard.
"""
from collections import Counter
from .shared import *

DAY_MS=86_400_000

def ts2_unbucketed(base):
    """Generate the physical DDL once from the A1 wide schema (no guessed sensor types)."""
    old=base['ddl']
    old=old.replace('bucket_day date, ', '')
    old=old.replace('PRIMARY KEY ((device_id,bucket_day),timestamp_ms)',
                    'PRIMARY KEY ((device_id),timestamp_ms)')
    old=old.replace(base['table_name'], 'ts2_iot_wide_unbucketed')
    if 'bucket_day' in old or 'PRIMARY KEY ((device_id),timestamp_ms)' not in old:
        raise ValueError('Unexpected A1 TS2 DDL: refusing unsafe rewrite')
    return old

def instantiate(active,blueprints,policy,rows):
    """Pure Stage 4 candidate generation; no costs, benchmarks or Stage9 data."""
    eligible={f for row in active for f in row['ts_families']}
    specs=[];mapping=[];missing=[]
    bydev=Counter(r['device_id'] for r in rows)
    m1_evidence=any('TS2' in row['ts_families'] and 'M1' in row['modifiers'] for row in active)
    for b in blueprints:
        family=b['family']
        if family not in eligible or family not in policy['supported_physical_families']:
            continue
        if family!='TS2':
            specs.append({'candidate_id':b['candidate_id'],'family':family,
                          'table':b['table_name'],'ddl':b['ddl'],'guard':b['guard'],
                          'partition_variant':'unchanged',
                          'status':'SCHEMA_INSTANTIATED_NOT_DEPLOYED'})
            continue
        allowed=policy['ts2_physical_variants']
        if len(allowed)!=len(set(allowed)) or not set(allowed)<= {'daily','unbucketed'}:
            raise ValueError('Invalid TS2 physical design-space variants')
        for variant in allowed:
            if variant=='daily':
                if not m1_evidence:
                    continue  # M1 cannot be inferred from physical design alone.
                ddl=b['ddl'];name=b['table_name'];ident=b['candidate_id']
                guard='M1 evidenced in workload; enumerate all UTC day partitions in time range'
            else:
                if max(bydev.values(),default=0)>policy['max_unbucketed_rows_per_device']:
                    continue
                ddl=ts2_unbucketed(b);name='ts2_iot_wide_unbucketed';ident=name
                guard=('one device partition with bounded estimated rows; '
                       'unbucketed partition may become too large under growth')
            specs.append({'candidate_id':ident,'family':'TS2','table':name,'ddl':ddl,
                          'guard':guard,'partition_variant':variant,
                          'exclusive_group':'TS2_partition_granularity',
                          'max_rows_per_device_guard':policy['max_unbucketed_rows_per_device'] if variant=='unbucketed' else None,
                          'status':'SCHEMA_INSTANTIATED_NOT_DEPLOYED'})
    for row in active:
        for family in row['ts_families']:
            subset=[s for s in specs if s['family']==family]
            if not subset:
                mapping.append({'query_id':row['query_id'],'family':family,
                                'candidate_id':None,'state':'NOT_MATERIALIZED',
                                'explanation':'Activation evidence, no physical candidate'})
                missing.append({'query_id':row['query_id'],'family':family})
            else:
                for c in subset:
                    mapping.append({'query_id':row['query_id'],'family':family,
                                    'candidate_id':c['candidate_id'],
                                    'state':'PHYSICAL_CANDIDATE',
                                    'explanation':'Activated; query coverage not yet validated'})
    if len(specs)!=len({s['candidate_id'] for s in specs}):
        raise ValueError('Duplicate candidate identifiers')
    return specs,mapping,missing

def run():
    files=inputs();p=require_policy();active=read(files['activation']);blueprints=read(files['blueprints'])
    specs,mapping,missing=instantiate(active,blueprints,p,read_rows(files['native']))
    write(path4(4,'10_candidate_specs.json'),specs)
    write(path4(4,'11_activated_candidate_map.json'),mapping)
    path4(4,'12_schema_blueprints.cql').write_text(
        '-- Rev2: analytical design space only; no Cassandra deployed.\n'+
        '\n'.join(s['ddl'] for s in specs)+'\n',encoding='utf-8')
    write(path4(4,'14_manifest.json'),manifest(4,{**files,'policy':POLICY},
          'SCHEMA_INSTANTIATED_NOT_DEPLOYED',
          {'candidate_count':len(specs),'activation_links':len(mapping),
           'unmaterialized_activations':missing,
           'ts2_variants':[s['partition_variant'] for s in specs if s['family']=='TS2'],
           'stage9_benchmark_used_in_design_space':False,
           'current_workload_is_illustrative':True}))
    return specs,mapping
