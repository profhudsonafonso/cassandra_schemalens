"""DKE Algorithm 1 specification instantiation, Cassandra time-series Stage 4.

Consumes *unchanged* per-query Stage 3 matrices; does NOT recalculate activation,
query coverage, write maintenance, HBench, cost estimates, or measured winners.
No TPC kit or Cassandra server required to generate deterministic CQL files.
"""
from __future__ import annotations

import csv
import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

FAMILIES = tuple(f"TS{i}" for i in range(9))
MODIFIERS = tuple(f"M{i}" for i in range(1,6))
FIELDS = {'driver_system_id':'text', 'sensor_name':'text', 'event_ts':'bigint',
          'event_id':'uuid', 'sensor_reading':'double', 'padding':'text'}
ROOT = Path(__file__).resolve().parents[3]


def read_csv(path: Path) -> list[dict[str,str]]:
    with path.open(newline='', encoding='utf-8') as fd:
        return list(csv.DictReader(fd))


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False)+'\n',encoding='utf-8')


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def strict_bool(value: str) -> bool:
    if value not in ('true','false'):
        raise ValueError(f'Expected true/false matrix flag, got {value!r}')
    return value == 'true'


def checked_families(row: dict[str,str], family_columns: tuple[str,...], key: str) -> set[str]:
    active = json.loads(row[key])
    if not isinstance(active,list) or len(active)!=len(set(active)) or not set(active)<=set(family_columns):
        raise ValueError(f'Invalid {key} on {row.get("query_id")}')
    reported={c for c in family_columns if strict_bool(row[c])}
    if reported!=set(active):
        raise ValueError(f'Flag mismatch in {key} on {row.get("query_id")}')
    return reported


def by_id(rows: list[dict[str,str]], label: str) -> dict[str,dict[str,str]]:
    res={}
    for row in rows:
        qid=row['query_id']
        if not qid or qid in res:raise ValueError(f'Duplicate/empty query_id in {label}')
        res[qid]=row
    return res


@dataclass(frozen=True)
class CassandraCandidateSpec:
    candidate_id: str
    family: str
    table: str
    partition_key: tuple[str,...]
    clustering_key: tuple[str,...]
    clustering_order: tuple[str,...]
    columns: dict[str,str]
    purpose: str
    bucket_seconds: int | None
    query_filter_mode: str
    read_cql_template: str
    write_cql_template: str
    preprocessing: tuple[str,...]
    access_requirements: tuple[str,...]
    maintenance_requirements: tuple[str,...]
    active_modifiers: tuple[str,...]
    query_plan_verified: bool = False
    write_plan_verified: bool = False
    benchmarked: bool = False

    def validate(self) -> None:
        if self.family not in FAMILIES:raise ValueError('Unknown family')
        if not self.partition_key or not self.clustering_key:raise ValueError('Missing PK/CK')
        if (set(self.partition_key) | set(self.clustering_key)) - self.columns.keys():
            raise ValueError('Missing key column')
        if len(self.clustering_key)!=len(self.clustering_order):raise ValueError('CK ordering mismatch')
        if set(self.partition_key)&set(self.clustering_key):raise ValueError('Duplicate PK/CK')
        if any(x not in ('ASC','DESC') for x in self.clustering_order):raise ValueError('Bad CK order')
        if set(self.columns)!=set(FIELDS)|({'bucket_start_ms'} if self.bucket_seconds else set()):
            raise ValueError('Unexpected/missing source field in physical table')
        if any(word in self.read_cql_template.upper() for word in ('ALLOW FILTERING','CREATE INDEX')):
            raise ValueError('Unsafe CQL primitive')
        if self.benchmarked or self.write_plan_verified or self.query_plan_verified:
            raise ValueError('Stage 4 cannot declare later-stage results')
        if any(ch not in 'abcdefghijklmnopqrstuvwxyz0123456789_' for ch in self.table):
            raise ValueError('Invalid table identifier')

    def ddl(self, keyspace: str = '') -> str:
        self.validate()
        part=', '.join(self.partition_key)
        clustering=', '.join(self.clustering_key)
        column_lines=[f'    {name} {typ}' for name,typ in self.columns.items()]
        column_lines.append(f'    PRIMARY KEY (({part}), {clustering})')
        ck=', '.join(f'{name} {direction}' for name,direction in zip(self.clustering_key,self.clustering_order))
        prefix=f'{keyspace}.' if keyspace else ''
        return f'CREATE TABLE IF NOT EXISTS {prefix}{self.table} (\n'+',\n'.join(column_lines)+f'\n) WITH CLUSTERING ORDER BY ({ck});'


def safe_bucket_values(values: Any, name: str) -> list[int]:
    if not isinstance(values,list) or not values or any(type(x) is not int or x<1 or x>86400 for x in values):
        raise ValueError(f'Invalid bucket widths: {name}')
    if len(values)!=len(set(values)):raise ValueError(f'Duplicate bucket widths: {name}')
    return values


def prototypes(policy: dict[str,Any]) -> dict[str,CassandraCandidateSpec]:
    """Instantiate physical *possibilities*, not performance winners."""
    if (policy.get('select_winner_by_latency') or policy.get('apply_compaction_automatically')
            or policy.get('apply_ttl_automatically')):
        raise ValueError('No outcome-based tuning or implicit physical modifiers at Stage 4')
    if policy.get('source_profile')!='sensor-name-only/raw_read':
        raise ValueError('Physical policy/source access mode mismatch')
    if policy.get('timestamp_representation')!='bigint_epoch_milliseconds_utc':
        raise ValueError('Stage4 supports only epoch milliseconds')
    if policy.get('query_interval_bounds')!='inclusive':raise ValueError('Boundary policy must be inclusive')
    ts1=safe_bucket_values(policy['bucket_width_seconds'],'TS1')
    ts3=safe_bucket_values(policy['ts3_bucket_width_seconds'],'TS3')
    all_specs={}

    def add(family: str, cid: str, table: str, pk: tuple[str,...], ck: tuple[str,...],
            bucket: int | None, mode: str, description: str,
            pre: tuple[str,...], access: tuple[str,...], maintenance: tuple[str,...]):
        cols=dict(FIELDS)
        if bucket is not None: cols['bucket_start_ms']='bigint'
        predicates=['driver_system_id = ?', 'sensor_name = ?'] if mode=='station_and_sensor' else ['sensor_name = ?']
        if bucket is not None: predicates.append('bucket_start_ms = ?')
        predicates += ['event_ts >= ?', 'event_ts <= ?']
        read=(f'SELECT driver_system_id, sensor_name, event_ts, event_id, sensor_reading, padding '
              f'FROM {table} WHERE '+ ' AND '.join(predicates)+';')
        columns=list(cols)
        write=f'INSERT INTO {table} ('+', '.join(columns)+') VALUES ('+', '.join('?' for _ in columns)+');'
        spec=CassandraCandidateSpec(cid,family,table,pk,ck,tuple('ASC' for _ in ck),cols,
                                    description,bucket,mode,read,write,pre,access,maintenance,
                                    ('M1','M3') if bucket else ('M3',))
        spec.validate()
        if cid in all_specs:raise ValueError('Duplicate candidate '+cid)
        all_specs[cid]=spec

    add('TS0','ts0_series_raw','ts0_series_raw',('driver_system_id','sensor_name'),
        ('event_ts','event_id'),None,'station_and_sensor',
        'Raw readings; potentially unbounded partitions; baseline only.',
        ('Use original event time as epoch milliseconds; no physical time bucket.',),
        ('A sensor-name-only read needs a complete station identity enumeration; no ALLOW FILTERING.',
         'Perform one read per station and each of two temporal windows; deduplicate by event_id only if same logical event.',),
        ('Insert every input observation; preserve payload padding and stable event_id.',))
    for secs in ts1:
        add('TS1',f'ts1_series_{secs}s',f'ts1_series_{secs}s',
            ('driver_system_id','sensor_name','bucket_start_ms'),('event_ts','event_id'),secs,
            'station_and_sensor',f'Raw observations grouped into {secs}-second UTC buckets, by fully identified sensor stream.',
            (f'bucket_start_ms = (event_ts // {secs*1000}) * {secs*1000}; floor for negative epochs.',),
            ('Enumerate stations for sensor-name-only predicate before reads.',
             'Split BOTH 5-second windows into all intersected UTC bucket partitions.',
             'Inclusive endpoints require bucket intersection at boundaries; client merges complete result.',),
            ('Write each observation to its UTC bucket; stable event_id makes retried inserts idempotent.',))
    for secs in ts3:
        add('TS3',f'ts3_sensor_{secs}s',f'ts3_sensor_{secs}s',
            ('sensor_name','bucket_start_ms'),('event_ts','driver_system_id','event_id'),secs,
            'sensor_only',f'Alternate projection across all stations for selected sensor name, {secs}-second UTC buckets.',
            (f'bucket_start_ms = (event_ts // {secs*1000}) * {secs*1000}; floor for negative epochs.',),
            ('Direct sensor-name and temporal partition access; no station enumeration.',
             'Query BOTH 5-second intervals and all intersected buckets, including inclusive endpoints.',
             'Output includes station identity for correct multistation semantics.',),
            ('A complete workload deployment must write each observation to this projection alongside the chosen canonical source.',
             'Extra storage and fanout are NOT free; quantify in Stages 6/7/9.',))
    return all_specs


def verify_inputs(root: Path, frozen: Path, policy_path: Path) -> tuple[dict[str,Any],dict[str,Any]]:
    inputs={
        'analytical':frozen/'01_analytical_matrix.csv',
        'evidence':frozen/'02_temporal_evidence.csv',
        'activation':frozen/'03_ts_activation_matrix.csv',
        'modifiers':frozen/'04_modifier_matrix.csv',
        'trace':frozen/'05_explanation_trace.json',
        'workload_manifest':root/'inputs/workload/query_manifest.csv',
        'eer':root/'inputs/schema/tpcx_iot_eer.json',
        'catalog':root/'config/tsdb_family_catalog.json',
        'ts_rules':root/'config/tsdb_activation_rules.json',
        'modifier_rules':root/'config/temporal_modifier_rules.json',
        'physical_policy':policy_path,
    }
    for name,p in inputs.items():
        if not p.is_file():raise FileNotFoundError(f'{name}: {p}')
    hashes={name:sha256(p) for name,p in inputs.items()}
    schema=read_json(inputs['eer'])
    catalog=read_json(inputs['catalog'])
    if tuple(f['family'] for f in catalog['families'])!=FAMILIES:raise ValueError('TS catalog changed')
    if tuple(r['family'] for r in read_json(inputs['ts_rules'])['rules'])!=FAMILIES:
        raise ValueError('TS activation rules changed')
    if tuple(r['family'] for r in read_json(inputs['modifier_rules'])['rules'])!=MODIFIERS:
        raise ValueError('Modifier rules changed')
    if schema['entities']['Reading']['temporal']['series_identity']!=['driver_system_id','sensor_name']:
        raise ValueError('Unexpected EER identity')
    if schema['entities']['Reading']['temporal']['time_attribute']!='event_ts':
        raise ValueError('Unexpected EER time field')
    if schema['entities']['Reading']['temporal']['metrics']!=['sensor_reading']:
        raise ValueError('Unexpected EER metric field')
    if schema['entities']['Reading']['attributes']!=['driver_system_id','sensor_name','event_ts','sensor_reading','padding']:
        raise ValueError('Unexpected source payload')
    rows={name:by_id(read_csv(inputs[name]),name) for name in ('analytical','evidence','activation','modifiers','workload_manifest')}
    traces=read_json(inputs['trace'])
    trace_map=by_id(traces,'trace')
    query_ids=set(rows['workload_manifest'])
    if query_ids!={'Q_INGEST','Q_READ_2_WINDOWS'}:
        raise ValueError('Frozen profile supports exactly one official-read interpretation and ingestion')
    if any(set(table)!=query_ids for table in [*rows.values(),trace_map]):
        raise ValueError('Query sets do not match across frozen matrices and trace')
    for qid in query_ids:
        analytical=rows['analytical'][qid]
        evidence=rows['evidence'][qid]
        act=checked_families(rows['activation'][qid],FAMILIES,'active_families')
        mods=checked_families(rows['modifiers'][qid],MODIFIERS,'active_modifiers')
        tr=trace_map[qid]
        if act!=set(tr['active_ts_families']) or mods!=set(tr['active_modifiers']):
            raise ValueError('Trace/activation conflict for '+qid)
        if set(act)!={r['family'] for r in tr['ts_rule_trace'] if r['passed']}:
            raise ValueError('Trace TS rules changed for '+qid)
        if set(mods)!={r['family'] for r in tr['modifier_rule_trace'] if r['passed']}:
            raise ValueError('Trace modifier rules changed for '+qid)
        for col in ('selected_root','schema_rc','selected_document_depth'):
            if str(tr['analytical_row'][col])!=analytical[col]:
                raise ValueError('Trace/EER conflict '+col)
        for col in ('series_identity_bound','has_time_range','returns_raw_observations','predicate_confidence'):
            source=tr['evidence'][col]
            target=evidence[col]
            expected=str(source).lower() if isinstance(source,bool) else str(source)
            if expected!=target:raise ValueError('Trace/evidence conflict '+col)
        if tr['provenance'].get('benchmark_results_used') is not False:
            raise ValueError('Benchmark results cannot influence Stage 4')
    ingest=rows['evidence']['Q_INGEST']; read=rows['evidence']['Q_READ_2_WINDOWS']
    if (ingest['operation']!='INSERT' or read['operation']!='SELECT'
            or read['series_identity_bound']!='false'
            or read['predicate_confidence']!='ASSUMPTION_TO_VERIFY_IN_KIT'
            or json.loads(read['temporal_window_lengths_seconds'])!=[5,5]
            or int(read['temporal_window_count'])!=2
            or int(read['historical_lookback_seconds'])!=1800):
        raise ValueError('Unsupported or changed temporal read semantics')
    if read_json(inputs['physical_policy']).get('source_profile')!='sensor-name-only/raw_read':
        raise ValueError('Source profile mismatch')
    return rows,{'hashes':hashes,'source_paths':{k:str(p.relative_to(root)) if p.is_relative_to(root) else str(p) for k,p in inputs.items()}}


def instantiate(root: Path = ROOT, output_dir: Path | None = None, policy_path: Path | None = None,
                frozen_dir: Path | None = None) -> dict[str,Any]:
    root=root.resolve()
    output=(output_dir or root/'outputs/tpcx_iot_stage4').resolve()
    policy=(policy_path or root/'config/tpcx_stage4_cassandra_policy.json').resolve()
    frozen=(frozen_dir or root/'inputs/stage3_frozen').resolve()
    if output==frozen or output.is_relative_to(frozen) or output.is_relative_to(root/'inputs'):
        raise ValueError('Output directory overlaps with source evidence')
    rows, audit=verify_inputs(root,frozen,policy)
    available=prototypes(read_json(policy))
    used=set().union(*(checked_families(r,FAMILIES,'active_families') for r in rows['activation'].values()))
    mods=set().union(*(checked_families(r,MODIFIERS,'active_modifiers') for r in rows['modifiers'].values()))
    supported={spec.family for spec in available.values()}
    if used-supported:raise ValueError(f'Unimplemented activated families: {sorted(used-supported)}')
    selected={cid:spec for cid,spec in available.items() if spec.family in used}
    links=[]
    per_query=[]
    for qid in sorted(rows['activation']):
        active=checked_families(rows['activation'][qid],FAMILIES,'active_families')
        active_mods=checked_families(rows['modifiers'][qid],MODIFIERS,'active_modifiers')
        mapping={f:sorted(cid for cid,s in selected.items() if s.family==f) for f in sorted(active)}
        if any(not ids for ids in mapping.values()):raise ValueError('Missing candidate mapping')
        for family,cids in mapping.items():
            for cid in cids:
                spec=selected[cid]
                links.append({'query_id':qid,'family':family,'candidate_id':cid,
                              'query_kind':rows['workload_manifest'][qid]['operation'],
                              'active_modifiers':sorted(active_mods),
                              'access_status':'TEMPLATE_ONLY_UNVERIFIED_STAGE5',
                              'write_status':'TEMPLATE_ONLY_UNVERIFIED_STAGE6',
                              'requirements':list(spec.access_requirements)})
        per_query.append({'query_id':qid,'selected_root':rows['analytical'][qid]['selected_root'],
                          'active_families':sorted(active),'active_modifiers':sorted(active_mods),
                          'candidate_ids_by_family':mapping,
                          'candidate_origin_does_not_imply_query_coverage':True,
                          'stage5_query_coverage_verified':False,'stage6_write_coverage_verified':False})
    output.mkdir(parents=True,exist_ok=True)
    ddl_folder=output/'cql'/'schemas'
    ddl_folder.mkdir(parents=True,exist_ok=True)
    stale={p.stem for p in ddl_folder.glob('*.cql')} - set(selected)
    if stale:raise ValueError('Stale DDL in output folder: '+repr(sorted(stale)))
    for cid,s in sorted(selected.items()):
        cql='-- SchemaLens TPCx-IoT Stage 4 (RESEARCH): DDL ONLY, NOT BENCHMARKED\n'
        cql+=f'-- {cid}: {s.family}; read and write routing unverified\n'
        cql+='-- M3 activated (potential compaction), no strategy automatically chosen\n'
        cql+=s.ddl()+'\n'
        (ddl_folder/f'{cid}.cql').write_text(cql,encoding='utf-8')
    write_json(output/'10_cassandra_candidate_specs.json',[asdict(s) for _,s in sorted(selected.items())])
    write_json(output/'11_query_candidate_map.json',per_query)
    write_json(output/'12_query_read_primitives.json',[
        {'candidate_id':cid,'family':s.family,'query_filter_mode':s.query_filter_mode,
         'query_cql_primitive':s.read_cql_template,
         'two_window_execution':'Two independent intervals, each split across all intersected UTC buckets; do not combine as CQL OR',
         'preprocessing':s.preprocessing,'access_requirements':s.access_requirements,
         'status':'NO_EXECUTABLE_PLAN_YET'} for cid,s in sorted(selected.items())])
    write_json(output/'12b_write_primitives.json',[
        {'candidate_id':cid,'family':s.family,'insert_cql_primitive':s.write_cql_template,
         'preprocessing':s.preprocessing,'maintenance_requirements':s.maintenance_requirements,
         'status':'NO_VALIDATED_WRITEPLAN_YET'} for cid,s in sorted(selected.items())])
    with (output/'13_query_candidate_map.csv').open('w',newline='',encoding='utf-8') as f:
        cols=['query_id','query_kind','family','candidate_id','active_modifiers','access_status','write_status']
        w=csv.DictWriter(f,fieldnames=cols);w.writeheader()
        for item in links:w.writerow({k:(json.dumps(item[k]) if k=='active_modifiers' else item[k]) for k in cols})
    report={'stage':4,'benchmark':'TPCx-IoT v2.1.2','engine':'Apache Cassandra',
            'scope':'CANDIDATE_SPECS_ONLY','stage3_source':'UPLOADED_FROZEN_OUTPUT_FILES',
            'query_count':len(per_query),'candidate_count':len(selected),
            'query_family_candidate_links':len(links),
            'candidate_counts_by_family':{fam:sum(c.family==fam for c in selected.values()) for fam in FAMILIES},
            'candidate_ids':sorted(selected),
            'activated_families':sorted(used),'activated_modifiers':sorted(mods),
            'input_sha256':audit['hashes'],'input_paths':audit['source_paths'],
            'activation_rules_modified':False,'source_stage3_reexecuted':False,
            'used_benchmark_results':False,'benchmark_executed':False,
            'query_plan_verified':False,'write_plan_verified':False,'hbench_selected':False,
            'physical_policy':'Explicit candidate bucket widths; no automatically chosen winner',
            'major_uncertainties':[
                'Kit 2.1.2 Java query binding unavailable: sensor-only assumption must be verified.',
                'Raw-read semantics and optional four aggregate templates are not treated as interchangeable.',
                'No station-key enumeration implementation yet for TS0/TS1.',
                'Stable event_id UUID added by experimental adapter; original event format lacks it.',
                'Replication/consistency and two-run TPC execution not yet implemented.',
                'TS0 partition growth may be unsafe at high scale; kept as unbucketed reference.',
                'No physical compaction or TTL chosen from M3/M1 labels.',
                'Source specifications do not include exact event sampling distribution.'
            ],
            'next_stages':['Stage5: ValidEquivalentPlan for each of two windows',
                           'Stage6: WritePlans to maintain chosen tables',
                           'Stage7: Pre-benchmark cost proxies',
                           'Stage8: Canonical fusion, compatible cover, HBench',
                           'Stage9: Joint actual Cassandra benchmark / preserve top-k retrospectively']}
    write_json(output/'14_stage4_manifest.json',report)
    return report
