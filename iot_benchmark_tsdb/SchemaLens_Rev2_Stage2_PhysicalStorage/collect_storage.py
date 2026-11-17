#!/usr/bin/env python3
"""Read-only Cassandra physical storage audit for frozen SchemaLens Rev2.

No writes to Cassandra. No nodetool flush, snapshot, compact, cleanup or repair.
Produces machine-readable results from system_schema and nodetool tablestats.
All code and comments are in English for supervisor-facing reproducibility.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import json
import re
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

KS_PATTERN = re.compile(r"sl_conf_r2_[a-f0-9]{8}_(?:reference|r2h_[a-f0-9]{12})\Z")
KEYSPACE_RE = re.compile(r"^Keyspace\s*:\s*(\S+)\s*$", re.I)
TABLE_RE = re.compile(r"^Table\s*:\s*(\S+)\s*$", re.I)
METRICS = {
    'sstable_count': ['SSTable count'],
    'sstable_live_bytes': ['Space used (live)'],
    'sstable_total_bytes': ['Space used (total)'],
    'snapshot_total_bytes': ['Space used by snapshots (total)'],
    'memtable_data_bytes': ['Memtable data size'],
    'partition_count_estimate': ['Number of partitions (estimate)'],
    'sstable_compression_ratio': ['SSTable Compression Ratio'],
    'bloom_filter_false_positives': ['Bloom filter false positives'],
}
MISSING = {'', 'NaN', 'N/A', 'null', 'None', '?'}


def digest(p: Path) -> str:
    h = hashlib.sha256()
    with p.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):
            h.update(block)
    return h.hexdigest()


def json_load(path: Path):
    return json.loads(path.read_text(encoding='utf-8'))


def csv_load(path: Path):
    with path.open(newline='',encoding='utf-8') as f:
        return list(csv.DictReader(f))


def csv_write(path: Path, rows: list[dict], keys: list[str]):
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=keys,extrasaction='ignore')
        w.writeheader();w.writerows(rows)


def parse_num(value):
    if value is None:return None
    token=value.strip().replace(',','').split(' ')[0]
    if token in MISSING:return None
    try:
        return int(token) if re.fullmatch(r'-?\d+',token) else float(token)
    except ValueError:return None


def parse_tablestats(content:str, keyspace:str) -> list[dict]:
    """Parse stable nodetool field names; do not invent missing metrics."""
    metrics={};current_ks=None;current_table=None
    def store():
        if current_table and (current_ks is None or current_ks==keyspace):
            metrics[current_table]={'keyspace':keyspace,'table':current_table,**current_values}
    current_values={}
    for raw in content.splitlines():
        s=raw.strip();km=KEYSPACE_RE.match(s);tm=TABLE_RE.match(s)
        if km:
            store();current_ks=km.group(1);current_table=None;current_values={}
        elif tm:
            store();current_table=tm.group(1);current_values={}
        elif current_table and ':' in s:
            k,v=s.split(':',1)
            for field,names in METRICS.items():
                if k.strip() in names:current_values[field]=parse_num(v)
    store()
    return list(metrics.values())


def validate_inputs(experiment:Path, results:Path) -> tuple[dict,list[dict],dict]:
    """Strictly tie storage measurements to the already completed frozen benchmark."""
    man=json_load(results/'manifest.json')
    if man.get('status')!='CONFIRMATORY_CASSANDRA_COMPLETE':
        raise ValueError('Benchmark manifest not marked complete')
    frozen=experiment/'outputs/v4_rev2_prebenchmark/stage8/28_hbench_v4_rev2_frozen.json'
    freeze_manifest=experiment/'outputs/v4_rev2_prebenchmark/stage8/manifest.json'
    if digest(frozen)!=man['frozen_HBench_sha256']:
        raise ValueError('Frozen HBench checksum differs from confirmatory manifest')
    if digest(freeze_manifest)!=man['frozen_manifest_sha256']:
        raise ValueError('Frozen Stage8 manifest checksum differs from confirmatory manifest')
    stage_manifest=json_load(freeze_manifest)
    root=experiment/'outputs/v4_rev2_prebenchmark'
    for rel,expected in stage_manifest.get('artifact_sha256',{}).items():
        if digest(root/rel)!=expected:
            raise ValueError('Frozen Stage 7/8 artifact checksum differs: '+rel)
    for key,rel in [('dataset_sha256','phase_a1/outputs/normalized_double_readings.csv'),
                    ('queries_sha256','phase_a1/outputs/query_instances_pilot.json')]:
        if key in man and digest(experiment/rel)!=man[key]:
            raise ValueError('Confirmatory source data or query file changed since benchmark: '+key)
    selected=json_load(frozen)
    if [x['deployment_id'] for x in selected]!=man['selected_ids']:
        raise ValueError('Selected IDs differ between freeze and confirmatory run')
    keyspaces=man['keyspaces']
    if set(keyspaces)!=set(man['selected_ids'])|{'BASELINE_ORIGINAL','BASELINE_Q8_OPTIMIZED'}:
        raise ValueError('Unexpected variants in benchmark manifest')
    if keyspaces['BASELINE_ORIGINAL']!=keyspaces['BASELINE_Q8_OPTIMIZED']:
        raise ValueError('Baseline original and optimized must share one table')
    for ks in keyspaces.values():
        if not isinstance(ks,str) or not KS_PATTERN.fullmatch(ks):
            raise ValueError('Refusing to inspect unexpected keyspace name')
    loaders=csv_load(results/'load.csv')
    expected=set(man['selected_ids'])|{'BASELINE_ORIGINAL'}
    if {r['variant'] for r in loaders}!=expected:
        raise ValueError('Load CSV is inconsistent with selected frozen variants')
    if any(r['keyspace']!=keyspaces[r['variant']] for r in loaders):
        raise ValueError('Load CSV keyspaces differ from manifest')
    return man,loaders,{h['deployment_id']:h for h in selected}


def collect_cql(session, keyspace, outdir):
    """Read from system_schema only; no data scans or partition COUNT(*)."""
    rows=list(session.execute('SELECT table_name FROM system_schema.tables WHERE keyspace_name=%s',(keyspace,)))
    names=sorted(r.table_name for r in rows)
    if not names:raise RuntimeError('Expected keyspace missing or empty: '+keyspace)
    tables=[]
    for table in names:
        colrows=list(session.execute('SELECT column_name,kind,position,type FROM system_schema.columns WHERE keyspace_name=%s AND table_name=%s',(keyspace,table)))
        columns=[{'name':r.column_name,'kind':r.kind,'position':r.position,'cql_type':r.type} for r in colrows]
        tables.append({'keyspace':keyspace,'table':table,'column_count':len(columns),
                       'partition_keys':[c['name'] for c in sorted(columns,key=lambda c:c['position']) if c['kind']=='partition_key'],
                       'clustering_keys':[c['name'] for c in sorted(columns,key=lambda c:c['position']) if c['kind']=='clustering'],
                       'columns':columns})
    (outdir/('schema_'+keyspace+'.json')).write_text(json.dumps(tables,indent=2,ensure_ascii=False),encoding='utf-8')
    return tables


def run_nodetool(command:list[str], keyspace:str, timeout:int) -> str:
    result=subprocess.run([*command,'tablestats',keyspace],capture_output=True,text=True,timeout=timeout)
    if result.returncode!=0:
        raise RuntimeError(f'nodetool tablestats {keyspace} exit={result.returncode}: '+result.stderr[-1200:])
    return result.stdout


def safe_output_dir(out:Path,experiment:Path,results:Path):
    for forbidden in (experiment.resolve(), results.resolve()):
        if out==forbidden or forbidden in out.parents:
            raise ValueError('Output must be outside frozen experiment and benchmark results')
    if out.exists():raise FileExistsError('Refusing to overwrite existing storage audit: '+str(out))


def summarize(rows: list[dict], variants:dict, load:list[dict]):
    lookup={(r['keyspace'],r['table']):r for r in rows}
    byks={}
    for r in rows:byks.setdefault(r['keyspace'],[]).append(r)
    loader={r['variant']:r for r in load}
    summary=[]
    for name,ks in variants.items():
        tables=byks.get(ks,[])
        def total(k):
            vals=[r.get(k) for r in tables]
            return sum(vals) if vals and all(isinstance(v,(int,float)) for v in vals) else None
        lv=loader['BASELINE_ORIGINAL' if name=='BASELINE_Q8_OPTIMIZED' else name]
        summary.append({'variant':name,'keyspace':ks,'table_count':len(tables),
                        'sstable_live_bytes':total('sstable_live_bytes'),
                        'sstable_total_bytes':total('sstable_total_bytes'),
                        'memtable_data_bytes':total('memtable_data_bytes'),
                        'sstable_count':total('sstable_count'),
                        'physical_write_operations':int(lv['physical_write_operations']),
                        'metric_cells':int(lv['metric_cells']),
                        'same_physical_reference':name.startswith('BASELINE_')})
    return summary


def make_report(summary, allrows, has_nodetool, man):
    lines=['# SchemaLens Cassandra Rev2 — armazenamento físico (Stage 2)','',
      'Coleta somente de leitura feita após o benchmark confirmatório; não altera o HBench.','',
      '## Proveniência','',
      f'- HBench SHA-256: `{man["frozen_HBench_sha256"]}`',
      f'- Benchmark: `{man["status"]}`',
      f'- Métricas SSTable disponíveis: **{"sim" if has_nodetool else "não"}**.','',
      '## Tamanho por configuração','',
      '| Configuração | Tabelas | SSTable live bytes | SSTable total bytes | Memtable bytes | SSTables | Escritas físicas |',
      '|---|---:|---:|---:|---:|---:|---:|']
    for r in summary:
        fmt=lambda k: str(r[k]) if r[k] is not None else 'N/D'
        lines.append(f'| `{r["variant"]}` | {r["table_count"]} | {fmt("sstable_live_bytes")} | {fmt("sstable_total_bytes")} | {fmt("memtable_data_bytes")} | {fmt("sstable_count")} | {fmt("physical_write_operations")} |')
    lines+=['','## Interpretação e limitações','',
      '- A baseline original e a baseline Q8 otimizada compartilham a mesma tabela: os tamanhos duplicados na apresentação não devem ser somados.',
      '- `Space used (live)` é espaço de SSTables na instância consultada, e não bytes lógicos, transferência de rede ou tamanho total do banco.',
      '- `Memtable data size` é memória estimada em buffers de escrita: não some indiscriminadamente às SSTables como se fosse um total físico uniforme.',
      '- SSTables podem estar vazias se dados ainda estiverem no memtable. Esta coleta NÃO executa `nodetool flush` para preservar o estado observado.',
      '- Se o SSTable count for zero, não conclua que o armazenamento é zero: faça uma etapa separada pós-benchmark com flush controlado, registrada em outro snapshot.',
      '- Compaction e SSTables antigas podem afetar `Space used (total)`; comparar após flush e compaction naturais deve ser tratado como etapa própria.',
      '- `Number of partitions (estimate)` é estimativa de cardinalidade, não tamanho máximo ou percentil das partições.',
      '- Esses resultados não medem ingestão concorrente, manutenção de TS4, latência do servidor ou dimensões de partição individualizadas.',
      '- Não reotimize a seleção congelada com essas medidas: uma mudança de custo exigiria nova revisão e novo holdout.','']
    if not has_nodetool:
        lines+=['**Auditoria parcial:** apenas `system_schema` foi coletado. Rode com `--nodetool-cmd` para obter SSTables.','']
    return '\n'.join(lines)+'\n'


def main(argv=None):
    p=argparse.ArgumentParser(description='Read-only Cassandra disk and schema audit (frozen Rev2)')
    p.add_argument('--experiment',required=True,type=Path)
    p.add_argument('--results',required=True,type=Path)
    p.add_argument('--out',required=True,type=Path)
    p.add_argument('--host',default='127.0.0.1')
    p.add_argument('--port',type=int,default=9043)
    p.add_argument('--nodetool-cmd',default='nodetool',help='e.g. "docker exec cassandra nodetool"; no shell execution')
    p.add_argument('--skip-nodetool',action='store_true',help='Collect system_schema only (partial physical audit)')
    p.add_argument('--timeout',type=int,default=120)
    args=p.parse_args(argv)
    exp=args.experiment.resolve();results=args.results.resolve();out=args.out.resolve()
    safe_output_dir(out,exp,results)
    man,loaders,selected=validate_inputs(exp,results)
    unique_keyspaces=sorted(set(man['keyspaces'].values()))
    nodetool_args=shlex.split(args.nodetool_cmd)
    if not nodetool_args and not args.skip_nodetool:
        raise ValueError('Missing nodetool executable')
    try:
        from cassandra.cluster import Cluster
    except ImportError as exc:
        raise RuntimeError('Install cassandra-driver in your existing venv: pip install cassandra-driver') from exc
    # Don't create output before verifying access to Cassandra and keyspaces.
    cluster=Cluster([args.host],port=args.port,connect_timeout=20)
    try:
        session=cluster.connect()
        liveks={r.keyspace_name for r in session.execute('SELECT keyspace_name FROM system_schema.keyspaces')}
        missing=set(unique_keyspaces)-liveks
        if missing:raise RuntimeError('Required Cassandra keyspaces are absent: '+', '.join(sorted(missing)))
        release=list(session.execute('SELECT release_version,cluster_name FROM system.local'))[0]
        out.mkdir(parents=True,exist_ok=False)
        schemas=[];table_stats=[];status={}
        for ks in unique_keyspaces:
            schema=collect_cql(session,ks,out)
            schemas.extend(schema)
            if not args.skip_nodetool:
                try:
                    txt=run_nodetool(nodetool_args,ks,args.timeout)
                except Exception as exc:
                    status[ks]={'status':'ERROR','detail':str(exc)}
                else:
                    (out/f'nodetool_tablestats_{ks}.txt').write_text(txt,encoding='utf-8')
                    parsed=parse_tablestats(txt,ks)
                    expected_tables={item['table'] for item in schema}
                    parsed_live={item['table'] for item in parsed if item.get('sstable_live_bytes') is not None}
                    success=bool(expected_tables) and expected_tables <= parsed_live
                    status[ks]={'status':'OK' if success else 'INCOMPLETE_METRICS',
                                'tables_parsed':len(parsed),'expected_tables':len(expected_tables)}
                    table_stats.extend(parsed)
        # Union over schema: missing metrics are absent, never guessed or zero-filled.
        metrics={(r['keyspace'],r['table']):r for r in table_stats}
        merged=[{'keyspace':r['keyspace'],'table':r['table'],'column_count':r['column_count'],
                 'partition_keys':'|'.join(r['partition_keys']),'clustering_keys':'|'.join(r['clustering_keys']),
                 **{k:metrics.get((r['keyspace'],r['table']),{}).get(k) for k in METRICS}}
                for r in schemas]
        variants=summarize(merged,man['keyspaces'],loaders)
        csv_write(out/'table_storage.csv',merged,['keyspace','table','column_count','partition_keys','clustering_keys',*METRICS])
        csv_write(out/'deployment_storage.csv',variants,['variant','keyspace','table_count','sstable_live_bytes',
                   'sstable_total_bytes','memtable_data_bytes','sstable_count','physical_write_operations',
                   'metric_cells','same_physical_reference'])
        schema_digest={k:digest(out/f'schema_{k}.json') for k in unique_keyspaces}
        metadata={'status':'STORAGE_AUDIT_COMPLETE' if (not args.skip_nodetool and all(v['status']=='OK' for v in status.values())) else 'STORAGE_AUDIT_PARTIAL',
            'timestamp_utc':datetime.now(timezone.utc).isoformat(),
            'cassandra_release_version':release.release_version,'cassandra_cluster_name':release.cluster_name,
            'host':args.host,'port':args.port,'keyspaces':unique_keyspaces,
            'frozen_HBench_sha256':man['frozen_HBench_sha256'],'confirmatory_manifest_sha256':digest(results/'manifest.json'),
            'stage7_8_unchanged':True,'nodetool_status':status,'schema_sha256':schema_digest,
            'no_database_changes':True,'no_flush_or_compaction':True}
        (out/'manifest_storage.json').write_text(json.dumps(metadata,indent=2,ensure_ascii=False),encoding='utf-8')
        (out/'RELATORIO_ARMAZENAMENTO_PT.md').write_text(make_report(variants,merged,metadata['status']=='STORAGE_AUDIT_COMPLETE',man),encoding='utf-8')
        print('STATUS:',metadata['status'])
        for ks,s in status.items():print('nodetool:',ks,s['status'],s.get('detail',''))
        print('OUTPUT:',out)
    finally:
        cluster.shutdown()

if __name__=='__main__':main()
