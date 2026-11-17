#!/usr/bin/env python3
"""Patch only the standalone E123 addon; never modify frozen experiment files."""
from pathlib import Path
import hashlib
import sys

OLD = """def ensure_tables(cluster, keyspace, tables):
    cluster.refresh_schema_metadata()
    ks=cluster.metadata.keyspaces.get(keyspace)
    if ks is None:raise RuntimeError(f'Missing existing keyspace {keyspace}; run original Stage9 first')
    missing=set(tables)-set(ks.tables)
    if missing:raise RuntimeError(f'{keyspace} missing tables: {sorted(missing)}')
"""
NEW = '''def ensure_tables(cluster, keyspace, tables):
    """Read Cassandra's system tables directly without a global schema refresh.

    Read-only E1/E2 require existing Stage9 keyspaces. The Cassandra Python
    driver can fail on cluster.refresh_schema_metadata() even when the
    underlying tables are accessible. No DDL, writes, or repairs are attempted.
    """
    # Both system_schema queries are keyed by keyspace_name. Never interpret
    # a missing row as permission to create or modify the original keyspaces.
    session = cluster.connect()
    ks = session.execute(
        'SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = %s',
        (keyspace,),
    )
    if not any(getattr(row, 'keyspace_name', None) == keyspace for row in ks):
        raise RuntimeError(
            f'Missing existing keyspace {keyspace}; run original Stage9 first')
    found = {
        str(row.table_name)
        for row in session.execute(
            'SELECT table_name FROM system_schema.tables WHERE keyspace_name = %s',
            (keyspace,),
        )
    }
    missing = set(tables) - found
    if missing:
        raise RuntimeError(f'{keyspace} missing tables: {sorted(missing)}')
'''

def main():
    project=Path.cwd()
    target=project/'SchemaLens_Diagnostics_V3_E1_E2_E3'/'diagnostic_e123'/'core.py'
    if not target.is_file():
        raise SystemExit('ERROR: execute from SchemaLens_IoT_Cassandra_V3_Automatico root; addon core.py not found')
    source=target.read_text(encoding='utf-8')
    if NEW in source:
        print('ALREADY PATCHED:',target)
        return
    if source.count(OLD)!=1:
        raise SystemExit('ERROR: addon code differs from expected version. No changes made.')
    before=hashlib.sha256(target.read_bytes()).hexdigest()
    backup=target.with_name('core.py.metadatafix_backup_'+before[:12])
    if not backup.exists():backup.write_bytes(target.read_bytes())
    changed=source.replace(OLD,NEW)
    temp=target.with_name('core.py.metadatafix_tmp')
    temp.write_text(changed,encoding='utf-8')
    temp.replace(target)
    print('PATCHED: E1/E2 system_schema read-only check')
    print('FILE:',target)
    print('BACKUP:',backup)
    print('BEFORE_SHA256:',before)
    print('AFTER_SHA256:',hashlib.sha256(target.read_bytes()).hexdigest())
    print('FROZEN_STAGE1_TO_8: NOT TOUCHED')

if __name__=='__main__':main()
