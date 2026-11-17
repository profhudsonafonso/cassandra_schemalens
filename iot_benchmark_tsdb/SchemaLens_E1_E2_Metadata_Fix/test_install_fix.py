"""Standalone regression of the E1/E2 metadata-read patch. No Cassandra access."""
import importlib.util
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

FIX=Path(__file__).with_name('install_fix.py')
CORE=Path(__file__).resolve().parents[1]/'SchemaLens_Diagnostics_V3_E1_E2_E3'/'diagnostic_e123'/'core.py'

class FakeCluster:
    def __init__(self,ks='sl_iot_ref_native_v3',tables=('iot_reference_table',)):
        self.ks,self.tables,self.queries=ks,tables,[]
    def connect(self):return self
    def execute(self,cql,params):
        assert cql.startswith('SELECT '),cql
        self.queries.append((cql,params))
        if 'system_schema.keyspaces' in cql:
            return [SimpleNamespace(keyspace_name=self.ks)] if params==(self.ks,) else []
        if 'system_schema.tables' in cql:
            return [SimpleNamespace(table_name=t) for t in self.tables] if params==(self.ks,) else []
        raise AssertionError(cql)
    def refresh_schema_metadata(self):raise AssertionError('Forbidden metadata refresh')

class Tests(unittest.TestCase):
    def test_patch_on_copy_and_original_preserved(self):
        import ast
        with tempfile.TemporaryDirectory() as d:
            project=Path(d);p=project/'SchemaLens_Diagnostics_V3_E1_E2_E3'/'diagnostic_e123'/'core.py'
            p.parent.mkdir(parents=True)
            spec=importlib.util.spec_from_file_location('metadatafix_installer_for_test', FIX)
            mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
            src=CORE.read_text(encoding='utf-8')
            if mod.NEW in src:src=src.replace(mod.NEW,mod.OLD)
            p.write_text(src,encoding='utf-8')
            original=p.read_bytes()
            subprocess.run([sys.executable,str(FIX)],cwd=project,check=True,capture_output=True)
            source=p.read_text()
            self.assertIn('FROM system_schema.tables',source)
            self.assertIn('FROM system_schema.keyspaces',source)
            self.assertNotIn('    cluster.refresh_schema_metadata()\n',source)
            ast.parse(source)
            ns={};tree=ast.parse(source);func=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='ensure_tables');exec(compile(ast.Module(body=[func],type_ignores=[]),'<snippet>','exec'),ns)
            f=ns['ensure_tables']
            good=FakeCluster();f(good,'sl_iot_ref_native_v3',['iot_reference_table'])
            self.assertEqual(len(good.queries),2)
            with self.assertRaisesRegex(RuntimeError,'missing tables'):
                f(FakeCluster(tables=()),'sl_iot_ref_native_v3',['iot_reference_table'])
            with self.assertRaisesRegex(RuntimeError,'Missing existing keyspace'):
                f(FakeCluster(),'nonexisting',['iot_reference_table'])
            subprocess.run([sys.executable,str(FIX)],cwd=project,check=True,capture_output=True)
            self.assertEqual(source,p.read_text())
            backups=list(p.parent.glob('core.py.metadatafix_backup_*'))
            self.assertEqual(len(backups),1)
            self.assertEqual(backups[0].read_bytes(),original)

if __name__=='__main__': unittest.main()
