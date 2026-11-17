"""Regression/security tests that require no Cassandra or Java generation."""
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase
from unittest.mock import Mock
from rev3_confirmatory import gate,benchmark

class SafetyAndStatisticsTests(TestCase):
    def test_verification_source_sha256_is_existing_audited_revision(self):
        from rev3 import core, prebenchmark, runtime
        gate.assert_rev3_implementation()
    def test_nearest_rank(self):
        vals=list(range(1,41))
        self.assertEqual(benchmark.pctl(vals,95),38)
        self.assertEqual(benchmark.pctl(vals,99),40)
    def test_safe_keyspaces(self):
        design=[{'deployment_id':'R3H_934e5af2474c'}]
        ks=benchmark.keyspace_names('a'*64,'b'*64,'tag',design)
        self.assertEqual(ks[benchmark.LABEL_ORIG],ks[benchmark.LABEL_OPT])
        for val in ks.values():benchmark.guard_keyspace(val)
    def test_keyspace_rejects_sql_injection(self):
        for x in ['prod','sl_conf_r2_12345678_reference','sl_conf_r3_1234567890_x;DROP',
                  'sl_conf_r3_1234_reference','sl_conf_r3_1234567890_reference ;']:
            with self.assertRaises(ValueError):benchmark.guard_keyspace(x)
    def test_unfrozen_name_is_refused(self):
        with self.assertRaises(ValueError):
            benchmark.keyspace_names('a'*64,'b'*64,'tag',[{'deployment_id':'arbitrary'}])
    def test_create_only_needs_token(self):
        session=Mock()
        with self.assertRaises(PermissionError):
            benchmark.create_only(session,'sl_conf_r3_1234567890_reference','bad')
        session.execute.assert_not_called()
    def test_create_only_never_drops(self):
        s=Mock()
        benchmark.create_only(s,'sl_conf_r3_1234567890_reference',benchmark.CONFIRM)
        self.assertTrue(s.execute.call_args_list)
        self.assertTrue(all('DROP' not in str(x) and 'TRUNCATE' not in str(x) for x in s.execute.call_args_list))
    def test_refuse_preexisting_database_keyspace_before_any_create(self):
        session=Mock()
        session.execute.return_value=[('existing',)]
        with self.assertRaises(FileExistsError):
            benchmark.ensure_absent(session,{'a':'sl_conf_r3_1234567890_reference'})
        self.assertTrue(all('CREATE' not in str(x) for x in session.execute.call_args_list))
    def test_match_one_plan(self):
        design={'deployment_id':'R3H_934e5af2474c','query_routes':{'Q8':{
           'plan_id':'pid','physical_plan':'TS0_DESC_LIMIT1'}}}
        catalog=[{'plan_id':'pid','request_count_proxy':2,'physical_rows_proxy':2}]
        raw=[{'variant':design['deployment_id'],'query_id':'Q8','execute_calls':2,'iterated_rows':2}]
        check=benchmark.analyze_proxies(raw,[design],catalog)
        self.assertEqual(check[0]['status'],'MATCH')
        raw[0]['iterated_rows']=3000
        self.assertEqual(benchmark.analyze_proxies(raw,[design],catalog)[0]['status'],'MISMATCH')
        raw.append({'variant':design['deployment_id'],'query_id':'Q8','execute_calls':2,'iterated_rows':2})
        self.assertEqual(benchmark.analyze_proxies(raw,[design],catalog)[0]['status'],'VARIABLE_OBSERVED')
    def test_sealed_json_refuses_overwrite(self):
        with TemporaryDirectory() as td:
            f=Path(td)/'sealed.json'
            gate.dump(f,{'ok':True})
            self.assertEqual(gate.read(f),{'ok':True})
            with self.assertRaises(FileExistsError):gate.dump(f,{'ok':False})
    def test_csv_does_not_overwrite(self):
        with TemporaryDirectory() as td:
            p=Path(td)/'out.csv'
            benchmark.save_csv(p,[{'a':2}])
            with self.assertRaises(FileExistsError):benchmark.save_csv(p,[{'a':3}])
    def test_same_run_tag_produces_same_names(self):
        d=[{'deployment_id':'R3H_934e5af2474c'}]
        a=benchmark.keyspace_names('1'*64,'2'*64,'x',d)
        b=benchmark.keyspace_names('1'*64,'2'*64,'x',d)
        c=benchmark.keyspace_names('1'*64,'3'*64,'x',d)
        self.assertEqual(a,b);self.assertNotEqual(a,c)

class StorageTests(TestCase):
    def test_parse_nodetool_storage(self):
        from rev3_confirmatory.storage import parse_stats
        sample='''Keyspace : sl_conf_r3_1234567890_reference
        Table: iot_reference_table
        SSTable count: 1
        Space used (live): 125460
        Memtable data size: 0
        Number of partitions (estimate): 6
        Compacted partition maximum bytes: 61214
        '''
        parsed=parse_stats(sample,'sl_conf_r3_1234567890_reference')
        self.assertEqual(len(parsed),1)
        self.assertEqual(parsed[0]['sstable_live_bytes'],125460)
        self.assertEqual(parsed[0]['max_compacted_partition_bytes'],61214)
