"""No external DB needed. Scientific boundaries are regression assertions."""
import json
import tempfile
import unittest
from pathlib import Path
from copy import deepcopy
from core.bridge import read_native, make_queries, make_cassandra_specs, coverage_decisions, bucket_start_ms, _result
from tools.activate_iot_benchmark_ts_only import compute

ROOT=Path(__file__).resolve().parents[1]

class BridgeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.data=read_native(ROOT/'fixtures/native')
        cls.scenario=json.loads((ROOT/'inputs/scenario.json').read_text())
        cls.ops=json.loads((ROOT/'inputs/operations.json').read_text())
        cls.queries=make_queries(cls.data,cls.scenario,cls.ops)

    def test_native_csv_repeated_header_and_sparse_null(self):
        self.assertEqual(self.data['count'],35)
        self.assertEqual(self.data['batch_headers'],6)
        self.assertNotIn(('device_2','s1',1704067201000),{(x['device_id'],x['sensor_name'],x['timestamp']) for x in self.data['records']})

    def test_native_provenance(self):
        self.assertEqual(len(self.data['files_sha256']),4)
        self.assertEqual(self.data['sensors'],['s0','s1'])

    def test_non_double_rejected(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t)
            (root/'schema.txt').write_text('device_0 s0 1\n')
            with self.assertRaisesRegex(ValueError,'DOUBLE'):
                read_native(root)

    def test_conflicting_duplicate_rejected(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t); (root/'schema.txt').write_text('device_0 s0 4\n')
            (root/'device_0').mkdir()
            (root/'device_0'/'BigBatch_0.csv').write_text('Sensor,s0\n1704067200000,1.0\nSensor,s0\n1704067200000,2.0\n')
            with self.assertRaisesRegex(ValueError,'Conflicting duplicate'):
                read_native(root)

    def test_nanosecond_timestamp_rejected_in_ms_pilot(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t); (root/'schema.txt').write_text('device_0 s0 4\n')
            (root/'device_0').mkdir()
            (root/'device_0'/'BigBatch_0.csv').write_text('Sensor,s0\n1704067200000000000,1.0\n')
            with self.assertRaisesRegex(ValueError,'TIMESTAMP_PRECISION'):
                read_native(root)

    def test_dotted_device_path_accepted_as_text_value(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t); (root/'schema.txt').write_text('root.sg_0.d_0 s0 4\n')
            (root/'root.sg_0.d_0').mkdir()
            (root/'root.sg_0.d_0'/'BigBatch_0.csv').write_text('Sensor,s0\n1704067200000,1.0\n')
            data=read_native(root)
            self.assertEqual(data['devices'], ['root.sg_0.d_0'])

    def test_too_many_records_fail_closed(self):
        with self.assertRaisesRegex(ValueError,'max_rows'):
            read_native(ROOT/'fixtures/native', max_rows=3)

    def test_twelve_operations(self):
        self.assertEqual([q['query_id'] for q in self.queries],[f'Q{i}' for i in range(1,13)])
        self.assertTrue(all(q['parameter_origin'].startswith('deterministic illustrative') for q in self.queries))

    def test_q1_precision_point(self):
        a=_result(self.data['records'],self.queries[0])
        self.assertEqual(len(a),2)
        self.assertTrue(all(x['timestamp']==self.queries[0]['point_ms'] for x in a))

    def test_q5_does_not_inherit_arbitrary_short_window(self):
        q=deepcopy(self.queries[4]); q['end_ms_exclusive']=q['start_ms']+1
        answer=_result(self.data['records'],q)
        self.assertEqual(answer[0]['aggregate'],3)  # three s0 values pass value > 10 across full series
        self.assertEqual(answer,_result(self.data['records'],self.queries[4]))

    def test_q8_is_latest_inside_requested_window_not_global_latest(self):
        q=deepcopy(self.queries[7]);q['end_ms_exclusive']=q['start_ms']+2001
        a=_result(self.data['records'],q)
        self.assertEqual(len(a),2)
        self.assertTrue(all(x['timestamp']==q['start_ms']+2000 for x in a))

    def test_q7_query_anchored_window(self):
        q=deepcopy(self.queries[6]);q['start_ms']+=1000;q['group_by_ms']=2000
        a=_result(self.data['records'],q)
        self.assertEqual(a[0]['window_start_ms'],q['start_ms'])
        self.assertEqual(a[0]['aggregate'],2)

    def test_q12_set_type_semantics_demo(self):
        q=deepcopy(self.queries[11]);q['set_op']='union';u=_result(self.data['records'],q)
        q['set_op']='intersect';i=_result(self.data['records'],q)
        q['set_op']='except';e=_result(self.data['records'],q)
        self.assertEqual(len(u),24)
        self.assertEqual(len(i),0) # device in pilot tuple makes disjoint
        self.assertEqual(len(e),12)

    def test_utc_bucket_negative(self):
        self.assertEqual(bucket_start_ms(-1),-86400000)

    def test_only_original_temporal_rules(self):
        activated,provenance=compute(ROOT/'vendor/tsdb_base',self.scenario,self.ops)
        self.assertEqual(len(activated),12)
        self.assertEqual(activated[7]['ts_families'],['TS0','TS4'])
        self.assertEqual(activated[6]['ts_families'],['TS0','TS5'])
        self.assertTrue(all(set(x['ts_families']) <= {f'TS{i}' for i in range(9)} for x in activated))
        self.assertTrue(provenance['base_rule_engine_sha256'])

    def test_no_feedback_from_benchmark(self):
        bad=deepcopy(self.scenario);bad['benchmark_used_to_activate']=True
        with self.assertRaisesRegex(ValueError,'Post-benchmark'):
            compute(ROOT/'vendor/tsdb_base',bad,self.ops)

    def test_specifications_do_not_claim_benchmark_ready(self):
        specs=make_cassandra_specs(self.data['sensors'],self.scenario)
        self.assertEqual({x['family'] for x in specs},{'TS0','TS1','TS2','TS4','TS5'})
        self.assertTrue(all(x['status']=='DDL_BLUEPRINT_ONLY_NOT_BENCHMARK_READY' for x in specs))
        self.assertTrue(all('ALLOW FILTERING' not in x['ddl'] for x in specs))

    def test_activation_is_not_coverage(self):
        activated,_=compute(ROOT/'vendor/tsdb_base',self.scenario,self.ops)
        gates=coverage_decisions(activated,self.queries,make_cassandra_specs(self.data['sensors'],self.scenario))
        self.assertEqual(len(gates),26)
        self.assertTrue(all(x['coverage']=='NOT_VALIDATED' for x in gates))
        self.assertEqual(len([x for x in gates if x['family']=='TS4']),1)

if __name__=='__main__':unittest.main()
