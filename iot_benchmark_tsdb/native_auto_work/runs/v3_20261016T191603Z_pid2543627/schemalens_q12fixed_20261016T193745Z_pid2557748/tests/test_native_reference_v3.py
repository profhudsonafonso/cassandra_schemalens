"""Offline correctness tests. No Cassandra, Maven or destructive commands."""
import hashlib
from copy import deepcopy
import tempfile
from pathlib import Path
import unittest
from types import SimpleNamespace

from iot_pipeline.native_reference_v3 import (NATIVE_LABEL, native_contract, requests_for,
    physical_to_raw, fetch_query_records, offline_checks)
from iot_pipeline.reference_baseline import build_wide_records,table_ddl
from iot_pipeline.shared import read,inputs,read_rows,sha,path4
from iot_pipeline.stage9 import confirm_frozen
from iot_pipeline.offline import compare
from iot_pipeline.native_reference_v3 import source_oracle


class FakeWideCassandra:
    def __init__(self,wide,sensors):
        self.wide=wide
        self.sensors=sensors
        self.calls=[]
    def execute(self,cql,params):
        assert 'ALLOW FILTERING' not in cql
        assert ' WHERE device_id=%s' in cql
        assert cql.startswith('SELECT timestamp_ms,')
        self.calls.append((cql,params))
        cols=cql.split(' FROM ')[0].split(',')[1:]
        dev=params[0]
        bounds=params[1:]
        selected=[]
        for d,t,*vs in self.wide:
            if d!=dev or (bounds and not bounds[0]<=t<bounds[1]):
                continue
            data={'timestamp_ms':t}
            data.update({'m_'+s:v for s,v in zip(self.sensors,vs)})
            selected.append(SimpleNamespace(**{c:data[c] for c in ['timestamp_ms',*cols]}))
        return selected


def input_fixture():
    # Reuse source-provided illustrative query structure, then mark the query
    # objects as generated TEST doubles, never a real Java capture.
    rows=[
      {'device_id':'d_0','sensor_name':'s_0','timestamp':1000,'value_double':1.5},
      {'device_id':'d_0','sensor_name':'s_1','timestamp':1000,'value_double':0.0},
      {'device_id':'d_0','sensor_name':'s_0','timestamp':2000,'value_double':3.5},
      {'device_id':'d_0','sensor_name':'s_1','timestamp':2000,'value_double':6.5},
      {'device_id':'d_1','sensor_name':'s_0','timestamp':2000,'value_double':3.5},
      {'device_id':'d_1','sensor_name':'s_1','timestamp':2000,'value_double':6.5},
      {'device_id':'d_1','sensor_name':'s_0','timestamp':3000,'value_double':7.5},
    ]
    original=read(inputs()['queries'])
    queries=[]
    for i,q in enumerate(original):
        x=deepcopy(q)
        x['devices']=['d_0','d_1'] if q['query_id']=='Q12' else ['d_0']
        x['sensors']=['s_0','s_1']
        x['parameter_origin']='sample from official GenerateQueryWorkLoad Java API TEST DOUBLE'
        x['capture_sha256']='a'*64
        x['start_ms']=0
        x['end_ms_exclusive']=4000
        x['point_ms']=2000
        x['group_by_ms']=1000 if q['query_id'] in {'Q7','Q11'} else None
        if q['query_id']=='Q12':
            x['set_op']='union'
            x['set_children']=[{'devices':['d_0'],'sensors':['s_0','s_1'],
                                'start_ms':0,'end_ms_exclusive':3000},
                               {'devices':['d_1'],'sensors':['s_0','s_1'],
                                'start_ms':2000,'end_ms_exclusive':4000}]
        queries.append(x)
    return rows,queries,['s_0','s_1']


class NativeReferenceV3Tests(unittest.TestCase):
    def setUp(self):
        self.rows,self.queries,self.sensors=input_fixture()
        self.wide=build_wide_records(self.rows,self.sensors)

    def test_01_native_table_structure(self):
        ddl=table_ddl(self.sensors)
        self.assertIn('m_s_0 double',ddl)
        self.assertIn('m_s_1 double',ddl)
        self.assertIn('PRIMARY KEY ((device_id), timestamp_ms)',ddl)
        self.assertNotIn('bucket_day',ddl)

    def test_02_native_shape_and_sparse_values(self):
        self.assertEqual(len(self.wide),4)
        self.assertEqual(sum(v is not None for row in self.wide for v in row[2:]),7)

    def test_03_native_contract_provenance_required(self):
        meta=native_contract(self.rows,self.queries,self.sensors)
        self.assertEqual(meta['query_classes'][0],'Q1')
        self.assertFalse(meta['capture_bytes_verified'])
        bad=deepcopy(self.queries)
        bad[0]['parameter_origin']='illustrative pilot'
        with self.assertRaisesRegex(ValueError,'not a Java-generated'):
            native_contract(self.rows,bad,self.sensors)

    def test_04_capture_hash_audited_if_supplied(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'capture.jsonl';path.write_text('test java source')
            actual=sha(path)
            for q in self.queries:q['capture_sha256']=actual
            self.assertTrue(native_contract(self.rows,self.queries,self.sensors,path)['capture_bytes_verified'])
            path.write_text('modified capture')
            with self.assertRaisesRegex(ValueError,'does not match'):
                native_contract(self.rows,self.queries,self.sensors,path)

    def test_05_all_twelve_offline(self):
        self.assertEqual(offline_checks(self.rows,self.queries,self.sensors),(4,12))

    def test_06_cassandra_fake_all_twelve(self):
        oracle=source_oracle()
        fake=FakeWideCassandra(self.wide,self.sensors)
        for q in self.queries:
            got=oracle(fetch_query_records(fake,q,self.sensors),q)
            expected=oracle(self.rows,q)
            self.assertTrue(compare(got,expected),q['query_id'])
        self.assertFalse(any('ALLOW FILTERING' in cql for cql,_ in fake.calls))

    def test_07_q5_q8_unrestricted(self):
        for qid in ['Q5','Q8']:
            q=next(q for q in self.queries if q['query_id']==qid)
            self.assertTrue(all(start is None for _,_,start,_ in requests_for(q)))
            q['start_ms']=9000
            self.assertTrue(all(start is None for _,_,start,_ in requests_for(q)))

    def test_08_q12_separate_intervals_and_no_duplicate_records(self):
        q=next(q for q in self.queries if q['query_id']=='Q12')
        self.assertEqual(len(requests_for(q)),2)
        fake=FakeWideCassandra(self.wide,self.sensors)
        vals=fetch_query_records(fake,q,self.sensors)
        self.assertEqual(len(vals),len({(x['device_id'],x['sensor_name'],x['timestamp']) for x in vals}))
        self.assertEqual(len(fake.calls),2)

    def test_09_union_intersect_except_match_oracle(self):
        oracle=source_oracle()
        q=next(q for q in self.queries if q['query_id']=='Q12')
        for op in ['union','intersect','except']:
            q['set_op']=op
            fake=FakeWideCassandra(self.wide,self.sensors)
            self.assertTrue(compare(oracle(self.rows,q),oracle(fetch_query_records(fake,q,self.sensors),q)),op)

    def test_10_reject_missing_child_and_unknown_sensor(self):
        q=next(q for q in self.queries if q['query_id']=='Q12')
        q['set_children'][0]['sensors']=['not_a_sensor']
        with self.assertRaisesRegex(ValueError,'incompatible projections|absent device'):
            native_contract(self.rows,self.queries,self.sensors)

    def test_11_comparator_accepts_only_matching_measured_manifests(self):
        """SYNTHETIC CSVs used only to exercise parser, not benchmark results."""
        from iot_pipeline.shared import write, csv_write
        from iot_pipeline.native_reference_v3 import NATIVE_LABEL
        from iot_pipeline.compare_native_v3 import compare_runs
        frozen=confirm_frozen()
        sha_h=sha(path4(8,'28_hbench_frozen.json'))
        sha_r=sha(inputs()['native'])
        sha_q=sha(inputs()['queries'])
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);baseline=root/'baseline';hbench=root/'hbench'
            baseline.mkdir();hbench.mkdir()
            write(baseline/'37_manifest.json',{
                'status':'NATIVE_REFERENCE_POST_FREEZE','deployment_id':NATIVE_LABEL,
                'baseline_part_of_hbench':False,'official_iot_benchmark_score':False,
                'correctness':'PASS_ALL_12_QUERIES','hbench_sha256':sha_h,
                'source_rows_sha256':sha_r,'query_instances_sha256':sha_q,
                'measurement_rounds':20,'warmup':5})
            write(hbench/'37_manifest.json',{
                'official_iot_benchmark_score':False,'hbench_sha256':sha_h,
                'source_rows_sha256':sha_r,'measurement_rounds':20,'warmup':5})
            fields=['deployment_id','query_id','family','p95_ms','successful_operations','correctness']
            csv_write(baseline/'35_query_p95.csv',[
                {'deployment_id':NATIVE_LABEL,'query_id':f'Q{i}','family':'REFERENCE',
                 'p95_ms':2,'successful_operations':20,'correctness':'PASS'}
                for i in range(1,13)],fields)
            csv_write(hbench/'35_query_p95.csv',[
                {'deployment_id':h['deployment_id'],'query_id':f'Q{i}',
                 'family':h['query_routes'][f'Q{i}']['family'],
                 'p95_ms':1,'successful_operations':20,'correctness':'PASS'}
                for h in frozen for i in range(1,13)],fields)
            per_query,summary,provenance=compare_runs(hbench,baseline)
            self.assertEqual(len(summary),len(frozen)+1)
            self.assertEqual(len(per_query),len(frozen)*12)
            self.assertEqual(summary[1]['relative_difference_pct'],-50)
            self.assertFalse(provenance['hbench_query_sha256_explicitly_attested'])
            m=read(baseline/'37_manifest.json');m['query_instances_sha256']='bad'
            write(baseline/'37_manifest.json',m)
            with self.assertRaisesRegex(ValueError,'query_instances_sha256'):
                compare_runs(hbench,baseline)

    def test_12_frozen_hbench_not_modified(self):
        before=sha(path4(8,'28_hbench_frozen.json'))
        confirm_frozen()
        offline_checks(self.rows,self.queries,self.sensors)
        self.assertEqual(sha(path4(8,'28_hbench_frozen.json')),before)


if __name__=='__main__':
    unittest.main()
