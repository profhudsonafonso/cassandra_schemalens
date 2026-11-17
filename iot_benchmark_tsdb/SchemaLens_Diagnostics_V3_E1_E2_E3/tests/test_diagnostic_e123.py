"""Offline regression for E1 E2 E3: no network and no Cassandra mutations."""
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

HERE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(HERE))
SOURCE=Path(os.environ.get('E123_TEST_BASE',str(HERE.parent)))
sys.path.insert(0,str(SOURCE))
# Users have native_reference_v3 in the isolated frozen V3 experiment.
# E123_TEST_REF is optional for source-only/offline package regression testing.
import iot_pipeline
if os.environ.get('E123_TEST_REF'):
    iot_pipeline.__path__.append(str(Path(os.environ['E123_TEST_REF'])))

from diagnostic_e123.core import (EXPECTED,ReadTrace,TraceSession,check_readonly_sql,
    create_synthetic_days,probes,find_ts2u,quantile,protect_output,run_path)
from diagnostic_e123.experiments import equal_projection_query,_create_diagnostic,_diagnostic_keyspace
from iot_pipeline.native_reference_v3 import source_oracle
from iot_pipeline.reference_baseline import build_wide_records
from iot_pipeline.offline import compare


def demo():
    rows=[]
    for d in ['d_0','d_1']:
        for t in [1_704_067_200_000,1_704_067_201_000,1_704_067_202_000]:
            for s,v in [('s_0',3.),('s_1',12.)]:
                rows.append(dict(device_id=d,sensor_name=s,timestamp=t,value_double=v+(t%10000)*0.001))
    qs=[]
    for i in range(1,13):
        qid='Q'+str(i)
        qs.append({'query_id':qid,'devices':['d_0','d_1'],'sensors':['s_0','s_1'],
                   'start_ms':1_704_067_200_000,'end_ms_exclusive':1_704_067_203_000,
                   'point_ms':1_704_067_201_000,'value_lower':5. if i in (3,5,6,10) else None,
                   'aggregation':'count' if i in (4,5,6,7,11) else None,
                   'group_by_ms':1000 if i in (7,11) else None,'set_op':'union' if i==12 else None})
    qs[-1]['set_children']=[{'devices':['d_0'],'sensors':['s_0','s_1'],
                               'start_ms':1_704_067_200_000,'end_ms_exclusive':1_704_067_203_000},
                              {'devices':['d_1'],'sensors':['s_0','s_1'],
                               'start_ms':1_704_067_201_000,'end_ms_exclusive':1_704_067_203_000}]
    return rows,qs,['s_0','s_1']


class FakeSession:
    def __init__(self,wide,sensors):
        self.wide=wide;self.sensors=sensors;self.calls=[]
    def execute(self,cql,params=None):
        assert cql.startswith('SELECT '),cql
        self.calls.append((cql,params))
        cols=cql.split(' FROM ')[0].split(',')[1:]
        # Supports only wide paths for controlled E2.
        out=[]
        for d,t,*values in self.wide:
            if d != params[0] or (len(params)==3 and not params[1]<=t<params[2]):continue
            r={'timestamp_ms':t}
            r.update({'m_'+s:v for s,v in zip(self.sensors,values)})
            out.append(SimpleNamespace(**{col:r[col] for col in ['timestamp_ms',*cols]}))
        return out


class Tests(unittest.TestCase):
    def setUp(self):
        self.rows,self.qs,self.sensors=demo()
        self.wide=build_wide_records(self.rows,self.sensors)

    def test_hash_fixed(self):self.assertEqual(len(EXPECTED),64)

    def test_p95_20(self):self.assertEqual(quantile(list(range(1,21)),95),19)

    def test_no_empty_quantile(self):
        with self.assertRaises(ValueError):quantile([],95)

    def test_sql_select_only(self):
        check_readonly_sql('SELECT * FROM x WHERE key=%s')
        for sql in ('DROP KEYSPACE production','TRUNCATE x','INSERT INTO x VALUES (1)',
                    'SELECT * FROM t; DROP KEYSPACE k;'):
            with self.subTest(sql=sql),self.assertRaises(ValueError):check_readonly_sql(sql)

    def test_trace_records_fetch(self):
        s=FakeSession(self.wide,self.sensors);trace=ReadTrace();t=TraceSession(s,trace)
        rec=t.execute('SELECT timestamp_ms,m_s_0 FROM iot_reference_table WHERE device_id=%s',('d_0',))
        self.assertEqual(len(rec),3);self.assertEqual(trace.calls,1);self.assertEqual(trace.returned_rows,3)
        self.assertGreaterEqual(trace.ns,0)

    def test_e2_same_cql_base_ts2u_all_queries(self):
        oracle=source_oracle();s=FakeSession(self.wide,self.sensors)
        for q in self.qs:
            a,pa=equal_projection_query(s,q,self.sensors,'iot_reference_table')
            b,pb=equal_projection_query(s,q,self.sensors,'ts2_iot_wide_unbucketed')
            self.assertTrue(compare(a,b),q['query_id'])
            self.assertTrue(compare(a,oracle(self.rows,q)),q['query_id'])
            self.assertEqual(pa['requests'],pb['requests'])
            self.assertEqual(pa['cql_rows'],pb['cql_rows'])

    def test_e2_q12_separate_child_bounds(self):
        s=FakeSession(self.wide,self.sensors)
        q=self.qs[-1]
        got,st=equal_projection_query(s,q,self.sensors,'iot_reference_table')
        self.assertEqual(st['requests'],2)
        self.assertEqual(len(got),3)
        self.assertEqual(len(s.calls),2)
        self.assertNotEqual(s.calls[0][1],s.calls[1][1])

    def test_e2_reject_arbitrary_table(self):
        with self.assertRaises(ValueError):
            equal_projection_query(FakeSession(self.wide,self.sensors),self.qs[0],self.sensors,'other')

    def test_e3_scale_multiplicative_and_deterministic(self):
        for days in (1,7,30):
            r=create_synthetic_days(self.rows,days)
            self.assertEqual(len(r),len(self.rows)*days)
            self.assertEqual(len({x['timestamp']//86_400_000 for x in r}),days)
            self.assertEqual(r,create_synthetic_days(self.rows,days))

    def test_e3_reject_crossday_input(self):
        source=self.rows+[dict(self.rows[0],timestamp=self.rows[0]['timestamp']+86_400_000)]
        with self.assertRaisesRegex(ValueError,'multiple UTC days'):create_synthetic_days(source,7)

    def test_e3_reject_invalid_days(self):
        for n in (0,-1,91):
            with self.assertRaises(ValueError):create_synthetic_days(self.rows,n)

    def test_e3_derived_labels_and_child_extents(self):
        r=create_synthetic_days(self.rows,7)
        qs=probes(self.qs,r,7)
        self.assertEqual(len(qs),17)
        self.assertEqual(sum(q['_diagnostic_query_label'].startswith('NATIVE_FIXED') for q in qs),12)
        extended=qs[-1]
        self.assertEqual(extended['query_id'],'Q12')
        self.assertEqual(extended['start_ms'],min(x['timestamp'] for x in r))
        self.assertEqual(extended['end_ms_exclusive'],max(x['timestamp'] for x in r)+1)
        self.assertTrue(all(c['start_ms']==extended['start_ms'] for c in extended['set_children']))

    def test_e3_only_diagnostic_keyspace(self):
        self.assertEqual(_diagnostic_keyspace('reference',7),'sl_diag_e3_reference_d07')
        s=Mock()
        with self.assertRaises(ValueError):_create_diagnostic(s,'sl_iot_ref_native_v3','RESET_DIAGNOSTIC_KEYSPACES_ONLY')
        with self.assertRaises(ValueError):_create_diagnostic(s,'sl_diag_e3_reference_d07','')
        s.execute.assert_not_called()
        _create_diagnostic(s,'sl_diag_e3_reference_d07','RESET_DIAGNOSTIC_KEYSPACES_ONLY')
        self.assertEqual(s.execute.call_count,2)

    def test_ts2u_identification(self):
        x={'deployment_id':'H_123','candidates':['ts2_iot_wide_unbucketed']}
        self.assertEqual(find_ts2u([x])['deployment_id'],'H_123')
        with self.assertRaises(ValueError):find_ts2u([])

    def test_protect_old_stage8(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            protect_output(root/'outputs'/'e1')
            (root/'outputs'/'e1').mkdir(parents=True)
            with self.assertRaises(FileExistsError):protect_output(root/'outputs'/'e1')
            with self.assertRaises(ValueError):protect_output(root/'stage8'/'newfile')

if __name__=='__main__':unittest.main()
