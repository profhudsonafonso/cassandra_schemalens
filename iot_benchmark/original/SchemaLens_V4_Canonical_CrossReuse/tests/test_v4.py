"""Pure offline tests; Cassandra is emulated only to check Q8 paging and projections."""
import unittest
from collections import namedtuple
from unittest.mock import patch
import sys,types
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from diagnostic_v4 import core as C

class FakeSimpleStatement:
    def __init__(self,query,fetch_size): self.query,self.fetch_size=query,fetch_size

class FakeSession:
    def __init__(self,rows):self.rows=rows;self.queries=[]
    def execute(self,stmt, params):
        cql=stmt.query if hasattr(stmt,'query') else stmt
        self.queries.append((cql,params,getattr(stmt,'fetch_size',None)))
        fields=cql.split('SELECT ')[1].split(' FROM ')[0].split(',')
        R=namedtuple('Record',fields)
        device=params[0]
        result=[]
        for row in self.rows:
            if row['device_id']!=device:continue
            if 'bucket_day=%s' in cql and row['day']!=params[1]:continue
            if 'timestamp_ms >= %s' in cql:
                start,end=params[-2:]
                if not start<=row['timestamp_ms']<end:continue
            result.append(R(*(row.get(f) for f in fields)))
        return result

class Tests(unittest.TestCase):
    def setUp(self):
        self.rows=[{'device_id':'d_0','timestamp_ms':30,'m_s_0':None,'m_s_1':4.0},
                   {'device_id':'d_0','timestamp_ms':20,'m_s_0':3.0,'m_s_1':None},
                   {'device_id':'d_0','timestamp_ms':10,'m_s_0':1.0,'m_s_1':2.0}]
        self.q8={'query_id':'Q8','devices':['d_0'],'sensors':['s_0','s_1'],
                 'start_ms':0,'end_ms_exclusive':100}
        self.sysmodules={'cassandra':types.ModuleType('cassandra'),
                         'cassandra.query':types.ModuleType('cassandra.query')}
        self.sysmodules['cassandra.query'].SimpleStatement=FakeSimpleStatement
    def test_01_q8_sparse_distinct_latest_per_sensor(self):
        s=FakeSession(self.rows)
        with patch.dict(sys.modules,self.sysmodules):
            got,calls,visited=C.read_wide(s,self.q8,['s_0','s_1'],'iot_reference_table',{0},optimized_q8=True,page_size=16)
        self.assertEqual({x['sensor_name']:(x['timestamp'],x['value_double']) for x in got},
                         {'s_0':(20,3.0),'s_1':(30,4.0)})
        self.assertEqual((calls,visited),(1,2));self.assertEqual(s.queries[0][2],16)
    def test_02_q8_missing_forever_scans_to_end(self):
        s=FakeSession([dict(r, m_s_0=None) for r in self.rows])
        with patch.dict(sys.modules,self.sysmodules):
            got,calls,visited=C.read_wide(s,self.q8,['s_0','s_1'],'iot_reference_table',{0},optimized_q8=True)
        self.assertEqual(len(got),1);self.assertEqual(visited,3)
    def test_03_q8_full_scan_control(self):
        s=FakeSession(self.rows)
        got,calls,visited=C.read_wide(s,self.q8,['s_0','s_1'],'iot_reference_table',{0},optimized_q8=False)
        self.assertEqual((calls,visited,len(got)),(1,3,4))
    def test_04_q12_independent_children(self):
        q={'query_id':'Q12','devices':['d_0'],'sensors':['s_0','s_1'],
           'start_ms':0,'end_ms_exclusive':40,
           'set_children':[{'devices':['d_0'],'sensors':['s_0','s_1'],'start_ms':10,'end_ms_exclusive':21},
                           {'devices':['d_0'],'sensors':['s_0','s_1'],'start_ms':30,'end_ms_exclusive':40}]}
        s=FakeSession(self.rows)
        r,c,n=C.read_wide(s,q,['s_0','s_1'],'ts2_iot_wide_unbucketed',{0})
        self.assertEqual(c,2);self.assertEqual(n,3)
        self.assertEqual({x['timestamp'] for x in r},{10,20,30})
        self.assertEqual(s.queries[0][1][-2:],(10,21));self.assertEqual(s.queries[1][1][-2:],(30,40))
    def test_05_different_bucket_days(self):
        from datetime import date
        days=[19700,19701]
        s=FakeSession([{'device_id':'d_0','timestamp_ms':d*C.DAY,'day':date(1970,1,1) if False else None,
                         'm_s_0':1.0} for d in days])
        # Validate enumeration through CQL even without Cassandra rows.
        q={'query_id':'Q2','devices':['d_0'],'sensors':['s_0'],
           'start_ms':days[0]*C.DAY,'end_ms_exclusive':(days[1]+1)*C.DAY}
        r,c,n=C.read_wide(s,q,['s_0'],'ts2_iot_wide',set(days),bucket=True)
        self.assertEqual(c,2)
        self.assertTrue(all('bucket_day=%s' in sql for sql,_,_ in s.queries))
    def test_06_safe_only_diagnostic_keyspaces(self):
        C.assert_safe_reset('sl_diag_v4_d01_ts2u_ts4',C.RESET)
        C.assert_safe_reset('sl_diag_v4_d30_ref',C.RESET)
        for bad in ('sl_iot_ref_native_v3','sl_iot_h_2641c9751dce','system',
                    'sl_diag_v4_d99_ref','sl_diag_v4_d01_ref;DROP KEYSPACE system'):
            with self.subTest(bad=bad),self.assertRaises(ValueError):C.assert_safe_reset(bad,C.RESET)
    def test_07_all_v4_routes(self):
        qq=[{'query_id':f'Q{i}'} for i in range(1,13)]
        p=C.plans(qq);self.assertEqual(len(p),4)
        for x in p:
            self.assertEqual(len(x['query_routes']),12)
            self.assertNotIn('TS0',x['families'])
            self.assertEqual(x['query_routes']['Q8']['family'],'TS4' if 'TS4' in x['families'] else 'TS2')
    def test_08_scale_is_deterministic(self):
        original=[{'device_id':'d_0','sensor_name':'s_0','timestamp':C.DAY*19700+100,'value_double':1.2}]
        scaled=C.create_days(original,7)
        self.assertEqual(len(scaled),7)
        self.assertEqual(scaled[-1]['timestamp']-scaled[0]['timestamp'],6*C.DAY)
        with self.assertRaises(ValueError):C.create_days(original,90)
    def test_09_fixed_vs_probe_labels(self):
        queries=[{'query_id':f'Q{i}','start_ms':1,'end_ms_exclusive':10,
                  'set_children':[{'devices':['d_0'],'sensors':['s_0'],'start_ms':1,'end_ms_exclusive':10}] if i==12 else None}
                 for i in range(1,13)]
        rows=[{'timestamp':1},{'timestamp':10}]
        q=C.generated_queries(queries,rows,True)
        self.assertEqual(len(q),17)
        self.assertTrue(q[-1]['_synthetic_parameter_extension'])
    def test_10_p95_nearest_rank(self):
        self.assertEqual(C.pctl(range(1,21),95),19)
    def test_11_old_keyspace_not_canonical(self):
        for v in C.DESIGNS:
            self.assertTrue(C.safe_keyspace(7,v).startswith('sl_diag_v4_d07_'))
        self.assertEqual(C.safe_keyspace(1,C.BASE),C.safe_keyspace(1,C.BASE_OPT))
    def test_12_offline_complete_semantics_with_fixture(self):
        import json
        from iot_pipeline.shared import inputs,read,read_rows
        from iot_pipeline.native_reference_v3 import source_oracle
        qs=read(inputs()['queries'])
        rows=read_rows(inputs()['native'])
        q12=next(q for q in qs if q['query_id']=='Q12')
        q12['set_children']=[{'devices':[d],'sensors':q12['sensors'],
                              'start_ms':q12['start_ms'],'end_ms_exclusive':q12['end_ms_exclusive']}
                             for d in q12['devices'][:2]]
        qq=C.generated_queries(qs,rows,True)
        checks=C.offline_cross_coverage(rows,qq,['s0','s1'])
        self.assertEqual(len(checks),68)
        self.assertTrue(all(c['correct'] for c in checks))
    def test_13_aggregate_filter_q8(self):
        data=[{'variant':'A','query_label':'NATIVE_FIXED_Q8','p95_ms':10},
              {'variant':'A','query_label':'NATIVE_FIXED_Q1','p95_ms':2},
              {'variant':'A','query_label':'EXTENDED_Q2_FULL_SPAN','p95_ms':1}]
        output={(x['variant'],x['metric']):x for x in C.aggregate(data,[])}
        self.assertEqual(output['A','NATIVE_EX_Q8']['mean_of_query_p95_ms'],2)
        self.assertEqual(output['A','NATIVE_ALL_12']['mean_of_query_p95_ms'],6)

if __name__=='__main__':unittest.main()
