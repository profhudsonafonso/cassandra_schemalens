"""Non-runtime Stage4 Rev2 contracts and independent checks across day boundaries."""
import copy
from types import SimpleNamespace
import unittest
from iot_pipeline import stage4,stage9
from iot_pipeline.offline import physical_state,read_records,compare
from iot_pipeline.shared import inputs, read,read_rows,require_policy,path4

class Stage4Rev2Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy=require_policy()
        cls.activations=read(inputs()['activation'])
        cls.blueprints=read(inputs()['blueprints'])
        cls.rows=read_rows(inputs()['native'])

    def test_daily_requires_m1_evidence(self):
        before=copy.deepcopy(self.activations)
        for a in before:a['modifiers']=[m for m in a['modifiers'] if m!='M1']
        specs,links,_=stage4.instantiate(before,self.blueprints,self.policy,self.rows)
        ts2=[s for s in specs if s['family']=='TS2']
        self.assertEqual([s['partition_variant'] for s in ts2],['unbucketed'])
        self.assertFalse(any(s['candidate_id']=='ts2_iot_wide' for s in specs))

    def test_unbucketed_guard_is_predeclared_and_enforced(self):
        p={**self.policy,'max_unbucketed_rows_per_device':1}
        specs,_,_=stage4.instantiate(self.activations,self.blueprints,p,self.rows)
        self.assertTrue(any(s['candidate_id']=='ts2_iot_wide' for s in specs))
        self.assertFalse(any(s['candidate_id']=='ts2_iot_wide_unbucketed' for s in specs))

    def test_ts2_variant_columns_partition_keys(self):
        specs=read(path4(4,'10_candidate_specs.json'))
        by={s['candidate_id']:s for s in specs}
        self.assertIn('PRIMARY KEY ((device_id,bucket_day),timestamp_ms)',by['ts2_iot_wide']['ddl'])
        self.assertIn('PRIMARY KEY ((device_id),timestamp_ms)',by['ts2_iot_wide_unbucketed']['ddl'])
        self.assertIn('m_s0 double',by['ts2_iot_wide_unbucketed']['ddl'])
        self.assertIn('m_s1 double',by['ts2_iot_wide_unbucketed']['ddl'])
        self.assertEqual(by['ts2_iot_wide']['exclusive_group'],by['ts2_iot_wide_unbucketed']['exclusive_group'])

    def test_no_cover_contains_both_ts2_granularities(self):
        covers=read(path4(8,'29_all_valid_covers.json'))
        for h in covers:
            self.assertFalse({'ts2_iot_wide','ts2_iot_wide_unbucketed'}<=set(h['candidates']))
        self.assertTrue(any('ts2_iot_wide_unbucketed' in h['candidates'] for h in covers))

    def test_unbucketed_query_across_midnight_in_memory(self):
        day=86_400_000
        t=20000*day
        rows=[{'device_id':'d0','sensor_name':'s0','timestamp':t+day-1,'value_double':2.},
              {'device_id':'d0','sensor_name':'s1','timestamp':t+day-1,'value_double':3.},
              {'device_id':'d0','sensor_name':'s0','timestamp':t+day+1,'value_double':4.}]
        state=physical_state(rows,self.policy['rollup_window_ms'])
        q={'query_id':'Q2','devices':['d0'],'sensors':['s0','s1'],
           'start_ms':t+day-10,'end_ms_exclusive':t+day+10}
        a=read_records(state,'TS2',q,candidate_id='ts2_iot_wide')
        b=read_records(state,'TS2',q,candidate_id='ts2_iot_wide_unbucketed')
        self.assertTrue(compare(sorted(a,key=lambda x:(x['sensor_name'],x['timestamp'])),
                                sorted(b,key=lambda x:(x['sensor_name'],x['timestamp']))))
        self.assertEqual(len(a),3)

    def test_candidate_aware_live_cql_uses_no_bucket_for_unbucketed(self):
        day=86_400_000;t=20000*day
        q={'query_id':'Q2','devices':['d0'],'sensors':['s0'],
           'start_ms':t+day-1,'end_ms_exclusive':t+day+1}
        class FakeSession:
            queries=[]
            def execute(self,cql,args):
                self.queries.append((cql,args))
                return [SimpleNamespace(timestamp_ms=t+day-1,m_s0=9.0)]
        fake=FakeSession()
        got=stage9._query_rows(fake,'TS2',q,['s0'],candidate_id='ts2_iot_wide_unbucketed')
        self.assertEqual(len(fake.queries),1)
        self.assertIn('ts2_iot_wide_unbucketed WHERE device_id=%s AND timestamp_ms',fake.queries[0][0])
        self.assertEqual(len(fake.queries[0][1]),3)
        self.assertEqual(got[0]['value_double'],9)

    def test_materializer_uses_candidate_spec(self):
        spec=next(s for s in read(path4(4,'10_candidate_specs.json'))
                  if s['candidate_id']=='ts2_iot_wide_unbucketed')
        h={'candidates':['ts0_iot_raw',spec['candidate_id']]}
        class FakeSession:
            def __init__(self):self.sql=[]
            def prepare(self,x):self.sql.append(x);return x
            def execute(self,sql,params=None):
                self.sql.append(sql)
                return []
        session=FakeSession()
        counts=stage9._materialize(session,h,self.rows,['s0','s1'],self.policy)
        self.assertEqual(counts['ts2_iot_wide_unbucketed'],18)
        self.assertTrue(any('INSERT INTO ts2_iot_wide_unbucketed' in x for x in session.sql))

if __name__=='__main__':unittest.main()
