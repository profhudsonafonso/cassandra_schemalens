from __future__ import annotations
import unittest
import tempfile
import json
from unittest.mock import patch
from pathlib import Path
from types import SimpleNamespace
from rev3 import core as C
from rev3 import runtime as R
from rev3 import prebenchmark as P
from rev3 import probe as Pr
from v4_auto import selector as A
from v4_rev2 import cost as K

DAY=K.MILLIS_DAY
class FakeSession:
    def __init__(self,rows):self.rows=rows;self.queries=[]
    def execute(self,sql,args):
        self.queries.append((sql,tuple(args)))
        selected=[r for r in self.rows if r.device==args[0] and r.sensor==args[1]]
        selected=sorted(selected,key=lambda r:r.timestamp_ms,reverse=True)
        if 'LIMIT 1' in sql:return selected[:1]
        return selected

class Tests(unittest.TestCase):
    def setUp(self):
        t=1704067200000
        self.rows=[{'device_id':'d0','sensor_name':s,'timestamp':t+day*DAY+i*1000,
                  'value_double':float(day*10+i+j)}
           for day in range(3) for i in range(3) for j,s in enumerate(('s_0','s_1'))]
        self.qs=[{'query_id':f'Q{i}','devices':['d0'],'sensors':['s_0','s_1'],
                  'point_ms':t,'start_ms':t,'end_ms_exclusive':t+3*DAY,
                  'set_children':[{'devices':['d0'],'sensors':['s_0','s_1'],'start_ms':t,'end_ms_exclusive':t+2*DAY},
                    {'devices':['d0'],'sensors':['s_0','s_1'],'start_ms':t+2*DAY,'end_ms_exclusive':t+3*DAY}]
                  if i==12 else None} for i in range(1,13)]
        self.specs=[{'candidate_id':cid,'family':fam,
                 'ddl':'CREATE TABLE ts0_iot_raw (...) WITH CLUSTERING ORDER BY (timestamp_ms DESC);'
                 if fam=='TS0' else 'CREATE TABLE x (...)',
                 **({'max_rows_per_device_guard':100000} if cid=='ts2_iot_wide_unbucketed' else {})}
                 for cid,fam in [('ts0_iot_raw','TS0'),('ts2_iot_wide_unbucketed','TS2'),
                                 ('ts2_iot_wide','TS2'),('ts4_iot_latest','TS4')]]
        self.policy={'read_proxy_weights':{'request_count':0.25,'logical_rows':0.75},
                     'preselect_weights':{'read':0.8,'storage':0.1,'write':0.1},'top_k':5}
        self.activation=[{'ts_families':['TS0','TS2','TS4']}]
    def requests(self,q):
        out=[];ops=q['set_children'] if q['query_id']=='Q12' else [q]
        for op in ops:
            start=None if q['query_id'] in ('Q5','Q8') else q['point_ms'] if q['query_id']=='Q1' else op['start_ms']
            end=None if start is None else start+1 if q['query_id']=='Q1' else op['end_ms_exclusive']
            out.append(('d0',('s_0','s_1'),start,end))
        return out
    def oracle(self,rows,q):
        acc={}
        for d,projection,st,en in self.requests(q):
            for r in rows:
                if r['device_id']==d and r['sensor_name'] in projection and (st is None or st<=r['timestamp']<en):
                    acc[(r['device_id'],r['sensor_name'],r['timestamp'])]=r['value_double']
        if q['query_id']=='Q8':
            latest={}
            for (d,s,t),v in acc.items():
                if (d,s) not in latest or t>latest[d,s][0]:latest[d,s]=(t,v)
            return sorted((d,s,t,v) for (d,s),(t,v) in latest.items())
        return sorted((*key,v) for key,v in acc.items())
    def data(self):
        eligible,_=A.candidate_catalog(self.specs,self.activation,self.rows,self.policy)
        cov=A.physical_reads(self.rows,self.qs,eligible,self.oracle,lambda a,b:a==b,self.requests,self.activation)
        writes,storage,reads,_,_=K.derive_rev2(self.rows,self.qs,eligible,cov,self.policy,self.requests)
        plans=C.make_plan_catalog(eligible,self.qs,cov,reads,self.rows)
        return eligible,cov,writes,storage,reads,plans
    def test_stage5_Q8_has_distinct_plan_ids(self):
        *_,plans=self.data()
        cand=[p for p in plans if p['query_id']=='Q8' and p['family']=='TS0']
        self.assertEqual({p['physical_plan'] for p in cand},{'TS0_FULL_SCAN','TS0_DESC_LIMIT1'})
        self.assertEqual(len({p['plan_id'] for p in cand}),2)
    def test_stage7_does_not_underestimate_full_scan(self):
        *_,plans=self.data()
        modes={p['physical_plan']:p for p in plans if p['query_id']=='Q8' and p['family']=='TS0'}
        self.assertEqual(modes['TS0_FULL_SCAN']['physical_rows_proxy'],18)
        self.assertEqual(modes['TS0_DESC_LIMIT1']['physical_rows_proxy'],2)
        self.assertEqual(modes['TS0_FULL_SCAN']['request_count_proxy'],2)
        self.assertGreater(modes['TS0_FULL_SCAN']['analytic_read_proxy'],modes['TS0_DESC_LIMIT1']['analytic_read_proxy'])
    def test_stage7_TS2_full_vs_early(self):
        *_,plans=self.data()
        d={p['physical_plan']:p for p in plans if p['query_id']=='Q8' and p['candidate_id']=='ts2_iot_wide'}
        self.assertEqual(d['TS2D_FULL_SCAN']['physical_rows_proxy'],9)
        self.assertEqual(d['TS2D_FULL_SCAN']['request_count_proxy'],3)
        self.assertEqual(d['TS2D_DESC_SPARSE']['physical_rows_proxy'],1)
    def test_stage8_prefers_optimized_and_keeps_diversity(self):
        e,c,w,s,r,p=self.data()
        covers,selected,pareto=C.select_schema_and_plan(e,self.qs,c,p,s,w,self.policy)
        self.assertEqual(len(covers),6)
        self.assertEqual({x['canonical_source'] for x in selected},set(A.CANONICAL))
        byroot={x['canonical_source']:x for x in selected}
        self.assertEqual(byroot['ts0_iot_raw']['query_routes']['Q8']['physical_plan'],'TS0_DESC_LIMIT1')
        self.assertTrue(all(x['deployment_id'].startswith('R3H_') for x in selected))
    def test_activation_independent_of_cross_coverage(self):
        e,c,w,s,r,p=self.data()
        # Activation is only provenance: a new workload can activate TS0 for Q7
        # while the existing TS2 candidate still covers Q7 independently.
        self.assertEqual(len(c),37)
        cc={x['candidate_id']:x for x in e}
        cov={'route_status':'DIRECT','activated_for_query':False,'cross_query_reuse':True}
        modes=C.validated_modes(cc['ts2_iot_wide_unbucketed'],self.qs[6],cov)
        self.assertEqual(modes,['TS2U_RANGE_SCAN'])
    def test_reject_wrong_plan_id(self):
        design={'candidates':['ts0_iot_raw'],'query_routes':{'Q8':{'candidate_id':'ts0_iot_raw',
                 'physical_plan':'TS0_DESC_LIMIT1','plan_id':'WRONG'}}}
        with self.assertRaisesRegex(ValueError,'plan_id'):
            R.verify_frozen_route(design,{'query_id':'Q8'}, {})
    def test_missing_ddl_prevents_TS0_limit1(self):
        e,c,w,s,r,p=self.data()
        ts0=next(x for x in e if x['family']=='TS0')
        ts0['ddl']='CREATE TABLE something with ASC';p=C.make_plan_catalog(e,self.qs,c,r,self.rows)
        modes={x['physical_plan'] for x in p if x['query_id']=='Q8' and x['family']=='TS0'}
        self.assertEqual(modes,{'TS0_FULL_SCAN'})
    def test_null_fallback_preserves_results(self):
        vals=[SimpleNamespace(device='d0',sensor='s_0',timestamp_ms=5,value_double=None),
              SimpleNamespace(device='d0',sensor='s_0',timestamp_ms=3,value_double=1.0),
              SimpleNamespace(device='d0',sensor='s_1',timestamp_ms=4,value_double=2.0)]
        sess=R.TracedSession(FakeSession(vals))
        q={'query_id':'Q8','devices':['d0'],'sensors':['s_0','s_1'],'value_lower':None}
        def oracle(records,q):
            latest={}
            for r in records:
                k=(r['device_id'],r['sensor_name'])
                if k not in latest or r['timestamp']>latest[k]['timestamp']:latest[k]=r
            return latest
        got,n=R.read_ts0_latest(sess,q,oracle)
        self.assertEqual(n,1)
        self.assertEqual(got['d0','s_0']['timestamp'],3)
        self.assertEqual(got['d0','s_1']['timestamp'],4)
        self.assertEqual(sess.execute_calls,3)
    def test_block_write_sql(self):
        sess=R.TracedSession(FakeSession([]))
        for sql in ('DELETE FROM ts0_iot_raw','UPDATE ts0_iot_raw SET value_double=0',
                    'DROP KEYSPACE system'):
            with self.assertRaises(ValueError):sess.execute(sql,('d0','s_0'))
    def test_new_identifiers_deterministic(self):
        e,c,w,s,r,p=self.data()
        a=C.select_schema_and_plan(e,self.qs,c,p,s,w,self.policy)[1]
        b=C.select_schema_and_plan(e,self.qs,c,p,s,w,self.policy)[1]
        self.assertEqual([x['deployment_id'] for x in a],[x['deployment_id'] for x in b])
    def test_offline_atomic_freeze_verify_and_refuse_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            exp=Path(d)
            for name in ('phase_a1/outputs/normalized_double_readings.csv',
                         'phase_a1/outputs/query_instances_pilot.json'):
                f=exp/name;f.parent.mkdir(parents=True,exist_ok=True);f.write_text('fixture-content')
            frozen=exp/P.P.OUTPUT/'stage8/28_hbench_v4_rev2_frozen.json'
            frozen.parent.mkdir(parents=True,exist_ok=True);frozen.write_text('[]')
            e,c,w,sto,r,p=self.data()
            source=(object(),e,c,r,sto,w,self.rows,self.qs,self.policy)
            with patch.object(P,'parent_hashes',return_value={'rev2_hbench':'a'*64,'rev2_manifest':'b'*64}), \
                 patch.object(P,'inputs',return_value=source), \
                 patch.object(P.P,'verify',return_value={'status':'FROZEN_V4_REV2_PREBENCH'}):
                P.generate(exp,write=True)
                m=P.verify(exp)
                self.assertEqual(m['selected_count'],3)
                self.assertEqual(m['compatible_configurations'],6)
                with self.assertRaises(FileExistsError):P.generate(exp,write=True)
                target=exp/P.OUTPUT/'stage8/28_hbench_v4_rev3_frozen.json'
                target.write_text('tampered')
                with self.assertRaisesRegex(ValueError,'Rev3 artifact modified'):
                    P.verify(exp)

    def test_cannot_reuse_rev2_labels_as_rev3(self):
        design={'candidates':['ts0_iot_raw'],'query_routes':{'Q8':{'candidate_id':'ts0_iot_raw',
               'physical_plan':'TS0_DESC_LIMIT1','plan_id':C.stable_id('Q8','ts0_iot_raw','TS0_DESC_LIMIT1')}}}
        with self.assertRaisesRegex(ValueError,'not found'):
            R.verify_frozen_route(design,{'query_id':'Q8'}, {})
    def test_rev2_keyspace_matching_requires_exact_membership(self):
        a=[{'deployment_id':'R3H_123456789abc','candidates':['ts2_iot_wide_unbucketed']}]
        b=[{'deployment_id':'R2H_abcdef012345','candidates':['ts2_iot_wide_unbucketed']}]
        m={'status':'CONFIRMATORY_CASSANDRA_COMPLETE','keyspaces':{'R2H_abcdef012345':'sl_conf_r2_aabbccdd_r2h_abcdef012345'}}
        self.assertEqual(len(Pr.map_existing_keyspaces(a,b,m)),1)
        a[0]['candidates']=['ts0_iot_raw']
        self.assertFalse(Pr.map_existing_keyspaces(a,b,m))

if __name__=='__main__':unittest.main()
