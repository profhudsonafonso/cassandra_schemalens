import tempfile
import unittest
from pathlib import Path
from v4_rev2 import cost as K,prebenchmark as P,benchmark as B
from v4_rev2.prepare_holdout import edit_properties
from v4_auto import selector as A

class Tests(unittest.TestCase):
    def setUp(self):
        base=1704067200000
        self.rows=[{'device_id':'d0','sensor_name':s,'timestamp':base+d*K.MILLIS_DAY+i*1000,'value_double':float(i+j)}
                   for d in (0,1,2) for i in (0,1,2) for j,s in enumerate(('s_0','s_1'))]
        self.specs=[{'candidate_id':cid,'family':fam,
             **({'max_rows_per_device_guard':100000} if cid=='ts2_iot_wide_unbucketed' else {})}
             for cid,fam in [('ts0_iot_raw','TS0'),('ts2_iot_wide_unbucketed','TS2'),
                              ('ts2_iot_wide','TS2'),('ts4_iot_latest','TS4')]]
        self.policy={'read_proxy_weights':{'request_count':0.25,'logical_rows':0.75},
                     'preselect_weights':{'read':0.8,'storage':0.1,'write':0.1},'top_k':5}
        self.queries=[{'query_id':f'Q{i}','devices':['d0'],'sensors':['s_0','s_1'],
              'point_ms':base,'start_ms':base,'end_ms_exclusive':base+3*K.MILLIS_DAY,
              'set_children':[{'devices':['d0'],'sensors':['s_0','s_1'],'start_ms':base,'end_ms_exclusive':base+2*K.MILLIS_DAY},
                              {'devices':['d0'],'sensors':['s_0','s_1'],'start_ms':base+2*K.MILLIS_DAY,'end_ms_exclusive':base+3*K.MILLIS_DAY}]
              if i==12 else None}
              for i in range(1,13)]
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
        for d,proj,s,e in self.requests(q):
            for r in rows:
                if r['device_id']==d and r['sensor_name'] in proj and (s is None or s<=r['timestamp']<e):
                    acc[r['device_id'],r['sensor_name'],r['timestamp']]=r['value_double']
        if q['query_id']=='Q8':
            most={}
            for (d,s,t),v in acc.items():
                if (d,s) not in most or t>most[d,s][0]:most[d,s]=(t,v)
            return sorted((d,s,t,v) for (d,s),(t,v) in most.items())
        return sorted((*k,v) for k,v in acc.items())
    def derive(self):
        eligible,_=A.candidate_catalog(self.specs,self.activation,self.rows,self.policy)
        coverage=A.physical_reads(self.rows,self.queries,eligible,self.oracle,lambda a,b:a==b,self.requests,self.activation)
        writes,storage,reads,partition,coeff=K.derive_rev2(self.rows,self.queries,eligible,coverage,self.policy,self.requests)
        return eligible,coverage,writes,storage,reads,partition,coeff
    def test_days_3_bucket_request_count_vs_one_unbucketed(self):
        *_,reads,_,_=self.derive()
        a={(x['query_id'],x['candidate_id']):x for x in reads}
        daily=a['Q2','ts2_iot_wide'];plain=a['Q2','ts2_iot_wide_unbucketed']
        self.assertEqual(daily['request_count_proxy'],3)
        self.assertEqual(daily['bucket_count_proxy'],3)
        self.assertEqual(daily['extra_bucket_fanout_proxy'],2)
        self.assertEqual(plain['request_count_proxy'],1)
        self.assertGreater(daily['analytic_read_proxy'],plain['analytic_read_proxy'])
    def test_q12_respects_distinct_child_operands(self):
        *_,reads,_,_=self.derive();a={(x['query_id'],x['candidate_id']):x for x in reads}
        self.assertEqual(a['Q12','ts2_iot_wide_unbucketed']['request_count_proxy'],2)
        self.assertEqual(a['Q12','ts2_iot_wide']['request_count_proxy'],3)
        self.assertEqual(a['Q12','ts2_iot_wide']['extra_bucket_fanout_proxy'],1)
    def test_1_day_equivalence_accepted_not_forced_ranking(self):
        self.rows=[r for r in self.rows if r['timestamp']//K.MILLIS_DAY==self.rows[0]['timestamp']//K.MILLIS_DAY]
        start=self.rows[0]['timestamp']
        for q in self.queries:
            q['end_ms_exclusive']=start+K.MILLIS_DAY
        *_,reads,_,_=self.derive();a={(x['query_id'],x['candidate_id']):x for x in reads}
        self.assertEqual(a['Q2','ts2_iot_wide']['analytic_read_proxy'],
                         a['Q2','ts2_iot_wide_unbucketed']['analytic_read_proxy'])

    def test_empty_day_buckets_still_create_requests(self):
        self.rows=[r for r in self.rows if r['timestamp']//K.MILLIS_DAY==self.rows[0]['timestamp']//K.MILLIS_DAY]
        *_,reads,_,_=self.derive();a={(x['query_id'],x['candidate_id']):x for x in reads}
        self.assertEqual(a['Q2','ts2_iot_wide']['bucket_count_proxy'],3)
        self.assertEqual(a['Q2','ts2_iot_wide_unbucketed']['request_count_proxy'],1)
    def test_partition_pressure_soft_hard(self):
        eligible,_,_,_,_,part,_=self.derive()
        x=part['ts2_iot_wide_unbucketed']
        self.assertEqual(x['max_partition_rows_proxy'],9)
        self.assertTrue(x['hard_guard_pass'])
        p=dict(K.REV2_POLICY,partition_hard_limit_bytes=16)
        part2=K.physical_partitions(self.rows,eligible,p)
        self.assertFalse(part2['ts2_iot_wide_unbucketed']['hard_guard_pass'])
    def test_canonical_and_q8_same_oracle(self):
        eligible,coverage,writes,storage,reads,parts,policy=self.derive()
        self.assertEqual(len(coverage),37)
        self.assertTrue(all(v['route_status']=='DIRECT' for v in coverage))
        allcovers,selected,pareto=K.select_rev2(eligible,coverage,storage,writes,reads,self.queries,self.policy)
        self.assertEqual(len(allcovers),6)
        self.assertEqual({h['canonical_source'] for h in selected},set(A.CANONICAL))
    def test_forbid_non_frozen_or_unknown_keyspaces(self):
        name=B.safe_ks('8ab91e03','R2H_123456789abc')
        self.assertEqual(name,'sl_conf_r2_8ab91e03_r2h_123456789abc')
        for x in ('sl_iot_ref_native_v3','sl_diag_v4_d30_ref','system','sl_conf_r2_x_ref'):
            with self.assertRaises(ValueError):B.create_only_safe(None,x,B.TOKEN)
        with self.assertRaises(ValueError):B.create_only_safe(None,name,'')
        with self.assertRaises(ValueError):B.safe_ks('8ab91e03','V4_TS2U')
    def test_holdout_disallows_same_directory_and_missing_reference(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for x in ('phase_a1/outputs/normalized_double_readings.csv','phase_a1/outputs/query_instances_pilot.json'):
                f=root/x;f.parent.mkdir(exist_ok=True,parents=True);f.write_text('[]')
            with self.assertRaises(ValueError):P.check_holdout(root,root,True)
            with self.assertRaises(ValueError):P.check_holdout(root,None,True)
    def test_generator_property_override_on_new_copy(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'iot.properties';p.write_text('DATA_SEED=666\nQUERY_SEED=666\nPOINT_STEP=1000\n')
            from v4_rev2.prepare_holdout import edit_properties
            edit_properties(p,{'DATA_SEED':13,'QUERY_SEED':17,'QUERY_INTERVAL':86400000})
            content=p.read_text()
            self.assertIn('DATA_SEED=13',content);self.assertIn('QUERY_SEED=17',content)
            self.assertEqual(content.count('QUERY_INTERVAL='),1)
    def test_no_observed_latency_in_cost_or_selector(self):
        source=(Path(K.__file__).read_text()+Path(P.__file__).read_text())
        for phrase in ('36_raw_latencies.csv','v4_queries.csv','v4_raw.csv','stage9_hbench_native_v3'):
            self.assertNotIn(phrase,source)
    def test_predicts_nonzero_temporal_span(self):
        eligible,coverage,writes,storage,reads,parts,policy=self.derive()
        self.assertTrue(any(x['bucket_count_proxy']>=3 for x in reads))
        self.assertTrue(all(x['returned_bytes_proxy']>=0 and x['physical_rows_proxy']>=0 for x in reads))

if __name__=='__main__':unittest.main()
