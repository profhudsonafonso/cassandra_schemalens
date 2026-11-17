import tempfile
import unittest
from pathlib import Path
from v4_auto import selector as S

class SelectorTests(unittest.TestCase):
    def setUp(self):
        self.rows=[{'device_id':'d_0','sensor_name':s,'timestamp':1704067200000+i*1000,'value_double':float(i+j)}
                   for i in range(5) for j,s in enumerate(('s_0','s_1'))]
        self.specs=[{'candidate_id':cid,'family':f,'guard':'g',
                      **({'max_rows_per_device_guard':1000} if cid=='ts2_iot_wide_unbucketed' else {})}
                    for cid,f in [('ts0_iot_raw','TS0'),('ts2_iot_wide_unbucketed','TS2'),
                                  ('ts2_iot_wide','TS2'),('ts4_iot_latest','TS4')]]
        self.activation=[{'ts_families':['TS0','TS2','TS4']}]
        start=1704067200000
        self.queries=[{'query_id':f'Q{i}', 'devices':['d_0'],'sensors':['s_0','s_1'],
                       'start_ms':start,'end_ms_exclusive':start+5000,'point_ms':start,
                       'set_op':'union' if i==12 else None,
                       'set_children':[{'devices':['d_0'],'sensors':['s_0','s_1'],
                                        'start_ms':start,'end_ms_exclusive':start+2000},
                                       {'devices':['d_0'],'sensors':['s_0','s_1'],
                                        'start_ms':start+2000,'end_ms_exclusive':start+5000}] if i==12 else None}
                      for i in range(1,13)]
        self.policy={'read_proxy_weights':{'request_count':0.25,'logical_rows':0.75},
                     'preselect_weights':{'read':0.8,'storage':0.1,'write':0.1},
                     'top_k':5}

    def requests(self,q):
        children=q['set_children'] if q['query_id']=='Q12' else [q]
        return [(d,tuple(ch['sensors']),
                 None if q['query_id'] in ('Q5','Q8') else ch['point_ms'] if q['query_id']=='Q1' else ch['start_ms'],
                 None if q['query_id'] in ('Q5','Q8') else ch['point_ms']+1 if q['query_id']=='Q1' else ch['end_ms_exclusive'])
                for ch in children for d in ch['devices']]

    def oracle(self,rows,q):
        filtered={}
        for d,projections,start,end in self.requests(q):
            for r in rows:
                if r['device_id']==d and r['sensor_name'] in projections and (start is None or start<=r['timestamp']<end):
                    filtered[r['device_id'],r['sensor_name'],r['timestamp']]=r['value_double']
        if q['query_id']=='Q8':
            newest={}
            for (d,s,t),v in filtered.items():
                if (d,s) not in newest or t>newest[d,s][0]:newest[d,s]=(t,v)
            return tuple(sorted((d,s,t,v) for (d,s),(t,v) in newest.items()))
        return tuple(sorted((*key,value) for key,value in filtered.items()))

    def make(self):
        eligible,rejected=S.candidate_catalog(self.specs,self.activation,self.rows,self.policy)
        plans=S.physical_reads(self.rows,self.queries,eligible,self.oracle,lambda x,y:x==y,self.requests,self.activation)
        writes,storage,read=S.derive_analytic(self.rows,self.queries,eligible,plans,self.policy,self.requests)
        covers,sel,pareto=S.search_covers(eligible,plans,storage,writes,read,self.queries,self.policy)
        return eligible,rejected,plans,writes,storage,read,covers,sel,pareto

    def test_01_v4_only_existing_activated_candidates(self):
        eleg,*_=self.make()
        self.assertEqual({s['candidate_id'] for s in eleg},set(S.CANONICAL)|{S.LATEST})

    def test_02_v4_cross_query_reuse_all_12_queries(self):
        _,_,plans,*_=self.make()
        self.assertEqual(len(plans),37)
        self.assertTrue(all(p['route_status']=='DIRECT' for p in plans))
        self.assertTrue(any(p['query_id']=='Q7' and p['candidate_id']=='ts2_iot_wide_unbucketed' for p in plans))

    def test_02b_activation_and_cross_query_distinguished(self):
        self.activation=[{'query_id':f'Q{i}','ts_families':['TS0']+(['TS2'] if i==2 else [])+(['TS4'] if i==8 else [])} for i in range(1,13)]
        # TS2 must be activated by at least Q2 and can serve Q7 by reuse.
        elig,_=S.candidate_catalog(self.specs,self.activation,self.rows,self.policy)
        plans=S.physical_reads(self.rows,self.queries,elig,self.oracle,lambda x,y:x==y,self.requests,self.activation)
        q2=next(p for p in plans if p['query_id']=='Q2' and p['candidate_id']=='ts2_iot_wide_unbucketed')
        q7=next(p for p in plans if p['query_id']=='Q7' and p['candidate_id']=='ts2_iot_wide_unbucketed')
        self.assertFalse(q2['cross_query_reuse'])
        self.assertTrue(q7['cross_query_reuse'])

    def test_03_v4_covers_exactly_one_canonical_source(self):
        *_,covers,sel,pareto=self.make()
        self.assertEqual(len(covers),6)
        self.assertTrue(sel)
        for d in covers:
            self.assertEqual(sum(x in S.CANONICAL for x in d['candidates']),1)
            self.assertEqual(set(d['query_routes']),{f'Q{i}' for i in range(1,13)})

    def test_04_v4_preserves_diverse_sources_no_latency(self):
        *_,covers,sel,pareto=self.make()
        self.assertEqual({d['canonical_source'] for d in sel},set(S.CANONICAL))
        self.assertTrue(all(d['measured_performance_used'] is False for d in covers))
        self.assertLessEqual(len(sel),self.policy['top_k'])

    def test_05_unbucketed_guard_enforced_prebenchmark(self):
        self.specs[1]['max_rows_per_device_guard']=2
        elig,rejected=S.candidate_catalog(self.specs,self.activation,self.rows,self.policy)
        self.assertNotIn('ts2_iot_wide_unbucketed',{s['candidate_id'] for s in elig})
        self.assertTrue(any('guard' in x['reason'] for x in rejected))

    def test_06_no_ts2_without_activation(self):
        self.activation=[{'ts_families':['TS0','TS4']}]
        elig,_=S.candidate_catalog(self.specs,self.activation,self.rows,self.policy)
        self.assertEqual({s['family'] for s in elig},{'TS0','TS4'})

    def test_07_q12_two_child_ranges_count_two_requests(self):
        _,_,_,_,_,reads,*_=self.make()
        item=next(x for x in reads if x['query_id']=='Q12' and x['candidate_id']=='ts2_iot_wide_unbucketed')
        self.assertEqual(item['request_count_proxy'],2)

    def test_08_original_query_window_points_and_read_proxy(self):
        _,_,_,_,_,reads,*_=self.make()
        q1=next(x for x in reads if x['query_id']=='Q1' and x['candidate_id']=='ts2_iot_wide_unbucketed')
        self.assertEqual(q1['request_count_proxy'],1)
        self.assertEqual(q1['physical_rows_proxy'],1)

    def test_09_new_canonical_reduces_write_proxy(self):
        _,_,_,_,_,_,covers,*_=self.make()
        ts0=next(x for x in covers if x['candidates']==['ts0_iot_raw'])
        ts2=next(x for x in covers if x['candidates']==['ts2_iot_wide_unbucketed'])
        self.assertEqual(ts0['analytic_cost']['write'],10)
        self.assertEqual(ts2['analytic_cost']['write'],5)

    def test_10_deterministic_hash_and_selection(self):
        a=self.make()[-2]
        b=self.make()[-2]
        self.assertEqual(a,b)

    def test_11_reject_wrong_v3_freeze(self):
        with tempfile.TemporaryDirectory() as tmp:
            f=Path(tmp)/'outputs/stage8/28_hbench_frozen.json'
            f.parent.mkdir(parents=True);f.write_text('{}')
            with self.assertRaises(ValueError):S.verify_old(tmp)

    def test_12_refuse_unverified_native_queries(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):S.verified_native(tmp,self.queries)

    def test_13_empty_eligible_guard(self):
        with self.assertRaises(ValueError):S.candidate_catalog([],self.activation,self.rows,self.policy)

    def test_14_q8_sparse_null_latest_per_sensor(self):
        self.rows=[r for r in self.rows if not (r['sensor_name']=='s_1' and r['timestamp']==1704067204000)]
        self.make()  # Uses oracle: newest timestamp differs per sensor

    def test_15_no_external_oracle_data_in_selector(self):
        source=Path(S.__file__).read_text()
        for forbidden in ('v4_raw.csv','v4_queries.csv','36_raw_latencies.csv','35_query_p95.csv','stage9/','diagnostics_v4/live'):
            self.assertNotIn(forbidden,source)

if __name__=='__main__':unittest.main()
