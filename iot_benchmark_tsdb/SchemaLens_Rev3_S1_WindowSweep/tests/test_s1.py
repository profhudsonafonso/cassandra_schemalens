import copy
import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import s1_sweep as s


def fixture_queries():
    ans=[]
    for i in range(1,13):
        q={'query_id':f'Q{i}','devices':['d_0'],'sensors':['s_0','s_1'],
           'start_ms':s.DAY,'end_ms_exclusive':s.DAY+1000,
           'point_ms':s.DAY,'aggregation':'count' if i in (7,11) else None,
           'group_by_ms':3600000 if i in (7,11) else None,
           'value_lower':None,'set_children':None,'set_op':None}
        if i==12:
            q['set_op']='union'
            q['set_children']=[{'devices':['d_0'],'sensors':['s_0','s_1'],
                                'start_ms':s.DAY,'end_ms_exclusive':s.DAY+1000},
                               {'devices':['d_1'],'sensors':['s_0','s_1'],
                                'start_ms':s.DAY+1,'end_ms_exclusive':s.DAY+1000}]
        ans.append(q)
    return ans

class WindowTests(unittest.TestCase):
    def test_source_queries_not_modified(self):
        q=fixture_queries();before=copy.deepcopy(q)
        cs=s.make_cases(q,0)
        self.assertEqual(q,before)
        self.assertEqual(len(cs),18)
        self.assertEqual(cs[0]['s1_case_id'],'Q2_W01')
        self.assertEqual(cs[-1]['s1_case_id'],'Q8_CONTROL')
        self.assertEqual(cs[-1]['start_ms'],before[7]['start_ms'])

    def test_q12_children_bounds_not_merely_outer(self):
        q=s.make_cases(fixture_queries(),0)
        sample=next(x for x in q if x['s1_case_id']=='Q12_W14')
        self.assertEqual(sample['end_ms_exclusive'],14*s.DAY)
        for ch in sample['set_children']:
            self.assertEqual(ch['start_ms'],0)
            self.assertEqual(ch['end_ms_exclusive'],14*s.DAY)

    def test_reject_bad_windows(self):
        for w in ((7,1),(0,1),(1,1)):
            with self.assertRaises(ValueError):s.make_cases(fixture_queries(),0,w)

    def test_dense_row_assumption(self):
        rows=[{'device_id':'d_0','sensor_name':'s_0','timestamp':100,'value_double':1.},
              {'device_id':'d_0','sensor_name':'s_1','timestamp':100,'value_double':2.}]
        self.assertEqual(s.analytic_sanity(rows,['s_0','s_1']),{'d_0':[100]})
        with self.assertRaises(ValueError):s.analytic_sanity(rows[:1],['s_0','s_1'])

    def test_nearest_rank(self):
        self.assertEqual(s.pctl(list(range(1,61)),95),57)
        self.assertEqual(s.pctl(list(range(1,41)),95),38)

    def test_selection_requires_two_canonical(self):
        with self.assertRaises(ValueError):s.select_designs([{'canonical_source':'ts0_iot_raw','deployment_id':'t'}])
        h=[{'canonical_source':'ts2_iot_wide_unbucketed','deployment_id':'u'},
           {'canonical_source':'ts2_iot_wide','deployment_id':'d'}]
        self.assertEqual(len(s.select_designs(h)),2)

    def test_summary_requires_full_combinations(self):
        with self.assertRaises(ValueError):s.summary_from_raw([],{'rounds_per_case_and_variant':60})

    def test_no_overwrite_json(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'seal.json';s.write_new(p,{'ok':1})
            with self.assertRaises(FileExistsError):s.write_new(p,{'ok':0})

    def test_pack_rejects_incomplete(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);s.write_new(p/'s1_preregister.json',{'status':s.S1_STATUS})
            with self.assertRaises(FileNotFoundError):s.pack(p,p.parent/'never.zip')

if __name__=='__main__':unittest.main()

class AuditIntegrationTests(unittest.TestCase):
    def test_recompute_complete_synthetic_result(self):
        # This is an OFFLINE synthetic AUDITOR test, not measured Cassandra data.
        with tempfile.TemporaryDirectory() as td:
            out=Path(td);folder=out/'results';folder.mkdir()
            cases=s.make_cases(fixture_queries(),0)
            labels=[s.LABEL_BASE,'R3H_111111111111','R3H_222222222222']
            reg={'status':s.S1_STATUS,'rounds_per_case_and_variant':20,
                 'selected_ids':{'ts2_iot_wide_unbucketed':labels[1],
                                 'ts2_iot_wide':labels[2]},
                 'oracle_cardinality_by_case':{q['s1_case_id']:2 for q in cases},
                 'expected_observations':18*3*20}
            s.write_new(out/'s1_preregister.json',reg)
            raw=[];orders=[]
            for q in cases:
                case=q['s1_case_id']
                for rep in range(1,21):
                    order=labels[rep%3:]+labels[:rep%3]
                    orders.append({'rep':rep,'case_id':case,'variants_order':'|'.join(order)})
                    for pos,label in enumerate(order,1):
                        raw.append({'case_id':case,'query_id':q['query_id'],
                             'window_days':q['s1_window_days'] if q['s1_window_days'] is not None else 'CONTROL',
                             'variant':label,'rep':rep,'position':pos,'latency_ms':1+rep*0.01+pos*0.1,
                             'execute_calls':1,'iterated_rows':100,'oracle_cardinality':2,
                             'plan_id':'fixture','physical_plan':'fixture','correctness':'PASS'})
            ss,cc=s.summary_from_raw(raw,reg)
            s.write_csv_new(folder/'s1_raw_latencies.csv',raw)
            s.write_csv_new(folder/'s1_orders.csv',orders)
            s.write_csv_new(folder/'s1_query_p95.csv',ss)
            s.write_csv_new(folder/'s1_window_comparison.csv',cc)
            hashes={p.name:s.sha(p) for p in folder.glob('*.csv')}
            s.write_new(folder/'manifest.json',{'status':'S1_WINDOW_SWEEP_COMPLETE',
                'premeasurement_prereg_sha256':s.sha(out/'s1_preregister.json'),
                'csv_sha256':hashes})
            with patch.object(s,'verify',return_value=(reg,None,None,None,cases)):
                p=s.audit_results('/dummy',out)
                self.assertTrue(p.exists())
                self.assertIn('1080',p.read_text())
