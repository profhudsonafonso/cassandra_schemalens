"""Regression checks for frozen-selection integrity and observed telemetry.
Run with: python3 -m unittest discover -s tests -v
"""
from __future__ import annotations
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import audit_stage78 as A


def dump(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,indent=2)+'\n',encoding='utf-8')


class AuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.exp=Path(self.tmp.name)/'exp'
        self.exp.mkdir()
        self.root=self.exp/A.REV2
        inputs={'candidate_specs':'outputs/stage4/10_candidate_specs.json',
                'activation':'phase_a1/outputs/activation_ts_only.json',
                'queries':'phase_a1/outputs/query_instances_pilot.json',
                'data':'phase_a1/outputs/normalized_double_readings.csv',
                'policy':'config/pipeline_policy.json'}
        sources={}
        for name,rel in inputs.items():
            obj={'preselect_weights':{'read':.8,'write':.1,'storage':.1}} if name=='policy' else {}
            dump(self.exp/rel,obj)
            sources[name]=A.sha(self.exp/rel)
        self.id='R2H_123456789abc'
        self.cid='ts2_iot_wide_unbucketed'
        self.route={'Q1':{'candidate_id':self.cid,'family':'TS2','plan_id':'Q1__'+self.cid,
                          'route_status':'DIRECT','reason':'canonical point reconstruction'}}
        self.cost={'read':5.,'write':2.,'storage':80.}
        self.h={'deployment_id':self.id,'canonical_source':self.cid,
                'candidates':[self.cid], 'families':['TS2'], 'query_routes':self.route,
                'selection_reason':'PARETO_FRONT','analytic_cost':self.cost,
                'preselection_score':1., 'semantically_complete_offline':True}
        self.cover=dict(self.h)
        self.read={'query_id':'Q1','candidate_id':self.cid,'plan_id':'Q1__'+self.cid,
                   'analytic_read_proxy':5.,'request_count_proxy':1,
                   'physical_rows_proxy':12,'extra_bucket_fanout_proxy':0,
                   'returned_bytes_proxy':480}
        self.explanation={'deployment_id':self.id,'selection_reason':'PARETO_FRONT',
                'query_explanations':[{'query_id':'Q1','candidate_id':self.cid,
                       'plan_cost_features':self.read}]}
        docs={
          'stage8/28_hbench_v4_rev2_frozen.json':[self.h],
          'stage8/29_all_compatible_covers.json':[self.cover],
          'stage7/23_read_proxies_rev2.json':[self.read],
          'stage7/22_write_proxies.json':[{'candidate_id':self.cid,'physical_writes_proxy':2}],
          'stage7/21_storage_proxies.json':[{'candidate_id':self.cid,'estimated_bytes_proxy':80}],
          'stage5/15_cross_query_coverage.json':[{'query_id':'Q1','candidate_id':self.cid,'plan_id':'Q1__'+self.cid,
                                                   'cross_query_reuse':True,'route_status':'DIRECT'}],
          'stage8/32_selection_explanations_rev2.json':[self.explanation],
          'stage7/24_partition_proxies_rev2.json':[{'candidate_id':self.cid,'max_partition_rows_proxy':12,'hard_guard_pass':True}],
          'stage7/25_rev2_cost_policy.json':{'request_cost_units':4.}
        }
        artifacts={}
        for rel,obj in docs.items():
            dump(self.root/rel,obj)
            artifacts[rel]=A.sha(self.root/rel)
        self.meta={'status':'FROZEN_V4_REV2_PREBENCH','independent_from_original_v3':True,
                   'no_measured_latency_used':True,'no_Stage9_results_used':True,'no_Cassandra_used':True,
                   'parent_v3_files_sha256':{},'parent_v3_hbench_sha256':'v3_sha','inputs_sha256':sources,
                   'artifact_sha256':artifacts,'selected_count':1,'cover_count':1,'pareto_count':1,'top_k':5,
                   'selected_ids':[self.id], 'dataset_query_hashes':{'data':'data1','queries':'queries1'},
                   'java_capture':{'sha256':'capture1'}}
        dump(self.root/'stage8/manifest.json',self.meta)
        self.measured={'frozen_HBench_sha256':A.sha(self.root/'stage8/28_hbench_v4_rev2_frozen.json'),
                       'frozen_manifest_sha256':A.sha(self.root/'stage8/manifest.json'),
                       'selected_ids':[self.id], 'query_classes':1, 'parent_v3_sha256':'v3_sha', 'dataset_sha256':'data1',
                       'queries_sha256':'queries1','new_generator_capture_sha256':'capture1'}

    def test_frozen_pareto_and_scores(self):
        result=A.audit_frozen(self.exp,self.measured)
        self.assertEqual(result['deployments'][0]['selection_reason'],'PARETO_FRONT')
        self.assertEqual(len(result['annotations']),1)

    def test_hash_tamper_rejected(self):
        path=self.root/'stage7/23_read_proxies_rev2.json'
        path.write_text(path.read_text()+' ',encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'hash mismatch'):
            A.audit_frozen(self.exp,self.measured)

    def test_changed_parent_input_rejected(self):
        path=self.exp/'config/pipeline_policy.json'
        path.write_text('{}',encoding='utf-8')
        with self.assertRaisesRegex(ValueError,'input changed'):
            A.audit_frozen(self.exp,self.measured)

    def test_changed_observed_frozen_hash_rejected(self):
        self.measured['frozen_HBench_sha256']='0'*64
        with self.assertRaisesRegex(ValueError,'Measured HBench SHA'):
            A.audit_frozen(self.exp,self.measured)

    def test_predicted_counts_matched_and_mismatched(self):
        frozen=A.audit_frozen(self.exp,self.measured)
        observed=[{'variant':self.id,'query_id':'Q1','requests_observed':'1',
                   'requests_stable':True,'rows_observed':'12','rows_stable':True}]
        comparisons,counts=A.compare_plan_counts(frozen,observed)
        self.assertEqual(counts['match'],1)
        observed[0]['requests_observed']='2'
        comparisons,counts=A.compare_plan_counts(frozen,observed)
        self.assertEqual(counts['mismatch'],1)

    def test_missing_ts0_telemetry_not_zero(self):
        f=A.audit_frozen(self.exp,self.measured)
        observed=[{'variant':self.id,'query_id':'Q1','requests_observed':'',
                   'requests_stable':False,'rows_observed':'','rows_stable':False}]
        comparisons,counts=A.compare_plan_counts(f,observed)
        self.assertEqual(comparisons[0]['status'],'NOT_INSTRUMENTED')
        self.assertNotIn('match',counts)

    def test_writes_vs_load(self):
        f=A.audit_frozen(self.exp,self.measured)
        written=A.compare_writes(f,[{'variant':self.id,'physical_write_operations':'2','load_seconds':'0.25'}])
        self.assertEqual(written[0]['status'],'MATCH')

    def test_nearest_rank_p95_40(self):
        self.assertEqual(A.percentile_nearest_rank(list(range(1,41)),95),38)
        self.assertEqual(A.percentile_nearest_rank(list(range(1,41)),99),40)

    def test_actual_uploaded_observations_are_consistent(self):
        data=Path(__file__).resolve().parents[1]/'example_observed'
        m,raw,qs,agg,loads,stats=A.audit_observed(data)
        self.assertEqual(len(raw),2400)
        self.assertEqual(len(qs),60)
        self.assertEqual(stats['orders'],480)
        self.assertEqual(len(agg),5)
        ts0=next(x for x in qs if x['variant']=='R2H_6355f4e57096')
        self.assertEqual(ts0['requests_observed'],'')

    def test_refuse_writing_in_frozen_experiment(self):
        resultdir=self.exp/'results';resultdir.mkdir()
        with self.assertRaisesRegex(ValueError,'outside the immutable frozen experiment'):
            A.run(resultdir,self.exp,self.exp/'outputs/new_audit')

if __name__=='__main__':unittest.main()
