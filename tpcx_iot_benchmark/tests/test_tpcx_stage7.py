"""Stage7 controls: no measured input, deterministic arithmetic, frozen provenance."""
from __future__ import annotations
import copy
import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from methodology.tpcx_iot.stage4.candidates import ROOT,read_json,sha256
from methodology.tpcx_iot.stage7.estimator import (
    _buckets_inclusive,_model_partition_rows,_verify_stage6,validate_policy,
    evaluate_candidate,generate,
)


class TestTPCxStage7(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.policy=read_json(ROOT/'config/tpcx_stage7_cost_policy.json')
        cls.specs,cls.reads,cls.writes,cls.hashes=_verify_stage6(ROOT)
        cls.rr={x['candidate_id']:x for x in cls.reads}
        cls.ww={x['candidate_id']:x for x in cls.writes}
        cls.rows={x['candidate_id']:evaluate_candidate(x,cls.rr[x['candidate_id']],
                                cls.ww[x['candidate_id']],cls.policy) for x in cls.specs}

    def test_stage6_source_verified(self):
        self.assertEqual(len(self.specs),5)
        self.assertEqual(set(x['family'] for x in self.specs),{'TS0','TS1','TS3'})
        self.assertTrue(all(v and len(v)==64 for v in self.hashes.values()))

    def test_policy_is_hypothetical_and_does_not_rank(self):
        validate_policy(self.policy)
        self.assertFalse(self.policy['scenario_is_measured'])
        self.assertFalse(self.policy['rank_candidates'])
        self.assertFalse(self.policy['select_hbench'])

    def test_policy_rejects_tainted_or_ranked(self):
        for key,value in [('benchmark_results_used',True),('rank_candidates',True),
                          ('select_hbench',True),('scenario_is_measured',True),
                          ('system_compaction','TWCS_AUTO_TUNED')]:
            with self.subTest(key=key):
                p=copy.deepcopy(self.policy);p[key]=value
                with self.assertRaisesRegex(ValueError,'Unsafe'):validate_policy(p)

    def test_bad_or_overlapping_windows_fail_closed(self):
        bad=[[[7199,7203],[6200,6205]],[[7199,7204],[7200,7205]],
             [[20000,20005],[0,5]],[[0,5],[7199,7204]]]
        for item in bad:
            p=copy.deepcopy(self.policy);p['two_closed_query_windows_seconds_from_start']=item
            with self.subTest(item=item):
                with self.assertRaises(ValueError):validate_policy(p)

    def test_horizon_and_origin_alignment_requirements(self):
        p=copy.deepcopy(self.policy);p['horizon_seconds']=0
        with self.assertRaises(ValueError):validate_policy(p)
        p=copy.deepcopy(self.policy);p['starting_epoch_ms_utc']=1
        with self.assertRaisesRegex(ValueError,'whole UTC second'):validate_policy(p)

    def test_200_sensor_types_fixed_for_profile(self):
        p=copy.deepcopy(self.policy);p['sensor_types_per_station']=15
        with self.assertRaisesRegex(ValueError,'200'):validate_policy(p)

    def test_utc_floor_including_before_epoch(self):
        self.assertEqual(_buckets_inclusive(-1,0,60),[-1,0])
        self.assertEqual(_buckets_inclusive(59,61,60),[0,1])
        self.assertEqual(_buckets_inclusive(3599,3601,3600),[0,1])

    def test_row_counts_source_events_equal_for_all_candidates(self):
        for c in self.rows.values():
            self.assertEqual(c['footprint']['estimated_rows_materialized'],2_880_000)
            self.assertEqual(c['write']['single_candidate_inserts_per_logical_event'],1)
            self.assertIsNone(c['write']['estimated_iotps'])

    def test_ts0_snapshot_has_2000_partitions_and_1440_rows_each(self):
        f=self.rows['ts0_series_raw']['footprint']
        self.assertEqual((f['estimated_physical_partitions'],f['max_rows_per_partition_proxy']),
                         (2000,1440))
        self.assertIn('UNBOUNDED_PARTITION_OVER_TIME',
                      self.rows['ts0_series_raw']['feasibility']['warnings'])

    def test_ts1_60_partition_explosion_visible_without_exclusion(self):
        c=self.rows['ts1_series_60s']
        self.assertEqual(c['footprint']['estimated_physical_partitions'],2_880_000)
        self.assertEqual(c['footprint']['max_rows_per_partition_proxy'],1)
        self.assertIn('TOTAL_PARTITION_COUNT_WARNING',c['feasibility']['warnings'])
        self.assertFalse(c['feasibility']['hard_rejection'])

    def test_ts3_projections_and_canonical_dependency(self):
        self.assertEqual(self.rows['ts3_sensor_3600s']['footprint']['max_rows_per_partition_proxy'],600)
        for c in [self.rows['ts3_sensor_60s'],self.rows['ts3_sensor_3600s']]:
            self.assertEqual(c['write']['writes_if_combined_with_one_canonical_table'],2)
            self.assertTrue(c['write']['projected_copy_needs_canonical_source'])
            self.assertIn('DURABLE_PROJECTION_REPAIR_REQUIRED',c['feasibility']['warnings'])

    def test_60s_cql_counts_split_two_windows(self):
        self.assertEqual(self.rows['ts1_series_60s']['read']['per_window_cql_requests'],[20,10])
        self.assertEqual(self.rows['ts3_sensor_60s']['read']['per_window_cql_requests'],[2,1])
        self.assertEqual(self.rows['ts0_series_raw']['read']['total_cql_requests_per_logical_read'],20)

    def test_3600s_cql_counts_split_two_windows(self):
        self.assertEqual(self.rows['ts1_series_3600s']['read']['total_cql_requests_per_logical_read'],30)
        self.assertEqual(self.rows['ts3_sensor_3600s']['read']['total_cql_requests_per_logical_read'],3)

    def test_expected_matching_rows_use_exact_synthetic_timestamps(self):
        for c in self.rows.values():
            self.assertEqual(c['read']['expected_matching_rows_per_window'],[10,0])
            self.assertEqual(c['read']['expected_matching_rows_total'],10)
            self.assertEqual(c['read']['expected_cql_selected_payload_bytes_proxy'],10240)
            self.assertEqual(c['read']['expected_final_projected_bytes_proxy'],160)

    def test_fanout_changes_with_station_count(self):
        p=copy.deepcopy(self.policy);p['station_count']=5
        spec=next(x for x in self.specs if x['family']=='TS1')
        r=evaluate_candidate(spec,self.rr[spec['candidate_id']],
                             self.ww[spec['candidate_id']],p)
        self.assertEqual(r['read']['total_cql_requests_per_logical_read'],15)
        self.assertEqual(r['read']['expected_matching_rows_total'],5)
        ts3=next(x for x in self.specs if x['family']=='TS3')
        r=evaluate_candidate(ts3,self.rr[ts3['candidate_id']],
                             self.ww[ts3['candidate_id']],p)
        self.assertEqual(r['read']['total_cql_requests_per_logical_read'],3)

    def test_partition_row_proxies_match_bruteforce_when_shifted(self):
        p=copy.deepcopy(self.policy)
        p['sample_interval_seconds_per_stream']=5
        p['horizon_seconds']=3600
        p['starting_epoch_ms_utc']=-37_000
        for s in self.specs:
            n,max_rows,min_rows=_model_partition_rows(s,p)
            ts=[-37+i*5 for i in range(720)]
            width=s['bucket_seconds']
            if width is None:
                expected=[720]
            else:
                counts={}
                for t in ts:
                    k=t//width;counts[k]=counts.get(k,0)+1
                multiplicity=p['station_count'] if s['family']=='TS3' else 1
                expected=[v*multiplicity for v in counts.values()]
            factor=p['sensor_types_per_station'] * (p['station_count'] if s['family']!='TS3' else 1)
            self.assertEqual((n,max_rows,min_rows),(len(expected)*factor,max(expected),min(expected)),s['candidate_id'])

    def test_higher_row_payload_scales_storage_proxy_not_request_count(self):
        p=copy.deepcopy(self.policy);p['logical_record_bytes_assumed']*=2
        c=self.specs[0]
        a=self.rows[c['candidate_id']]
        b=evaluate_candidate(c,self.rr[c['candidate_id']],self.ww[c['candidate_id']],p)
        self.assertGreater(b['footprint']['rf_adjusted_logical_bytes_proxy'],
                           a['footprint']['rf_adjusted_logical_bytes_proxy'])
        self.assertEqual(a['read']['total_cql_requests_per_logical_read'],
                         b['read']['total_cql_requests_per_logical_read'])

    def test_unchanged_frozen_outputs_and_deterministic_artifacts(self):
        src=[ROOT/f'outputs/tpcx_iot_stage{i}'/file for i,file in [
            ('4','10_cassandra_candidate_specs.json'),('5','20_queryplans.json'),('6','30_writeplans.json')]]
        before=[sha256(x) for x in src]
        with tempfile.TemporaryDirectory() as t1, tempfile.TemporaryDirectory() as t2:
            m1=generate(ROOT,Path(t1));m2=generate(ROOT,Path(t2))
            self.assertEqual(m1,m2)
            for name in ('40_scenario_evidence.json','41_physical_footprint_proxies.json',
                         '42_read_cost_proxies.json','43_write_cost_proxies.json',
                         '44_feasibility_diagnostics.json','45_candidate_cost_matrix.csv',
                         '46_cost_manifest.json'):
                self.assertEqual(sha256(Path(t1)/name),sha256(Path(t2)/name))
            self.assertFalse(m1['benchmark_results_used'])
            self.assertFalse(m1['hbench_selected'])
            self.assertIsNone(m1['score_estimated_workload'])
        self.assertEqual(before,[sha256(x) for x in src])

    def test_prohibit_outputs_overwriting_prior_stage(self):
        with self.assertRaisesRegex(ValueError,'overlaps'):
            generate(ROOT,ROOT/'outputs/tpcx_iot_stage6')

    def test_covers_only_conditional_and_not_measured(self):
        for c in self.rows.values():
            self.assertEqual(c['feasibility']['status'],'CONDITIONAL_ANALYTICAL_ONLY')
            self.assertTrue(c['feasibility']['true_feasibility_not_proven'])
            self.assertIsNone(c['read']['estimated_latency_ms'])
            self.assertIsNone(c['read']['actual_rows_scanned'])

    def test_generated_csv_has_five_distinct_candidates(self):
        with tempfile.TemporaryDirectory() as t:
            generate(ROOT,Path(t))
            with (Path(t)/'45_candidate_cost_matrix.csv').open(newline='') as fd:
                items=list(csv.DictReader(fd))
            self.assertEqual(len(items),5)
            self.assertEqual(len(set(x['candidate_id'] for x in items)),5)
            self.assertTrue(all(x['operational_coverage_proven']=='false' for x in items))

    def test_stage6_manifest_tampering_detected(self):
        # Test with an isolated full source tree: does not mutate user project.
        with tempfile.TemporaryDirectory() as t:
            dest=Path(t)/'project'
            shutil.copytree(ROOT,dest,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
            path=dest/'outputs/tpcx_iot_stage6/33_writeplan_manifest.json'
            blob=read_json(path);blob['source_sha256']['stage5_manifest_sha256']='0'*64
            path.write_text(json.dumps(blob))
            with self.assertRaisesRegex(ValueError,'provenance'):
                generate(dest,dest/'outputs/tpcx_iot_stage7')

if __name__=='__main__': unittest.main()
