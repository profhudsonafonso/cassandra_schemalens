"""Offline correctness and provenance regression tests for Cassandra Stage 4."""
from __future__ import annotations

import copy
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from methodology.tpcx_iot.stage4.candidates import (
    ROOT, FAMILIES, checked_families, instantiate, prototypes, read_csv, read_json,
    verify_inputs,
)


class PhysicalCandidateStage4Tests(unittest.TestCase):
    def setUp(self):
        self.policy = read_json(ROOT/'config/tpcx_stage4_cassandra_policy.json')
        self.frozen = ROOT/'inputs/stage3_frozen'

    def test_five_candidates_only_from_precomputed_activated_families(self):
        with tempfile.TemporaryDirectory() as td:
            report=instantiate(output_dir=Path(td)/'physical')
            self.assertEqual(report['candidate_count'],5)
            self.assertEqual({k:v for k,v in report['candidate_counts_by_family'].items() if v},
                             {'TS0':1,'TS1':2,'TS3':2})
            self.assertFalse(report['query_plan_verified'])
            self.assertFalse(report['write_plan_verified'])
            self.assertFalse(report['benchmark_executed'])
            self.assertFalse(report['used_benchmark_results'])

    def test_upstream_matched_exact_original_source_matrices(self):
        expected=ROOT/'outputs/tpcx_iot_stage1_3'
        for filename in ['01_analytical_matrix.csv','02_temporal_evidence.csv',
                         '03_ts_activation_matrix.csv','04_modifier_matrix.csv',
                         '05_explanation_trace.json']:
            self.assertEqual((self.frozen/filename).read_bytes(),(expected/filename).read_bytes())

    def test_strict_input_provenance_and_rule_consistency(self):
        rows, audit=verify_inputs(ROOT,self.frozen,ROOT/'config/tpcx_stage4_cassandra_policy.json')
        self.assertEqual(set(rows['analytical']),{'Q_INGEST','Q_READ_2_WINDOWS'})
        self.assertEqual(len(audit['hashes']),11)
        act=rows['activation']['Q_READ_2_WINDOWS']
        self.assertEqual(checked_families(act,FAMILIES,'active_families'),{'TS0','TS1','TS3'})

    def test_pk_ck_support_range_and_full_sensor_access_without_filtering(self):
        specs=prototypes(self.policy)
        self.assertEqual(specs['ts0_series_raw'].partition_key,('driver_system_id','sensor_name'))
        self.assertEqual(specs['ts1_series_60s'].partition_key,
                         ('driver_system_id','sensor_name','bucket_start_ms'))
        self.assertEqual(specs['ts3_sensor_60s'].partition_key,('sensor_name','bucket_start_ms'))
        for c in specs.values():
            c.validate()
            self.assertTrue(c.read_cql_template.endswith('event_ts <= ?;'))
            self.assertNotIn('ALLOW FILTERING',c.ddl().upper())
            self.assertIn('event_id uuid',c.ddl())
            self.assertIn('padding text',c.ddl())
            self.assertNotIn('compaction',c.ddl())
            self.assertNotIn('default_time_to_live',c.ddl())
            self.assertEqual(c.benchmarked,False)

    def test_stable_id_protects_equal_timestamp_same_sensor(self):
        specs=prototypes(self.policy)
        for s in specs.values():
            self.assertIn('event_id',s.clustering_key)
            self.assertIn('event_ts',s.clustering_key)
        self.assertIn('driver_system_id',specs['ts3_sensor_60s'].clustering_key)

    def test_ingest_families_not_artificially_extended(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'generated'
            instantiate(output_dir=out)
            links=json.loads((out/'11_query_candidate_map.json').read_text())
            by_q={x['query_id']:x for x in links}
            self.assertEqual(by_q['Q_INGEST']['candidate_ids_by_family'],
                             {'TS0':['ts0_series_raw']})
            self.assertEqual(by_q['Q_READ_2_WINDOWS']['candidate_ids_by_family'],{
                'TS0':['ts0_series_raw'],
                'TS1':['ts1_series_3600s','ts1_series_60s'],
                'TS3':['ts3_sensor_3600s','ts3_sensor_60s']})
            self.assertTrue(all(x['candidate_origin_does_not_imply_query_coverage'] for x in links))

    def test_mutated_matrix_rejected_before_materialization(self):
        with tempfile.TemporaryDirectory() as td:
            fp=Path(td)/'frozen'
            shutil.copytree(self.frozen,fp)
            p=fp/'03_ts_activation_matrix.csv'
            text=p.read_text()
            self.assertIn('"[""TS0"", ""TS1"", ""TS3""]"',text)
            p.write_text(text.replace('Q_READ_2_WINDOWS,true,true', 'Q_READ_2_WINDOWS,true,false'))
            with self.assertRaisesRegex(ValueError,'Flag mismatch'):
                instantiate(output_dir=Path(td)/'out',frozen_dir=fp)

    def test_mutated_trace_rejected_before_materialization(self):
        with tempfile.TemporaryDirectory() as td:
            fp=Path(td)/'frozen';shutil.copytree(self.frozen,fp)
            p=fp/'05_explanation_trace.json'
            content=json.loads(p.read_text())
            content[1]['ts_rule_trace'][0]['passed']=False
            p.write_text(json.dumps(content))
            with self.assertRaisesRegex(ValueError,'Trace TS rules'):
                instantiate(output_dir=Path(td)/'out',frozen_dir=fp)

    def test_policy_cannot_select_by_benchmark_or_automatically_tune(self):
        for field in ['select_winner_by_latency','apply_compaction_automatically','apply_ttl_automatically']:
            edited={**self.policy,field:True}
            with self.assertRaisesRegex(ValueError,'outcome-based tuning|implicit'):
                prototypes(edited)

    def test_non_tpc_profile_rejected(self):
        edited={**self.policy,'source_profile':'station-and-sensor/raw_read'}
        with self.assertRaises(ValueError):
            prototypes(edited)

    def test_bucket_policy_rejects_invalid_widths(self):
        for bad in [[0],[True],[60,60],[],[-5]]:
            p={**self.policy,'bucket_width_seconds':bad}
            with self.assertRaises(ValueError):
                prototypes(p)

    def test_ddl_and_manifest_are_byte_deterministic(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            instantiate(output_dir=Path(a));instantiate(output_dir=Path(b))
            for x in Path(a).rglob('*'):
                if x.is_file():
                    y=Path(b)/x.relative_to(a)
                    self.assertTrue(y.is_file())
                    self.assertEqual(x.read_bytes(),y.read_bytes(),str(x))


if __name__=='__main__':unittest.main()
