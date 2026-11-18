"""Stage 8 regression: correct, auditable conditional covers without leakage."""
from __future__ import annotations
import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from methodology.tpcx_iot.stage4.candidates import ROOT,read_json,sha256
from methodology.tpcx_iot.stage8.consolidator import (
    VERSION, AXES, checked_policy, generate, _admissible, _pareto,
    _dominates, _physical_signature, _select, _stage7_validated,
)

class Stage8Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root=ROOT
        cls.p=read_json(ROOT/'config/tpcx_stage8_consolidation_policy.json')
        cls.base=ROOT/'outputs/tpcx_iot_stage8'
        cls.specs=read_json(ROOT/'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json')
        cls.rp=read_json(ROOT/'outputs/tpcx_iot_stage5/20_queryplans.json')
        cls.wp=read_json(ROOT/'outputs/tpcx_iot_stage6/30_writeplans.json')
        cls.hs=read_json(cls.base/'54_hbench_selected.json')
        cls.all=read_json(cls.base/'52_complete_compatible_deployments.json')
        cls.front=read_json(cls.base/'53_pareto_preselection.json')
        cls.manifest=read_json(cls.base/'58_stage8_manifest.json')

    def test_policy_locked_no_leakage(self):
        checked_policy(self.p)
        for name,value in [('selection_uses_measured_results',True),('research_only',False),
                           ('require_projection_canonical_source',False),('preselection_mode','select_best_p95')]:
            changed=copy.deepcopy(self.p);changed[name]=value
            with self.assertRaises(ValueError):checked_policy(changed)

    def test_bounded_valid_policy(self):
        p=copy.deepcopy(self.p);p['top_k']=0
        with self.assertRaises(ValueError):checked_policy(p)
        p=copy.deepcopy(self.p);p['normalized_score_weights']['read_cql_requests']=0.75
        with self.assertRaises(ValueError):checked_policy(p)

    def test_no_silent_truncation(self):
        p=copy.deepcopy(self.p);p['exact_enumeration_limit_candidates']=3
        with self.assertRaises(ValueError):checked_policy(p)

    def test_source_stage7_integrity(self):
        specs,read,write,cost,hashes=_stage7_validated(self.root)
        self.assertEqual(len(specs),5);self.assertEqual(len(cost),5)
        self.assertEqual(set(x['candidate_id'] for x in read),set(cost))
        self.assertEqual(set(x['candidate_id'] for x in write),set(cost))
        self.assertEqual(self.manifest['source_hashes'],hashes)

    def test_candidate_signature_includes_bucket_and_access(self):
        specs={x['candidate_id']:x for x in self.specs}
        s1=specs['ts1_series_3600s'];s2=specs['ts1_series_60s']
        self.assertNotEqual(_physical_signature(s1),_physical_signature(s2))
        self.assertEqual(len(set(_physical_signature(x) for x in specs.values())),5)

    def test_origin_separate_from_coverage(self):
        cov=read_json(self.base/'51_validated_conditional_coverage.json')
        ts1=next(x for x in cov if x['candidate_id']=='ts1_series_60s')
        self.assertNotIn('Q_INGEST',ts1['origin_queries'])
        self.assertIn('Q_INGEST',ts1['cross_query_validated_conditional_coverage'])
        self.assertFalse(ts1['operational_coverage_proven'])

    def test_exact_enumeration_accounting(self):
        excl=read_json(self.base/'59_excluded_combinations.json')
        self.assertEqual(self.manifest['exact_nonempty_subsets_enumerated'],31)
        self.assertEqual(len(self.all),9)
        self.assertEqual(len(excl['excluded_as_incompatible'])+len(self.all),31)
        self.assertEqual(self.manifest['pareto_covers'],len(self.front['pareto_rows']))

    def test_projection_cannot_standalone(self):
        specs={x['candidate_id']:x for x in self.specs}
        writes={x['candidate_id']:x for x in self.wp}
        self.assertEqual(_admissible(('ts3_sensor_3600s',),specs,writes,self.p)[1],'missing_canonical_source')
        self.assertEqual(_admissible(('ts0_series_raw','ts1_series_60s'),specs,writes,self.p)[1],'multiple_canonical_sources')
        self.assertEqual(_admissible(('ts0_series_raw','ts3_sensor_3600s','ts3_sensor_60s'),specs,writes,self.p)[1],'multiple_alternate_projections')

    def test_complete_read_and_write_routing(self):
        rp={x['plan_id']:x for x in self.rp}
        wp={x['plan_id']:x for x in self.wp}
        for h in self.all:
            self.assertEqual(h['conditional_query_coverage'],['Q_INGEST','Q_READ_2_WINDOWS'])
            self.assertEqual(set(h['route']),{'Q_INGEST','Q_READ_2_WINDOWS'})
            rd=h['route']['Q_READ_2_WINDOWS'];wr=h['route']['Q_INGEST']
            self.assertIn(rd['candidate_id'],h['tables'])
            self.assertEqual(rp[rd['plan_id']]['candidate_id'],rd['candidate_id'])
            self.assertEqual({wp[i]['candidate_id'] for i in wr['all_required_write_plans']},set(h['tables']))
            self.assertFalse(h['atomicity_verified'])
            self.assertFalse(h['durable_repair_verified'])

    def test_non_dominated_frontier(self):
        ids={x['deployment_id'] for x in self.front['pareto_rows']}
        self.assertEqual(ids,{x['deployment_id'] for x in _pareto(self.all)})
        for p in self.front['pareto_rows']:
            self.assertFalse(any(_dominates(r,p) for r in self.all))
        self.assertEqual(len(ids),7)

    def test_control_and_diverse_hbench(self):
        self.assertTrue(0<len(self.hs)<=self.p['top_k'])
        self.assertIn(['ts0_series_raw'],[x['tables'] for x in self.hs])
        table_cover={c for r in self.hs for c in r['tables']}
        self.assertEqual(table_cover,{s['candidate_id'] for s in self.specs})
        self.assertEqual({x['deployment_id'] for x in self.hs}<=set(self.front['pareto_deployment_ids']),True)
        self.assertEqual({x['hbench_selection_order'] for x in self.hs},set(range(1,len(self.hs)+1)))

    def test_integer_structural_objectives(self):
        for d in self.all:
            self.assertEqual(set(d['objectives']),set(AXES))
            self.assertTrue(all(type(v) is int and v>0 for v in d['objectives'].values()))
            self.assertFalse(d['benchmark_used_for_selection'])
        self.assertTrue(self.manifest['selection_scores_are_hypothetical_structural_proxies'])

    def test_no_false_claims(self):
        for key in ('selection_uses_measured_results','official_tpcx_iot_kit_verified',
                    'live_cassandra_executed','operational_coverage_proven',
                    'distributed_atomicity_proven','durable_recovery_proven',
                    'stage3_reexecuted','activation_rules_changed'):
            self.assertIs(self.manifest[key],False)

    def test_all_output_ddl_paths_exist(self):
        import csv
        with (self.base/'57_stage9_execution_plan.csv').open(newline='') as stream:
            rows=list(csv.DictReader(stream))
        self.assertEqual(len(rows),len(self.hs))
        for row in rows:
            for relative in row['cql_ddl_paths'].split(';'):
                self.assertTrue((ROOT/relative).is_file(),relative)

    def test_hashes_unchanged_after_generate(self):
        before={p:str(sha256(ROOT/p)) for p in (Path('outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json'),
                    Path('outputs/tpcx_iot_stage5/20_queryplans.json'),
                    Path('outputs/tpcx_iot_stage6/30_writeplans.json'),
                    Path('outputs/tpcx_iot_stage7/46_cost_manifest.json'))}
        with tempfile.TemporaryDirectory() as t:
            manifest=generate(ROOT,Path(t)/'stage8')
            self.assertEqual(manifest['hbench_ids'],self.manifest['hbench_ids'])
        after={p:str(sha256(ROOT/p)) for p in before}
        self.assertEqual(before,after)

    def test_deterministic_output_files(self):
        with tempfile.TemporaryDirectory() as t:
            directory=Path(t)
            generate(ROOT,directory/'a')
            generate(ROOT,directory/'b')
            for name in self.manifest['files']:
                self.assertEqual(sha256(directory/'a'/name),sha256(directory/'b'/name),name)

    def test_refuse_upstream_output_overlap(self):
        with self.assertRaises(ValueError):generate(ROOT,ROOT/'outputs/tpcx_iot_stage7')

    def test_full_roster_not_yet_verified(self):
        for h in self.hs:
            if h['route']['Q_READ_2_WINDOWS']['candidate_id'].startswith('ts1') or h['route']['Q_READ_2_WINDOWS']['candidate_id'].startswith('ts0'):
                self.assertIn('complete_station_roster',h['route']['Q_READ_2_WINDOWS']['requirements'])

if __name__=='__main__':unittest.main()
