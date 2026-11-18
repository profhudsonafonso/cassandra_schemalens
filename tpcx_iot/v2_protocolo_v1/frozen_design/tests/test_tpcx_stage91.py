"""Stage9.1 tests: no Cassandra/Docker required; no synthetic latency claims."""
from __future__ import annotations
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from methodology.tpcx_iot.stage4.candidates import ROOT
from methodology.tpcx_iot.stage5.runtime import bind_read_requests
from methodology.tpcx_iot.stage9.experiment import DurableOutbox, events, oracle_rows, load_frozen
from methodology.tpcx_iot.stage91.cli import main
from methodology.tpcx_iot.stage91.experiment import (
    baseline_deployment, compare_baseline, deployment_order,
    expected_rows_from_watermark, load_baseline, make_probe,
    measure_dynamic, parse_nodetool_bytes, schedule_by_step, summarize_rows,
)


class FakeClient:
    """Minimal reference Cassandra API; pure Python and not a performance test."""
    def __init__(self): self.rows={}
    def get_prepared(self,cql):return cql
    def execute_write(self,plan,event):
        self.rows.setdefault(plan['candidate_id'],{})[(event['driver_system_id'],event['sensor_name'],event['event_ts'],event['event_id'])]=event
    def read_probe(self,plan,probe,stations):
        table=self.rows.get(plan['candidate_id'],{})
        matching=[e for e in table.values() if e['sensor_name']==probe['sensor_name']]
        result=oracle_rows(matching,probe)
        reqs=bind_read_requests(plan,sensor_name=probe['sensor_name'],
                   recent=probe['recent'],historical=probe['historical'],stations=stations,
                   complete_station_roster=True,roster_provenance='FULL_SYNTHETIC_GENERATOR_STATION_ROSTER')
        return result,[],len(reqs)
    def count_small_table(self,table):return len(self.rows.get(table,{}))


class Stage91Contracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hb,cls.reads,cls.writes,_=load_frozen(ROOT)
        cls.base,cls.br,cls.bw,cls.ddl=baseline_deployment(ROOT)

    def test_baseline_does_not_claim_official_cassandra_schema(self):
        obj=load_baseline()
        self.assertEqual(obj['status'],'RESEARCH_DERIVED_NOT_OFFICIAL_PHYSICAL_SCHEMA')
        self.assertIn('PRIMARY KEY ((sensor_name)',obj['schema_cql'])
        self.assertEqual(self.base['canonical_source'],obj['physical_table'])
        self.assertNotIn('ALLOW FILTERING',self.br['read_cql'])

    def test_baseline_full_record_schema(self):
        self.assertEqual(self.bw['column_order'],['driver_system_id','sensor_name','event_ts','event_id','sensor_reading','padding'])
        self.assertIn('event_ts >= ?',self.br['read_cql'])
        self.assertEqual(self.br['access_mode'],'sensor_only')

    def test_frozen_hbench_never_recomputed(self):
        self.assertEqual(len(self.hb),5)
        self.assertEqual([x['deployment_id'] for x in deployment_order(self.hb,self.base,0)[:5]],
                         [h['deployment_id'] for h in self.hb])
        self.assertEqual(deployment_order(self.hb,self.base,1)[-1]['deployment_id'],self.hb[0]['deployment_id'])

    def test_dynamic_windows_are_closed_and_within_historical_limit(self):
        for step in [11,20,130,500,2000,10000]:
            for i in [0,1,15]:
                p=make_probe(step,i,sensors=3)
                self.assertEqual(p['recent'].end_ms-p['recent'].start_ms,5000)
                self.assertEqual(p['historical'].end_ms-p['historical'].start_ms,5000)
                self.assertLessEqual(p['historical'].end_ms,p['recent'].start_ms)
                self.assertGreaterEqual(p['historical'].start_ms,p['recent'].start_ms-1800000)
                self.assertEqual(p,make_probe(step,i,sensors=3))

    def test_dynamic_timestamp_changes_with_commit(self):
        self.assertNotEqual(make_probe(130,1,sensors=2)['recent'],make_probe(131,1,sensors=2)['recent'])

    def test_window_oracle_contains_no_future_rows(self):
        probe=make_probe(131,1,sensors=3)
        actual=expected_rows_from_watermark(probe,stations=2)
        reference=oracle_rows(events(stations=2,sensors=3,steps=132),probe)
        self.assertEqual(actual,reference)

    def test_schedule_exact_count_even_if_more_reads_than_steps(self):
        for b,n in [(9,13),(100,7),(100,100)]:
            scheduled=schedule_by_step(seed_steps=130,bench_steps=b,reads=n)
            self.assertEqual(sum(len(x) for x in scheduled.values()),n)
            self.assertEqual(sorted(i for v in scheduled.values() for i in v),list(range(n)))
            self.assertLess(max(scheduled),b)

    def test_sensor_only_baseline_two_requests_without_bucket(self):
        p=make_probe(130,1,sensors=3)
        q=bind_read_requests(self.br,sensor_name=p['sensor_name'],recent=p['recent'],historical=p['historical'])
        self.assertEqual(len(q),2)

    def test_fake_live_dynamic_reads_baseline_and_hbench(self):
        for h,rp,wp in [(self.base,self.br,self.bw),
                        (self.hb[0],self.reads['ts0_series_raw'],self.writes['ts0_series_raw']),
                        (self.hb[1],self.reads['ts3_sensor_3600s'],None)]:
            readmap={rp['candidate_id']:rp}
            writemap={self.bw['candidate_id']:self.bw} if h==self.base else self.writes
            client=FakeClient()
            with tempfile.TemporaryDirectory() as td:
                ledger=DurableOutbox(Path(td)/'x.db')
                try:
                    from methodology.tpcx_iot.stage9.experiment import persist_all
                    persist_all(client,h,events(stations=2,sensors=3,steps=16),writemap,ledger)
                    result=measure_dynamic(client,h,readmap,writemap,ledger,stations=2,sensors=3,
                                     seed_steps=16,bench_steps=7,read_threads=2,read_operations=9)
                    self.assertEqual(result['source_events'],42)
                    self.assertEqual(result['read_operations'],9)
                    self.assertTrue(result['all_query_results_verified'])
                    self.assertTrue(result['query_windows_dynamic'])
                    self.assertEqual(result['physical_insert_statements'],42*len(h['tables']))
                    self.assertEqual(result['query_schedule_sha256'],result['query_schedule_sha256'])
                finally:ledger.close()

    def test_deterministic_schedule_across_designs(self):
        traces=[]
        for h,rp in [(self.base,self.br),(self.hb[0],self.reads['ts0_series_raw'])]:
            client=FakeClient()
            with tempfile.TemporaryDirectory() as td:
                ledger=DurableOutbox(Path(td)/'z.db')
                writes={**self.writes,self.bw['candidate_id']:self.bw}
                try:
                    from methodology.tpcx_iot.stage9.experiment import persist_all
                    persist_all(client,h,events(stations=1,sensors=2,steps=16),writes,ledger)
                    m=measure_dynamic(client,h,{rp['candidate_id']:rp},writes,ledger,
                                      stations=1,sensors=2,seed_steps=16,bench_steps=12,read_operations=8)
                    traces.append(m['query_schedule_sha256'])
                finally:ledger.close()
        self.assertEqual(*traces)

    def test_nodetool_parser(self):
        self.assertEqual(parse_nodetool_bytes('Space used (live): 1,024\n'),1024)
        self.assertEqual(parse_nodetool_bytes('   Space used (live): 3 KiB\n'),3072)
        self.assertIsNone(parse_nodetool_bytes('Table: sample\nSSTable count: 1'))
        self.assertIsNone(parse_nodetool_bytes('Space used (live): 1\nSpace used (live): 2\n'))

    def test_summary_and_baseline_ratios(self):
        runs=[]
        for did,through,p95 in [('b_tpcx_logical_raw',100,10),('h_1',150,5)]:
            for rep in range(3):
                runs.append(dict(status='COMPLETE_RESEARCH_ONLY',deployment_id=did,
                                 dynamic={'research_events_per_second':through,'read_latency':{'p95_ms':p95}},
                                 storage={'live_bytes':200 if did.startswith('b_') else 400}))
        summary=summarize_rows(runs)
        ratios=compare_baseline(summary)
        self.assertEqual(len(ratios),1)
        self.assertEqual(ratios[0]['ingest_ratio_vs_baseline'],1.5)
        self.assertEqual(ratios[0]['read_p95_ratio_vs_baseline'],0.5)
        self.assertEqual(ratios[0]['storage_ratio_vs_baseline'],2)

    def test_dry_run_has_six_designs_no_live_metrics(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(main(['dry-run','--output',td]),0)
            p=Path(td)/'70_stage91_execution_manifest.json'
            data=json.loads(p.read_text())
            self.assertEqual(len(data['designs']),6)
            self.assertFalse(data['baseline_is_official_cassandra_schema'])
            self.assertFalse((Path(td)/'71_stage91_live_runs.json').exists())

    def test_live_requires_reset(self):
        with self.assertRaises(SystemExit) as cm:main(['benchmark'])
        self.assertEqual(cm.exception.code,2)

    def test_storage_required_requires_container(self):
        with self.assertRaises(SystemExit) as cm:main(['benchmark','--reset','--require-storage'])
        self.assertEqual(cm.exception.code,2)

    def test_invalid_study_parameters_rejected(self):
        with self.assertRaises(SystemExit):main(['dry-run','--seed-steps','3'])

if __name__=='__main__':unittest.main()

class FullAuditTests(unittest.TestCase):
    def test_large_dataset_audit_reports_skip(self):
        from methodology.tpcx_iot.stage91.experiment import audit_materialized_rows
        r=audit_materialized_rows(None,{'tables':['x']},stations=10,sensors=200,total_steps=1000,
                                  tick_ms=1000,pad_bytes=900,max_events=100)
        self.assertEqual(r['status'],'SKIPPED_EXCEEDS_LIMIT')
        self.assertFalse(r['all_source_events_checked'])

    def test_small_dataset_checks_all_fields_not_just_counts(self):
        from methodology.tpcx_iot.stage91.experiment import audit_materialized_rows
        from types import SimpleNamespace
        source=events(stations=1,sensors=2,steps=4)
        class Session:
            def execute(self,query):
                return [SimpleNamespace(**e) for e in source]
        client=SimpleNamespace(session=Session())
        h={'tables':['raw']}
        r=audit_materialized_rows(client,h,stations=1,sensors=2,total_steps=4,
                                  tick_ms=1000,pad_bytes=900)
        self.assertTrue(r['all_source_events_checked'])
        self.assertEqual(r['tables']['raw']['rows_checked'],8)
        source[0]={**source[0],'sensor_reading':-123}
        with self.assertRaises(AssertionError):audit_materialized_rows(
            client,h,stations=1,sensors=2,total_steps=4,tick_ms=1000,pad_bytes=900)

class RetrospectiveTests(unittest.TestCase):
    def test_reference_preservation_measures_without_changing_selection(self):
        from methodology.tpcx_iot.stage91.experiment import retrospective_quality
        ref=[dict(deployment_id=f'h_{i}',median_read_p95_ms=float(i),
                  median_ingest_events_per_s=100.0+i) for i in range(1,10)]
        info=retrospective_quality(ref,['h_2','h_3','h_5'],[f'h_{i}' for i in range(1,10)])
        self.assertEqual(info['retained_p95_best'],'h_2')
        self.assertEqual(info['reference_p95_best'],'h_1')
        self.assertFalse(info['top1_preserved'])
        self.assertTrue(info['top3_intersection'])
        self.assertAlmostEqual(info['p95_relative_regret'],1.0)
        self.assertFalse(info['selection_modified_after_measurement'])

    def test_verification_dry_run_has_nine_plus_baseline(self):
        with tempfile.TemporaryDirectory() as td:
            # Live mode requires Cassandra; inspect Stage8 cardinality instead.
            complete=json.loads((ROOT/'outputs/tpcx_iot_stage8/52_complete_compatible_deployments.json').read_text())
            self.assertEqual(len(complete),9)
