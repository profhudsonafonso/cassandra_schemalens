"""Offline Stage9 contract and failure-replay tests; NO live Cassandra claims."""
from __future__ import annotations

import json
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from unittest.mock import patch

from methodology.tpcx_iot.stage4.candidates import ROOT
from methodology.tpcx_iot.stage5.runtime import ClosedWindow, bind_read_requests
from methodology.tpcx_iot.stage9.cli import main
from methodology.tpcx_iot.stage9.experiment import (
    BASE_MS, DurableOutbox, duration_stats, events, keyspace,
    load_frozen, oracle_rows, percentile, probes,
)


class Stage9Contracts(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hbench,cls.reads,cls.writes,cls.hashes=load_frozen(ROOT)

    def test_00_exactly_five_preselected_deployments(self):
        self.assertEqual(len(self.hbench),5)
        self.assertEqual(len({h['deployment_id'] for h in self.hbench}),5)

    def test_01_hbench_unmodified_selection(self):
        self.assertEqual([h['hbench_selection_order'] for h in self.hbench],[1,2,3,4,5])
        self.assertFalse(any('measured_latency' in h for h in self.hbench))

    def test_02_routes_use_known_readplans(self):
        for h in self.hbench:
            t=h['route']['Q_READ_2_WINDOWS']['candidate_id']
            self.assertIn(t,h['tables'])
            self.assertEqual(self.reads[t]['read_cql'],h['route']['Q_READ_2_WINDOWS']['read_cql'])

    def test_03_all_required_write_tables(self):
        for h in self.hbench:
            actual=set(h['route']['Q_INGEST']['all_required_write_plans'])
            want={self.writes[t]['plan_id'] for t in h['tables']}
            self.assertEqual(actual,want)

    def test_04_experiment_keyspaces_are_safe_and_distinct(self):
        ks=[keyspace(h) for h in self.hbench]
        self.assertEqual(len(set(ks)),len(ks))
        for k in ks:self.assertRegex(k,r'^sl_tpcx_h_[0-9a-f]{12}$')
        with self.assertRaises(ValueError):keyspace(self.hbench[0],'danger;drop')

    def test_05_dataset_is_deterministic(self):
        self.assertEqual(events(steps=65),events(steps=65))
        self.assertNotEqual(events(steps=65),events(steps=66))

    def test_06_generator_has_global_source_ids(self):
        es=events(stations=3,sensors=4,steps=65)
        self.assertEqual(len(es),3*4*65)
        self.assertEqual(len({e['event_id'] for e in es}),len(es))

    def test_07_boundary_60s_crossing(self):
        p=probes()[0]
        self.assertEqual((p['recent'].start_ms-BASE_MS,p['recent'].end_ms-BASE_MS),(58000,63000))
        self.assertEqual((p['historical'].start_ms-BASE_MS,p['historical'].end_ms-BASE_MS),(11000,16000))
        self.assertEqual((p['recent'].end_ms-p['recent'].start_ms),5000)
        self.assertEqual((p['historical'].end_ms-p['historical'].start_ms),5000)
        self.assertLess(p['historical'].end_ms,p['recent'].start_ms)

    def test_08_sensor_only_ts3_has_fewer_partitions(self):
        h=self.hbench[1]
        plan=self.reads[h['route']['Q_READ_2_WINDOWS']['candidate_id']]
        p=probes()[0]
        req=bind_read_requests(plan,sensor_name=p['sensor_name'],recent=p['recent'],
                               historical=p['historical'])
        self.assertEqual(len(req),2) # both windows lie in same hourly bucket
        self.assertTrue(all(r['station_id'] is None for r in req))

    def test_09_roster_dependent_ts0(self):
        plan=self.reads['ts0_series_raw'];p=probes()[0]
        with self.assertRaises(ValueError):
            bind_read_requests(plan,sensor_name=p['sensor_name'],recent=p['recent'],
                               historical=p['historical'],stations=['ST000','ST001'])
        req=bind_read_requests(plan,sensor_name=p['sensor_name'],recent=p['recent'],
                               historical=p['historical'],stations=['ST000','ST001'],
                               complete_station_roster=True,roster_provenance='SYNTHETIC_GENERATOR')
        self.assertEqual(len(req),4)

    def test_10_oracle_does_not_collapse_equal_values_across_stations(self):
        es=events(stations=2,sensors=1,steps=65)
        p=probes(sensors=1)[0]
        out=oracle_rows(es,p)
        self.assertEqual(len(out),24)  # 2 stations x 2 closed 5s windows x 6 timestamps

    def test_11_statistics_are_unmeasured_if_empty(self):
        self.assertIsNone(percentile([],0.95))
        self.assertIsNone(duration_stats([])['p95_ms'])
        self.assertEqual(percentile([1,2,3,4,5],0.95),4.8)

    def test_12_dryrun_does_not_claim_live_results(self):
        with tempfile.TemporaryDirectory() as td:
            self.assertEqual(main(['dry-run','--output',td]),0)
            obj=json.loads((Path(td)/'60_stage9_execution_manifest.json').read_text())
            self.assertEqual(obj['manifest']['status'],'PREPARED_NOT_MEASURED')
            self.assertFalse(obj['manifest']['tpc_compliant'])
            self.assertEqual(len(obj['hbench']),5)
            self.assertFalse((Path(td)/'61_stage9_live_results.json').exists())

    def test_13_live_mode_requires_explicit_reset(self):
        with self.assertRaises(SystemExit) as cx:
            main(['smoke'])
        self.assertEqual(cx.exception.code,2)

    def test_14_reject_corrupted_source_manifest(self):
        # This test checks selected paths rather than overwriting immutable inputs.
        with patch('methodology.tpcx_iot.stage9.experiment.sha256',return_value='x'*64):
            with self.assertRaises(ValueError):load_frozen(ROOT)


class OutboxTests(unittest.TestCase):
    def test_15_pending_ack_and_replay(self):
        with tempfile.TemporaryDirectory() as td:
            path=Path(td)/'outbox.sqlite'
            two=events(stations=1,sensors=1,steps=2)
            box=DurableOutbox(path)
            for ev in two:box.persist(ev)
            self.assertEqual(box.unapplied_count(['ts0_series_raw','ts3_sensor_3600s']),4)
            ev0,table0=next(iter(box.pending(['ts0_series_raw','ts3_sensor_3600s'])))
            box.acknowledge(ev0,table0)
            self.assertEqual(box.unapplied_count(['ts0_series_raw','ts3_sensor_3600s']),3)
            box.close()
            reopened=DurableOutbox(path)
            self.assertEqual(reopened.unapplied_count(['ts0_series_raw','ts3_sensor_3600s']),3)
            for ev,table in list(reopened.pending(['ts0_series_raw','ts3_sensor_3600s'])):
                reopened.acknowledge(ev,table)
            self.assertEqual(reopened.unapplied_count(['ts0_series_raw','ts3_sensor_3600s']),0)
            reopened.close()

    def test_16_duplicate_event_replay_no_new_mutations(self):
        with tempfile.TemporaryDirectory() as td:
            box=DurableOutbox(Path(td)/'ledger.sqlite')
            e=events(stations=1,sensors=1,steps=1)[0]
            box.persist(e);box.persist(e)
            self.assertEqual(box.unapplied_count(['x']),1)
            box.acknowledge(e,'x');box.acknowledge(e,'x')
            self.assertEqual(box.unapplied_count(['x']),0)
            box.close()

    def test_17_conflicting_payload_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            box=DurableOutbox(Path(td)/'ledger.sqlite')
            e=events(stations=1,sensors=1,steps=1)[0]
            box.persist(e)
            conflict={**e,'sensor_reading':-99}
            with self.assertRaises(ValueError):box.persist(conflict)
            box.close()

    def test_18_failure_before_ack_stays_pending(self):
        with tempfile.TemporaryDirectory() as td:
            box=DurableOutbox(Path(td)/'ledger.sqlite')
            e=events(stations=1,sensors=1,steps=1)[0]
            box.persist(e)
            # Crash before acknowledge: pending retains BOTH physical tables.
            box.close()
            box=DurableOutbox(Path(td)/'ledger.sqlite')
            self.assertEqual(box.unapplied_count(['canonical','projection']),2)
            box.close()

    def test_19_all_deployments_have_distinct_fixed_outboxes(self):
        hbench,_,_,_=load_frozen(ROOT)
        paths=[f'{keyspace(h)}.sqlite' for h in hbench]
        self.assertEqual(len(set(paths)),5)

if __name__=='__main__':
    unittest.main()
