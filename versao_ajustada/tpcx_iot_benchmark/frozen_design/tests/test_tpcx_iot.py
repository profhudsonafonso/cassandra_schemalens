import tempfile
import unittest
from pathlib import Path
from methodology.tpcx_iot.pipeline import (ROOT, conceptual_schema, workload,
    analytical_record, temporal_evidence, run, read_json)
from methodology.common.rule_engine import activate

class TPCxIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.schema=conceptual_schema(None,None,False)
        self.policy=read_json(ROOT/'config/temporal_policy.json')
        self.rules=read_json(ROOT/'config/tsdb_activation_rules.json')

    def test_source_contains_exactly_two_operations_not_three_independent_queries(self):
        qs=workload('sensor-name-only')
        self.assertEqual([q['query_id'] for q in qs],['Q_INGEST','Q_READ_2_WINDOWS'])
        self.assertEqual(qs[1]['time_windows_seconds'],[5,5])
        self.assertIn(' OR ',qs[1]['logical_sql'])

    def test_raw_time_range_and_alternate_access_preserved(self):
        q=workload('sensor-name-only')[1]
        ev=temporal_evidence(self.schema,q,self.policy)
        active,_=activate(ev,self.rules)
        self.assertEqual(active,['TS0','TS1','TS3'])
        self.assertFalse(ev['series_identity_bound'])
        self.assertEqual(ev['metric_count'],1)
        self.assertEqual(ev['temporal_window_count'],2)

    def test_full_sensor_key_eliminates_alternate_metric_projection(self):
        q=workload('station-and-sensor')[1]
        ev=temporal_evidence(self.schema,q,self.policy)
        active,_=activate(ev,self.rules)
        self.assertEqual(active,['TS0','TS1'])
        self.assertTrue(ev['series_identity_bound'])

    def test_secondary_aggregate_templates_are_separate_and_clearly_labeled(self):
        qs=workload('station-and-sensor','aggregate_templates')
        self.assertEqual([q['query_id'] for q in qs],
                         ['Q_INGEST','Q_READ_MAX','Q_READ_MIN','Q_READ_AVG','Q_READ_COUNT'])
        for q in qs[1:]:
            self.assertIn('SECONDARY_ACADEMIC',q['source'])
            self.assertIn('THEN r.sensor_reading END',q['logical_sql'])
            ev=temporal_evidence(self.schema,q,self.policy)
            self.assertEqual(ev['aggregate_function_count'],2)
            active,_=activate(ev,self.rules)
            # Current TS5 rule REQUIRES TIME_BUCKET (not present in these queries).
            self.assertEqual(active,['TS0'])

    def test_ingest_does_not_activate_read_specific_families(self):
        q=workload('sensor-name-only')[0]
        active,_=activate(temporal_evidence(self.schema,q,self.policy),self.rules)
        self.assertEqual(active,['TS0'])

    def test_analytical_rows_from_explicit_eer_fragment(self):
        q=workload('station-and-sensor')[1]
        row=analytical_record(self.schema,q,'station-and-sensor')
        self.assertEqual((row['schema_rc'],row['selected_document_depth'],row['selected_re_at_d1']), (1,1,0))
        self.assertEqual(row['selected_root'],'SensorStream')
        row2=analytical_record(self.schema,q,'sensor-name-only')
        self.assertEqual(row2['selected_root'],'Reading')

    def test_uncertainties_do_not_trigger_unjustified_modifiers(self):
        ev=temporal_evidence(self.schema,workload('sensor-name-only')[1],self.policy)
        self.assertFalse(ev['hot_series'])
        self.assertEqual(ev['retention_days'],None)
        self.assertFalse(ev['late_arrival_allowed'])

    def test_manifest_and_snapshots_created(self):
        with tempfile.TemporaryDirectory(dir=ROOT) as td:
            summary=run(ROOT,Path(td)/'result','sensor-name-only')
            out=Path(td)/'result'
            self.assertEqual(summary['queries'],2)
            self.assertEqual(summary['activated_counts'],{'TS0':2,'TS1':1,'TS3':1})
            self.assertTrue((out/'inputs_snapshot/inputs/schema/tpcx_iot_eer.json').exists())
            self.assertTrue((out/'05_explanation_trace.json').exists())

if __name__=='__main__':unittest.main()
