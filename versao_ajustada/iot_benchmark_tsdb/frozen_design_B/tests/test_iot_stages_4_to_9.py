"""Contract tests. All pass without a Cassandra service; they are not performance results."""
from __future__ import annotations
import copy
import sys
import unittest
from pathlib import Path

from iot_pipeline.shared import ROOT,A1,OUT,inputs,read,read_rows,require_policy,path4,sha
from iot_pipeline import stage4,stage5,stage6,stage7,stage8,stage9
from iot_pipeline.offline import physical_state,read_records,from_rollup,offline_validate,compare
sys.path.insert(0,str(A1))
from core.bridge import _result

class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows=read_rows(inputs()['native'])
        cls.queries=read(inputs()['queries'])
        cls.state=physical_state(cls.rows,require_policy()['rollup_window_ms'])

    def test_01_original_activation_ts_only(self):
        activation=read(inputs()['activation']); self.assertEqual(len(activation),12)
        for r in activation:
            self.assertTrue(all(f.startswith('TS') for f in r['ts_families']))
            self.assertTrue(all(m.startswith('M') for m in r['modifiers']))
        self.assertIn('TS4',next(x for x in activation if x['query_id']=='Q8')['ts_families'])

    def test_02_instantiation_follows_activation(self):
        specs=read(path4(4,'10_candidate_specs.json'))
        families={x['family'] for x in specs}
        self.assertEqual(families,{'TS0','TS1','TS2','TS4','TS5'})
        self.assertEqual(len(specs),6)
        self.assertTrue(all('PRIMARY KEY' in x['ddl'] for x in specs))

    def test_03_no_physical_selection_unactivated(self):
        activated={f for r in read(inputs()['activation']) for f in r['ts_families']}
        specs=read(path4(4,'10_candidate_specs.json'))
        self.assertTrue(all(s['family'] in activated for s in specs))

    def test_04_q12_set_op_must_have_raw(self):
        matches=[p for p in read(path4(5,'15_query_plans.json')) if p['query_id']=='Q12' and p['route_status']!='INELIGIBLE']
        self.assertIn('TS0',[x['family'] for x in matches])

    def test_05_non_aligned_rollup_rejected(self):
        p=require_policy();q=next(q for q in self.queries if q['query_id']=='Q7')
        status,reason=stage5.route_family(q,'TS5',p)
        self.assertEqual(status,'INELIGIBLE');self.assertIn('unaligned',reason)

    def test_06_aligned_rollup_allowed_with_right_granularity(self):
        # Construct a NON-VACUOUS positive case at the policy's TS5 granularity.
        # Native Java GROUP BY can use a different interval; in that case the
        # original query must not be silently classified as a valid TS5 route.
        p=require_policy()
        native_q=copy.deepcopy(next(q for q in self.queries if q['query_id']=='Q7'))
        if native_q['group_by_ms'] != p['rollup_window_ms']:
            self.assertEqual(stage5.route_family(native_q,'TS5',p)[0], 'INELIGIBLE')
        q=copy.deepcopy(native_q)
        g=p['rollup_window_ms']
        self.assertGreater(g, 0)
        q['group_by_ms']=g
        eligible=[r for r in self.rows
                  if r['device_id'] in q['devices'] and r['sensor_name'] in q['sensors']]
        if not eligible:
            # The captured Java Q7 may target an empty device/sensor selection.
            # This is a SYNTHETIC POSITIVE fixture, not a claim about native Q7.
            self.assertTrue(self.rows, 'No source points to build TS5 positive fixture')
            anchor=self.rows[0]
            q['devices']=[anchor['device_id']]
            q['sensors']=[anchor['sensor_name']]
            eligible=[anchor]
        self.assertTrue(eligible, 'TS5 positive fixture must include source points')
        timestamp=min(r['timestamp'] for r in eligible)
        q['start_ms']=(timestamp//g)*g
        q['end_ms_exclusive']=q['start_ms']+2*g
        self.assertEqual(stage5.route_family(q,'TS5',p)[0], 'CONDITIONAL')
        res=from_rollup(self.state,q)
        expected=_result(self.rows,q)
        self.assertTrue(res, 'TS5 positive fixture must produce nonempty results')
        self.assertTrue(compare(res, expected))

    def test_07_ts4_historical_window_fallback(self):
        q=copy.deepcopy(next(q for q in self.queries if q['query_id']=='Q8'))
        q['end_ms_exclusive']=q['start_ms']+2001
        got=_result(read_records(self.state,'TS4',q),q);expected=_result(self.rows,q)
        self.assertTrue(compare(got,expected))

    def test_08_ts0_complete_queries(self):
        for q in self.queries:
            with self.subTest(qid=q['query_id']):
                self.assertTrue(compare(_result(read_records(self.state,'TS0',q),q),_result(self.rows,q)))

    def test_09_ts1_day_boundaries(self):
        for q in self.queries:
            if q['query_id'] in stage5.BUCKETED:
                self.assertTrue(compare(_result(read_records(self.state,'TS1',q),q),_result(self.rows,q)))

    def test_10_ts2_sparse_values(self):
        for q in self.queries:
            if q['query_id'] in stage5.WIDE:
                self.assertTrue(compare(_result(read_records(self.state,'TS2',q),q),_result(self.rows,q)))

    def test_11_stage6_writes_have_canonical_source(self):
        plans=read(path4(6,'18_write_plans.json'))
        self.assertEqual(len(plans),6)
        self.assertTrue(all(x['source_of_truth']=='canonical TS0' for x in plans))
        self.assertFalse(next(x for x in plans if x['family']=='TS5')['incremental_write_support'])

    def test_12_costs_not_latency(self):
        manifest=read(path4(7,'27_manifest.json'))
        self.assertTrue(manifest['no_runtime_or_latency_observations_used'])
        self.assertFalse(manifest['benchmark_measured'])

    def test_13_stage8_hashes_and_selection(self):
        selected=stage9.confirm_frozen();manifest=read(path4(8,'32_frozen_manifest.json'))
        self.assertEqual(manifest['frozen_hbench_sha256'],sha(path4(8,'28_hbench_frozen.json')))
        self.assertLessEqual(len(selected),require_policy()['top_k'])

    def test_14_stage8_full_valid_cover(self):
        selected=read(path4(8,'28_hbench_frozen.json'));qids={q['query_id'] for q in self.queries}
        for h in selected:
            self.assertEqual(qids,set(h['query_routes']))
            self.assertIn('TS0',h['families'])
            self.assertTrue(set(r['candidate_id'] for r in h['query_routes'].values())<=set(h['candidates']))

    def test_15_stage8_no_oracle(self):
        m=read(path4(8,'32_frozen_manifest.json'))
        self.assertFalse(m['benchmark_measured']);self.assertTrue(m['selection_is_not_observed_performance'])

    def test_16_offline_reference_validation(self):
        checks=offline_validate()
        self.assertEqual(len(checks),len(self.queries)*len(read(path4(8,'28_hbench_frozen.json'))))
        self.assertTrue(all(c['reference_match'] for c in checks))

    def test_17_unconfirmed_keyspace_rejected(self):
        class NoExecute:
            def execute(self,*args):raise AssertionError('Should not reach DB')
        with self.assertRaises(ValueError):stage9._create_keyspace(NoExecute(),'production_keyspace','production_keyspace')
        with self.assertRaises(ValueError):stage9._create_keyspace(NoExecute(),'sl_iot_h_000000000000','no')

    def test_18_percentiles(self):
        self.assertEqual(stage9._pctl(list(range(1,21)),95),19)
        self.assertEqual(stage9._pctl(list(range(1,21)),99),20)

    def test_19_nonsensical_relative_regret_not_generated(self):
        # No paper evaluation metrics exist prior to live measurements.
        self.assertFalse((OUT/'stage8/near_best.csv').exists())
        self.assertFalse((OUT/'stage8/measured_latency.csv').exists())

if __name__=='__main__':unittest.main()

class LiveQueryCompilerContractTests(unittest.TestCase):
    """A fake result-bearing Cassandra session validates generated SELECT argument contracts."""
    @classmethod
    def setUpClass(cls):
        cls.rows=read_rows(inputs()['native']);cls.qs=read(inputs()['queries'])
        cls.state=physical_state(cls.rows,require_policy()['rollup_window_ms'])

    def test_20_live_read_templates_against_fake_session(self):
        from types import SimpleNamespace as NS
        from iot_pipeline.offline import date_of
        state=self.state
        class FakeSession:
            def execute(self,sql,args):
                if 'FROM ts0_iot_raw' in sql:
                    d,s=args[:2]
                    bounds=args[2:] if len(args)>2 else None
                    return [NS(timestamp_ms=t,value_double=v) for (de,se,t),v in state['TS0'].items()
                            if de==d and se==s and (bounds is None or bounds[0]<=t<bounds[1])]
                if 'FROM ts1_iot_bucket' in sql:
                    d,s,day,start,end=args
                    return [NS(timestamp_ms=t,value_double=v) for (de,se,dd),ts in state['TS1'].items()
                            if de==d and se==s for t,v in ts.items() if date_of(t)==day and start<=t<end]
                if 'FROM ts2_iot_wide_unbucketed' in sql:
                    d,start,end=args
                    return [NS(timestamp_ms=t,**{f'm_{sensor}':val for sensor,val in measures.items()})
                            for (de,t),measures in state['TS2_UNBUCKETED'].items()
                            if de==d and start<=t<end]
                if 'FROM ts2_iot_wide' in sql:
                    d,day,start,end=args
                    return [NS(timestamp_ms=t,**{f'm_{sensor}':val for sensor,val in pairs.items()})
                            for (de,dd,t),pairs in state['TS2'].items()
                            if de==d and date_of(t)==day and start<=t<end]
                if 'FROM ts4_iot_latest' in sql:
                    d,s=args
                    t,v=state['TS4'][(d,s)]
                    return [NS(latest_timestamp_ms=t,value_double=v)]
                if 'FROM ts5_iot_rollup' in sql:
                    d,s,day,start,end=args
                    return [NS(window_start_ms=w,value_sum=x['sum'],sample_count=x['count'],minimum=x['min'],maximum=x['max'])
                            for (de,se,w),x in state['TS5'].items() if de==d and se==s and date_of(w)==day and start<=w<end]
                raise AssertionError('Unexpected Cassandra statement: '+sql)
        selected=read(path4(8,'28_hbench_frozen.json'))
        for h in selected:
            for q in self.qs:
                with self.subTest(h=h['deployment_id'],q=q['query_id']):
                    got=stage9.execute_query(FakeSession(),q,h['query_routes'][q['query_id']],['s0','s1'])
                    self.assertTrue(compare(got,_result(self.rows,q)))
