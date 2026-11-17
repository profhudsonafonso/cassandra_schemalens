"""Offline Stage5 tests: query semantics, provenance, conditional coverage."""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
import copy
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from methodology.tpcx_iot.stage5.planner import (
    ROOT,build_plans,generate,validate_sql_intent,verify_read_cql,verify_stage4,
)
from methodology.tpcx_iot.stage5.offline import (
    event,fixtures,logical_oracle,mock_cassandra_result,physical_index,probe,run_offline_suite,
)
from methodology.tpcx_iot.stage5.runtime import (
    ClosedWindow,bind_read_requests,bucket_starts,merge_read_results,
    normalize_station_roster,
)


class Stage5PlansTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.specs,cls.audit=verify_stage4(ROOT)
        cls.plans={p.candidate_id:asdict(p) for p in build_plans(cls.specs)}

    def windows(self):
        return ClosedWindow(59_000,64_000),ClosedWindow(0,5_000)

    def bind(self,cid, stations=None,complete=False,provenance=None):
        a,b=self.windows()
        return bind_read_requests(self.plans[cid],sensor_name='temp',recent=a,historical=b,
                                  stations=stations,complete_station_roster=complete,
                                  roster_provenance=provenance)

    def test_five_plans_same_families_and_no_benchmark_claim(self):
        self.assertEqual(len(self.plans),5)
        self.assertEqual(Counter(p['family'] for p in self.plans.values()),{'TS0':1,'TS1':2,'TS3':2})
        self.assertTrue(all(p['read_coverage_status']=='CONDITIONAL_OFFLINE_ONLY' for p in self.plans.values()))
        self.assertTrue(all(p['write_coverage_status']=='STAGE6_UNVERIFIED' for p in self.plans.values()))
        self.assertTrue(all(not p['benchmark_used_for_selection'] for p in self.plans.values()))
        self.assertTrue(all(p['live_coverage_status']=='STAGE9_UNVERIFIED' for p in self.plans.values()))

    def test_read_primitive_exactly_matches_stage4(self):
        for s in self.specs:
            self.assertEqual(self.plans[s['candidate_id']]['read_cql'],s['read_cql_template'])
            verify_read_cql(s)

    def test_cql_must_include_every_partition_component(self):
        for spec in self.specs:
            invalid=dict(spec)
            invalid['read_cql_template']=invalid['read_cql_template'].replace(
                f"{spec['partition_key'][0]} = ? AND ","")
            with self.assertRaises(ValueError):verify_read_cql(invalid)

    def test_cql_reject_allow_filtering_and_pseudopushdowns(self):
        for tail in [' ALLOW FILTERING',' OR sensor_name = ?',' GROUP BY sensor_name']:
            invalid=dict(self.specs[0])
            invalid['read_cql_template']=invalid['read_cql_template'].replace(';',tail+';')
            with self.assertRaises(ValueError):verify_read_cql(invalid)

    def test_bucket_utc_closed_boundaries(self):
        self.assertEqual(bucket_starts(59_000,60_000,60),[0,60_000])
        self.assertEqual(bucket_starts(60_000,65_000,60),[60_000])
        self.assertEqual(bucket_starts(-60_000,0,60),[-60_000,0])
        self.assertEqual(bucket_starts(-60_500,-55_500,60),[-120_000,-60_000])
        self.assertEqual(bucket_starts(3_599_000,3_604_000,3600),[0,3_600_000])

    def test_invalid_window_duration_and_types(self):
        for a,b in [(0,4999),(0,5001),(0,0),(True,5001),(0.5,5000.5)]:
            with self.assertRaises(ValueError):ClosedWindow(a,b)

    def test_refuse_uncertified_roster_and_duplicate_station_ids(self):
        for stations,complete,prov in [(None,False,None),(['A'],False,None),
                                       (['A'],True,None),(['A','A'],True,'loader'),
                                       ([],True,'loader')]:
            with self.assertRaises(ValueError):
                self.bind('ts1_series_60s',stations,complete,prov)
        # TS3 needs no station list (but Stage6 must maintain the projection).
        self.assertEqual(len(self.bind('ts3_sensor_60s')),3)

    def test_fanout_two_windows_and_buckets(self):
        self.assertEqual(len(self.bind('ts0_series_raw',['A','B'],True,'fixture')),4)
        self.assertEqual(len(self.bind('ts1_series_60s',['A','B'],True,'fixture')),6)
        self.assertEqual(len(self.bind('ts1_series_3600s',['A','B'],True,'fixture')),4)
        self.assertEqual(len(self.bind('ts3_sensor_60s')),3)
        self.assertEqual(len(self.bind('ts3_sensor_3600s')),2)
        for cid,p in self.plans.items():
            rr=self.bind(cid,['A','B'],True,'fixture')
            self.assertTrue(all(len(r['params'])==r['cql'].count('?') for r in rr))
            self.assertEqual(set(r['window_id'] for r in rr),{'TI1_RECENT','TI2_HISTORICAL'})

    def test_invalid_historical_window_outside_1800s(self):
        with self.assertRaisesRegex(ValueError,'Historic'):
            bind_read_requests(self.plans['ts3_sensor_60s'],sensor_name='temp',
                recent=ClosedWindow(2_000_000,2_005_000),
                historical=ClosedWindow(100_000,105_000))
        with self.assertRaisesRegex(ValueError,'Historic'):
            bind_read_requests(self.plans['ts3_sensor_60s'],sensor_name='temp',
                recent=ClosedWindow(10_000,15_000),
                historical=ClosedWindow(11_000,16_000))

    def test_all_25_offline_probes_equal_independent_sql_oracle(self):
        records=run_offline_suite(list(self.plans.values()))
        self.assertEqual(len(records),25)
        self.assertTrue(all(x['passed'] for x in records))
        self.assertTrue(all(x['validation_scope'].startswith('DETERMINISTIC_OFFLINE') for x in records))

    def test_overlap_de_dupes_one_physical_event_not_equal_values(self):
        case=next(f for f in fixtures() if f[0]=='touching_windows_dedup')
        for cid,p in self.plans.items():
            name,recent,historical,rows=case
            ids=list(sorted(set(x['driver_system_id'] for x in rows)))
            reqs=bind_read_requests(p,sensor_name='temp',recent=recent,historical=historical,
                  stations=ids,complete_station_roster=True,roster_provenance='fixture')
            idx=physical_index(p,rows)
            actual=merge_read_results(p,[(req,mock_cassandra_result(p,req,idx)) for req in reqs],
                                      sensor_name='temp',recent=recent,historical=historical)
            self.assertEqual(len(actual),len(rows))
            self.assertEqual(sum(x['event_ts']==119_000 for x in actual),2)

    def test_same_value_same_time_different_event_id_preserves_multiplicity(self):
        case=fixtures()[0]
        for cid,p in self.plans.items():
            output=probe(p,*case)
            self.assertTrue(output['passed'],cid)
            self.assertGreater(output['actual_result_count'],5)

    def test_merge_reject_wrong_sensor_and_malformed_response(self):
        plan=self.plans['ts3_sensor_60s']
        recent,historical=self.windows()
        req=self.bind('ts3_sensor_60s')[0]
        data=event('A','voltage',60_000,1)
        data['bucket_start_ms']=60_000
        with self.assertRaisesRegex(ValueError,'sensor'):
            merge_read_results(plan,[(req,[data])],sensor_name='temp',recent=recent,historical=historical)
        data['sensor_name']='temp'
        del data['event_ts']
        with self.assertRaisesRegex(ValueError,'Incomplete'):
            merge_read_results(plan,[(req,[data])],sensor_name='temp',recent=recent,historical=historical)

    def test_generation_outputs_conditional_not_verified_full_workload(self):
        with tempfile.TemporaryDirectory() as td:
            report=generate(output_dir=Path(td)/'stage5')
            out=Path(td)/'stage5'
            coverage=json.loads((out/'21_read_coverage.json').read_text())
            self.assertEqual(report['offline_probes_passed'],25)
            self.assertFalse(report['operational_coverage_claimed'])
            self.assertFalse(report['write_plans_completed'])
            self.assertEqual(len(coverage),10)
            self.assertTrue(all(not x['eligible_for_complete_workload_HBench'] for x in coverage))
            self.assertEqual(sum(x['read_structural_coverage'] for x in coverage),5)
            self.assertFalse(report['benchmark_results_used'])

    def test_stage4_outputs_are_unchanged_by_stage5(self):
        upstream=ROOT/'outputs/tpcx_iot_stage4'
        before={p.relative_to(upstream):p.read_bytes() for p in upstream.rglob('*') if p.is_file()}
        with tempfile.TemporaryDirectory() as td:
            generate(output_dir=Path(td)/'stage5')
        after={p.relative_to(upstream):p.read_bytes() for p in upstream.rglob('*') if p.is_file()}
        self.assertEqual(before,after)

    def test_byte_determinism_of_outputs(self):
        with tempfile.TemporaryDirectory() as td1,tempfile.TemporaryDirectory() as td2:
            a,b=Path(td1),Path(td2)
            generate(output_dir=a);generate(output_dir=b)
            for f in a.glob('*.json'):
                self.assertEqual(f.read_bytes(),(b/f.name).read_bytes())

    def test_reject_tampered_stage4_candidate_without_reactivation(self):
        with tempfile.TemporaryDirectory() as td:
            target=Path(td)/'copy'
            shutil.copytree(ROOT,target)
            file=target/'outputs/tpcx_iot_stage4/10_cassandra_candidate_specs.json'
            specs=json.loads(file.read_text())
            specs[0]['partition_key']=['sensor_name']
            file.write_text(json.dumps(specs))
            with self.assertRaisesRegex(ValueError,'drifted'):
                verify_stage4(target)

    def test_reject_changed_source_query(self):
        with tempfile.TemporaryDirectory() as td:
            target=Path(td)/'query'
            snap=target/'outputs/tpcx_iot_stage1_3/inputs_snapshot/inputs/workload/sql'
            inp=target/'inputs/workload/sql'
            snap.mkdir(parents=True);inp.mkdir(parents=True)
            original=(ROOT/'inputs/workload/sql/Q_READ_2_WINDOWS.sql').read_bytes()
            (snap/'Q_READ_2_WINDOWS.sql').write_bytes(original)
            (inp/'Q_READ_2_WINDOWS.sql').write_bytes(original.replace(b'sensor_name',b'driver_system_id'))
            with self.assertRaisesRegex(ValueError,'changed'):
                validate_sql_intent(target)

    def test_reject_unknown_query_binding(self):
        changed=dict(self.plans['ts3_sensor_60s'])
        changed['query_id']='different'
        recent,historical=self.windows()
        with self.assertRaises(ValueError):
            bind_read_requests(changed,sensor_name='temp',recent=recent,historical=historical)


if __name__=='__main__':unittest.main()
