"""Stage 6 regression: deterministic Cassandra WritePlans and reference maintenance."""
from __future__ import annotations

from collections import Counter
from dataclasses import asdict
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from uuid import UUID,uuid4

from methodology.tpcx_iot.stage5.offline import logical_oracle
from methodology.tpcx_iot.stage5.runtime import ClosedWindow
from methodology.tpcx_iot.stage6.planner import (
    ROOT, build_write_plans,generate,verify_insert,verify_policy,verify_source,
)
from methodology.tpcx_iot.stage6.materializer import (
    AuthoritativeRoster, ReferenceMaterializer, InMemoryMutationSink,
    PartialWriteFailure, bucket_start, event_uuid, json_ready, normalize_event,
)
from methodology.tpcx_iot.stage6.offline import run_offline_suite


class TestTPCxStage6(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.specs,cls.reads,cls.audit=verify_source(ROOT)
        cls.plans=[asdict(p) for p in build_write_plans(cls.specs,cls.reads)]
        cls.read_by_id={p['candidate_id']:p for p in cls.reads}
        cls.roster=AuthoritativeRoster(('ST2','ST1'),'declared_test_source_catalog',True)

    def e(self,station='ST1',sensor='temp',ts=60_000,reading=2.0,source='rec1',padding='raw'):
        return dict(driver_system_id=station,sensor_name=sensor,event_ts=ts,
                    sensor_reading=reading,source_record_id=source,padding=padding)

    def mk(self,candidates=None):
        return ReferenceMaterializer(self.plans,self.roster,candidates)

    def test_only_active_families_and_roles(self):
        self.assertEqual(len(self.plans),5)
        self.assertEqual(Counter(p['family'] for p in self.plans),Counter({'TS0':1,'TS1':2,'TS3':2}))
        self.assertEqual(sum(p['role']=='materialized_sensor_projection' for p in self.plans),2)
        self.assertTrue(all(not p['cassandra_verified'] and not p['benchmarked'] for p in self.plans))

    def test_frozen_write_cql_exact_column_order_and_placeholder_count(self):
        for spec in self.specs:
            self.assertEqual(verify_insert(spec),tuple(spec['columns']))
            p=next(p for p in self.plans if p['candidate_id']==spec['candidate_id'])
            self.assertEqual(p['write_cql'],spec['write_cql_template'])
            self.assertEqual(p['write_cql'].count('?'),len(p['column_order']))

    def test_cql_tampering_rejected(self):
        spec=dict(self.specs[0]);spec['write_cql_template']=spec['write_cql_template'].replace('?,','42,',1)
        with self.assertRaisesRegex(ValueError,'INSERT'):verify_insert(spec)

    def test_policy_fail_closed(self):
        policy=json.loads((ROOT/'config/tpcx_stage6_write_policy.json').read_text())
        verify_policy(policy)
        policy['use_benchmark_to_choose_candidates']=True
        with self.assertRaisesRegex(ValueError,'unsafe'):verify_policy(policy)

    def test_immutable_event_replay_zero_mutations(self):
        m=self.mk()
        first=m.apply([self.e()])
        self.assertEqual(len(first),5)
        self.assertEqual(m.apply([self.e()]),[])
        self.assertEqual(len(m.events),1)
        self.assertTrue(all(len(rows)==1 for rows in m.rows.values()))

    def test_two_different_events_same_timestamp_must_not_collapse(self):
        m=self.mk()
        a,b=self.e(source='unique-a'),self.e(source='unique-b')
        result=m.apply([a,b])
        self.assertEqual(len(result),10)
        self.assertEqual(len(m.events),2)
        self.assertTrue(all(len(rows)==2 for rows in m.rows.values()))
        recent=ClosedWindow(60_000,65_000); historical=ClosedWindow(0,5_000)
        for cid,plan in self.read_by_id.items():
            answer,_=m.mock_read(plan,sensor_name='temp',recent=recent,historical=historical)
            self.assertEqual(len(answer),2,cid)

    def test_multi_station_ts3_same_sensor_colocated(self):
        m=self.mk(['ts3_sensor_60s'])
        m.apply([self.e('ST1',source='a'),self.e('ST2',source='b')])
        rows=list(m.rows['ts3_sensor_60s'].values())
        self.assertEqual({x['driver_system_id'] for x in rows},{'ST1','ST2'})
        self.assertEqual({x['bucket_start_ms'] for x in rows},{60_000})
        res,requests=m.mock_read(self.read_by_id['ts3_sensor_60s'],sensor_name='temp',
                                 recent=ClosedWindow(60_000,65_000),historical=ClosedWindow(0,5_000))
        self.assertEqual(len(res),2)
        self.assertEqual(requests,2)

    def test_bucket_floor_boundaries_including_negative_timestamp(self):
        self.assertEqual(bucket_start(-1,60),-60_000)
        self.assertEqual(bucket_start(0,60),0)
        self.assertEqual(bucket_start(59_999,60),0)
        self.assertEqual(bucket_start(60_000,60),60_000)
        self.assertEqual(bucket_start(3_600_000,3600),3_600_000)
        self.assertIsNone(bucket_start(12,None))

    def test_stable_uuid_independent_of_reading_and_explicit_equivalence(self):
        a,b=self.e(reading=1),self.e(reading=100)
        id1=normalize_event(a)['event_id'];id2=normalize_event(b)['event_id']
        self.assertEqual(id1,id2)
        a['event_id']=str(id1)
        self.assertEqual(normalize_event(a)['event_id'],id1)
        a['event_id']=str(uuid4())
        with self.assertRaisesRegex(ValueError,'differs'):normalize_event(a)

    def test_event_identity_requires_stable_source_id_or_uuid(self):
        no_identity=self.e();del no_identity['source_record_id']
        with self.assertRaisesRegex(ValueError,'Stable'):normalize_event(no_identity)
        no_identity['event_id']=str(uuid4())
        self.assertIsInstance(normalize_event(no_identity)['event_id'],UUID)

    def test_reject_same_event_id_changed_payload_without_mutation(self):
        m=self.mk()
        m.apply([self.e()])
        snapshot={cid:dict(rows) for cid,rows in m.rows.items()}
        with self.assertRaisesRegex(ValueError,'Conflicting rewrite'):
            m.apply([self.e(reading=111)])
        self.assertEqual(m.rows,snapshot)

    def test_atomic_batch_prevalidation_offline(self):
        m=self.mk()
        with self.assertRaisesRegex(ValueError,'Conflicting rewrite'):
            m.apply([self.e(source='a'),self.e(source='a',reading=999)])
        self.assertFalse(m.events)
        self.assertTrue(all(not x for x in m.rows.values()))

    def test_station_must_be_in_explicit_complete_roster(self):
        m=self.mk()
        with self.assertRaisesRegex(ValueError,'missing'):
            m.apply([self.e(station='ST3')])
        self.assertEqual(len(m.events),0)

    def test_roster_rejects_fabricated_completeness(self):
        for stations,complete,provenance in [(['ST1'],False,'source'),(['ST1'],True,''),
                                              (['ST1','ST1'],True,'source'),([],True,'source')]:
            with self.assertRaises(ValueError):AuthoritativeRoster(tuple(stations),provenance,complete)

    def test_nan_bool_timestamp_padding_and_unsupported_delete_rejected(self):
        for key,bad in [('sensor_reading',float('nan')),('sensor_reading',True),
                        ('event_ts',True),('event_ts',2**64),('padding',None),
                        ('operation','DELETE')]:
            evt=self.e();evt[key]=bad
            with self.subTest(key=key,bad=str(bad)):
                with self.assertRaises(ValueError):normalize_event(evt)

    def test_source_uuid_cannot_point_at_two_physical_events(self):
        m=self.mk()
        eid=uuid4()
        a=self.e(source='a');b=self.e(station='ST2',source='b')
        del a['source_record_id'];del b['source_record_id']
        a['event_id']=str(eid);b['event_id']=str(eid)
        with self.assertRaisesRegex(ValueError,'reused'):m.apply([a,b])
        self.assertFalse(m.events)

    def test_partial_failure_does_not_claim_distributed_atomicity(self):
        m=self.mk()
        actions=m.apply([self.e(source='a'),self.e(source='b',station='ST2')])
        sink=InMemoryMutationSink()
        with self.assertRaises(PartialWriteFailure):sink.apply(actions,fail_after=3)
        self.assertFalse(sink.is_equal_to(m))
        sink.apply(m.recovery_replay())
        self.assertTrue(sink.is_equal_to(m))
        sink.apply(m.recovery_replay())
        self.assertTrue(sink.is_equal_to(m))

    def test_single_table_candidate_is_writable_without_eagerly_materializing_all(self):
        m=self.mk(['ts1_series_60s'])
        actions=m.apply([self.e()])
        self.assertEqual([x['candidate_id'] for x in actions],['ts1_series_60s'])
        self.assertEqual(set(m.rows),{'ts1_series_60s'})

    def test_read_write_cross_check_25_scenarios(self):
        probes,example=run_offline_suite(self.plans,self.reads)
        self.assertEqual(len(probes),32)
        self.assertTrue(all(x['passed'] for x in probes))
        self.assertTrue(all(x['scope']=='OFFLINE_REFERENCE_ONLY' for x in probes))
        self.assertEqual(len(example),10)

    def test_pipeline_generation_flags_and_hashes(self):
        with tempfile.TemporaryDirectory() as td:
            out=Path(td)/'stage6';manifest=generate(output_dir=out)
            self.assertEqual(manifest['write_plan_count'],5)
            self.assertEqual(manifest['offline_probes_passed'],32)
            self.assertFalse(manifest['live_cassandra_executed'])
            self.assertFalse(manifest['distributed_atomicity_proven'])
            self.assertFalse(manifest['hbench_selected'])
            coverage=json.loads((out/'31_write_coverage.json').read_text())
            self.assertTrue(all(not x['eligible_for_complete_workload_HBench'] for x in coverage))

    def test_reject_stage5_mutation_before_write_generation(self):
        with tempfile.TemporaryDirectory() as td:
            target=Path(td)/'copy'
            shutil.copytree(ROOT,target)
            plan_path=target/'outputs/tpcx_iot_stage5/20_queryplans.json'
            data=json.loads(plan_path.read_text());data[0]['physical_table']='fake'
            plan_path.write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError,'drifted'):verify_source(target)

    def test_upstream_hashes_unchanged_after_generation(self):
        upstream=[ROOT/'outputs/tpcx_iot_stage1_3',ROOT/'outputs/tpcx_iot_stage4',ROOT/'outputs/tpcx_iot_stage5']
        def inventory():return {str(p):p.read_bytes() for folder in upstream for p in folder.rglob('*') if p.is_file()}
        before=inventory()
        with tempfile.TemporaryDirectory() as td:generate(output_dir=Path(td))
        self.assertEqual(before,inventory())

    def test_output_deterministic_at_byte_level(self):
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b:
            generate(output_dir=Path(a));generate(output_dir=Path(b))
            self.assertEqual({p.name:p.read_bytes() for p in Path(a).glob('*.json')},
                             {p.name:p.read_bytes() for p in Path(b).glob('*.json')})


if __name__=='__main__':unittest.main()
