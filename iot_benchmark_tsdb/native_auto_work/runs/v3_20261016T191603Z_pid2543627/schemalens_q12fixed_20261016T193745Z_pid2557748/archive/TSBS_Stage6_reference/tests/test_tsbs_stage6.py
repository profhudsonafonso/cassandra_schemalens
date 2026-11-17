"""Stage 6 regression and synthetic semantic tests; NO Cassandra server required."""
from __future__ import annotations

import json
import math
from datetime import date
import tempfile
import unittest
from pathlib import Path

from methodology.tsbs.stage5.planner import NS_DAY, NS_HOUR, NS_MINUTE
from methodology.tsbs.stage5.runtime import evaluate_with_fetcher
from methodology.tsbs.stage6.materializer import ReferenceMaterializer, bucket_date, normalize_event
from methodology.tsbs.stage6.cassandra_executor import ExperimentalCassandraExecutor
from methodology.tsbs.stage6.planner import generate, sha256
from methodology.tsbs.stage6.tsbs_adapter import parse_cassandra_cpu_lines

ROOT = Path(__file__).resolve().parents[1]


class Stage6Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as temporary:
            cls.report = generate(ROOT, Path(temporary))
            cls.writeplans = json.loads((Path(temporary) / "18_write_plans.json").read_text())
            cls.coverage = (Path(temporary) / "19_write_coverage_matrix.csv").read_text()
        cls.schema = json.loads((ROOT / "inputs/schema/tsbs_cpu_only_schema.json").read_text())
        cls.metrics = cls.schema["entities"]["CPUReading"]["temporal"]["metrics"]
        cls.readplans = json.loads((ROOT / "outputs/tsbs_stage5/15_query_plans.json").read_text())

    @classmethod
    def reading(cls, host="host_0", ns=0, usage_user=22.0, **changes):
        v = {metric: float(i + 10) for i, metric in enumerate(cls.metrics)}
        v.update({"usage_user": float(usage_user), **changes})
        return {"hostname": host, "timestamp_ns": ns, **v}

    @classmethod
    def plan(cls, qid, cid):
        plans = [p for p in cls.readplans if p["query_id"] == qid and p["candidate_id"] == cid]
        assert len(plans) == 1, (qid, cid)
        return plans[0]

    def new_materializer(self, candidates=None):
        return ReferenceMaterializer(self.writeplans, self.metrics, candidates)

    def test_plan_cardinality_and_ts_only(self):
        self.assertEqual(self.report["candidate_count"], 9)
        self.assertEqual(self.report["query_plan_links"], 37)
        self.assertEqual((self.report["raw_candidates"], self.report["derived_candidates"]), (4, 5))
        self.assertFalse(self.report["live_CQL_executed"])
        self.assertFalse(self.report["C0_C9_used"])
        self.assertEqual({p["family"] for p in self.writeplans}, {"TS0", "TS1", "TS2", "TS5", "TS6"})
        self.assertEqual(len(self.coverage.strip().splitlines()), 38)
        for p in self.writeplans:
            self.assertEqual(p["write_cql"].count("?"), len(p["column_order"]))
            self.assertEqual(p["delete_cql"].count("?"), len(p["identity_columns"]))
            self.assertFalse(p["live_driver_implemented"])
            self.assertFalse(p["distributed_atomic_maintenance_proven"])

    def test_sources_remain_unchanged(self):
        files = ["outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
                 "outputs/tsbs_stage5/15_query_plans.json", "config/tsdb_activation_rules.json",
                 "config/temporal_modifier_rules.json", "outputs/tsbs_run/04_tsdb_activation_matrix.csv"]
        before = {name: sha256(ROOT / name) for name in files}
        with tempfile.TemporaryDirectory() as temporary:
            generate(ROOT, Path(temporary))
        self.assertEqual(before, {name: sha256(ROOT / name) for name in files})

    def test_one_complete_sample_and_repeat_are_idempotent(self):
        engine = self.new_materializer()
        sample = self.reading()
        changed = engine.apply([sample])
        self.assertEqual(len(changed), 63)
        self.assertTrue(all(x["execution_status"] == "NOT_EXECUTED_REFERENCE_ONLY" for x in changed))
        self.assertEqual(engine.host_roster(), ["host_0"])
        self.assertEqual(engine.apply([sample, sample]), [])
        self.assertEqual(len(engine.events), 1)
        self.assertEqual({cid: len(rows) for cid, rows in engine.rows.items()},
                         {"ts0_metric_raw": 10,
                          "ts1_metric_bucket_1d": 10,
                          "ts1_metric_bucket_7d": 10,
                          "ts2_wide_bucket_1d": 1,
                          "ts5_max_by_host_minute": 10,
                          "ts5_max_by_host_hour": 10,
                          "ts5_sum_count_by_host_hour": 10,
                          "ts5_global_max_minute": 1,
                          "ts6_hour_host_dimension": 1})
        removed = engine.apply([{"operation": "delete", "hostname": "host_0", "timestamp_ns": 0}])
        self.assertEqual(len(removed), 63)
        self.assertTrue(all(x["operation"] == "DELETE" for x in removed))
        self.assertEqual(engine.host_roster(), [])
        self.assertTrue(all(not rows for rows in engine.rows.values()))

    def test_last_event_wins_in_one_batch_and_duplicate_is_noop(self):
        engine = self.new_materializer(["ts5_max_by_host_minute"])
        a, b = self.reading(usage_user=40), self.reading(usage_user=80)
        actions = engine.apply([a, b, b])
        self.assertEqual(len(actions), 10)
        key = ("host_0", "usage_user", "1970-01-01", 0)
        self.assertEqual(engine.rows["ts5_max_by_host_minute"][key]["max_value"], 80)
        self.assertEqual(engine.apply([b]), [])

    def test_backfill_correction_and_retract_max(self):
        engine = self.new_materializer()
        low = self.reading(ns=10_000_000_000, usage_user=15)
        high = self.reading(ns=20_000_000_000, usage_user=97)
        self.assertTrue(engine.apply([low, high]))
        gkey = ("usage_user", "1970-01-01", 0)
        self.assertEqual(engine.rows["ts5_global_max_minute"][gkey]["max_value"], 97)
        lower = self.reading(ns=20_000_000_000, usage_user=12)
        updates = engine.apply([lower])
        self.assertTrue(any(m["candidate_id"] == "ts5_global_max_minute" and m["row"]["max_value"] == 15 for m in updates))
        self.assertEqual(engine.rows["ts5_global_max_minute"][gkey]["max_value"], 15)
        earliest = self.reading(ns=5_000_000_000, usage_user=99)
        engine.apply([earliest])  # late arrival (earlier timestamp)
        self.assertEqual(engine.rows["ts5_global_max_minute"][gkey]["max_value"], 99)
        engine.apply([{"operation": "delete", "hostname": "host_0", "timestamp_ns": 5_000_000_000}])
        self.assertEqual(engine.rows["ts5_global_max_minute"][gkey]["max_value"], 15)

    def test_avg_sum_count_updates_do_not_double_count(self):
        engine = self.new_materializer()
        a = self.reading(ns=0, usage_user=10)
        b = self.reading(ns=NS_MINUTE, usage_user=20)
        c = self.reading(ns=2 * NS_MINUTE, usage_user=30)
        engine.apply([a, b, c, a])
        key = ("host_0", "usage_user", "1970-01-01", 0)
        r = engine.rows["ts5_sum_count_by_host_hour"][key]
        self.assertEqual((r["value_sum"], r["sample_count"]), (60, 3))
        modified = self.reading(ns=NS_MINUTE, usage_user=40)
        engine.apply([modified])
        r = engine.rows["ts5_sum_count_by_host_hour"][key]
        self.assertEqual((r["value_sum"], r["sample_count"]), (80, 3))
        dkey = ("1970-01-01", 0, "host_0")
        d = engine.rows["ts6_hour_host_dimension"][dkey]
        self.assertEqual((d["sum_usage_user"], d["count_usage_user"]), (80, 3))
        engine.apply([{"operation": "delete", "hostname": "host_0", "timestamp_ns": NS_MINUTE}])
        self.assertEqual(engine.rows["ts5_sum_count_by_host_hour"][key]["sample_count"], 2)
        self.assertEqual(engine.rows["ts5_sum_count_by_host_hour"][key]["value_sum"], 40)

    def test_same_source_populates_distinct_days_and_seven_day_bucket(self):
        engine = self.new_materializer(["ts1_metric_bucket_1d", "ts1_metric_bucket_7d"])
        records = [self.reading(ns=NS_DAY - 1), self.reading(ns=NS_DAY), self.reading(ns=8 * NS_DAY)]
        engine.apply(records)
        self.assertEqual(bucket_date(8 * NS_DAY, 7), "1970-01-08")
        self.assertIn(("host_0", "usage_user", "1970-01-01", NS_DAY), engine.rows["ts1_metric_bucket_7d"])
        self.assertIn(("host_0", "usage_user", "1970-01-02", NS_DAY), engine.rows["ts1_metric_bucket_1d"])

    def test_reject_partial_invalid_cpu_records(self):
        invalid = self.reading()
        invalid.pop("usage_idle")
        with self.assertRaisesRegex(ValueError, "Full dense"):
            normalize_event(invalid, self.metrics)
        invalid = self.reading(usage_user=math.nan)
        with self.assertRaisesRegex(ValueError, "finite"):
            normalize_event(invalid, self.metrics)
        invalid = self.reading()
        invalid["timestamp_ns"] = True
        with self.assertRaisesRegex(ValueError, "timestamp_ns"):
            normalize_event(invalid, self.metrics)
        with self.assertRaisesRegex(ValueError, "unexpected"):
            normalize_event({"operation": "delete", "hostname": "host_0", "timestamp_ns": 0, "usage_user": 1}, self.metrics)

    def test_stage5_read_results_match_reference_write_derived_rows(self):
        engine = self.new_materializer()
        rows = [self.reading(host="host_0", ns=10_000_000_000, usage_user=50),
                self.reading(host="host_0", ns=20_000_000_000, usage_user=95),
                self.reading(host="host_0", ns=NS_MINUTE + 5_000_000_000, usage_user=65),
                self.reading(host="host_1", ns=10_000_000_000, usage_user=99),
                self.reading(host="host_1", ns=20_000_000_000, usage_user=11),
                self.reading(host="host_1", ns=NS_MINUTE + 5_000_000_000, usage_user=42)]
        engine.apply(rows)
        fetch = engine.query_mock_rows
        cases = [
            ("Q01", ["ts0_metric_raw", "ts5_max_by_host_minute"],
             {"hostnames": ["host_0"], "start_ns": 0, "end_ns": 2 * NS_MINUTE}),
            ("Q10", ["ts0_metric_raw", "ts5_sum_count_by_host_hour", "ts6_hour_host_dimension"],
             {"hostnames": ["host_0", "host_1"], "start_ns": 0, "end_ns": NS_HOUR,
              "complete_host_roster": True}),
            ("Q13", ["ts0_metric_raw", "ts1_metric_bucket_1d", "ts2_wide_bucket_1d"],
             {"hostnames": ["host_0"], "start_ns": 0, "end_ns": NS_HOUR,
              "dense_cpu_metrics": True}),
            ("Q16", ["ts0_metric_raw", "ts5_global_max_minute"],
             {"hostnames": ["host_0", "host_1"], "end_ns": 3 * NS_MINUTE,
              "dataset_start_ns": 0, "complete_host_roster": True}),
        ]
        for qid, cids, params in cases:
            with self.subTest(query=qid):
                actual = [evaluate_with_fetcher(self.plan(qid, cid), fetch, **params) for cid in cids]
                for observed in actual[1:]:
                    self.assertEqual(observed, actual[0])
        latest = evaluate_with_fetcher(self.plan("Q15", "ts0_metric_raw"), fetch,
                                       hostnames=["host_0", "host_1"],
                                       dense_cpu_metrics=True, complete_host_roster=True)
        self.assertEqual({r["hostname"]: r["timestamp_ns"] for r in latest},
                         {"host_0": NS_MINUTE + 5_000_000_000, "host_1": NS_MINUTE + 5_000_000_000})

    def test_parser_from_native_tsbs_cassandra_format(self):
        ts = 1_451_606_400_000_000_000  # 2016-01-01 00:00:00 UTC
        def line(m):
            return f"series_double,cpu,hostname=host_0,region=eu-west-1,team=NYC,{m},2016-01-01,{ts},7.5\n"
        observations = parse_cassandra_cpu_lines((line(m) for m in self.metrics), self.metrics)
        self.assertEqual(len(observations), 1)
        self.assertEqual(observations[0]["usage_user"], 7.5)
        self.assertEqual(len(self.new_materializer().apply(observations)), 63)
        with self.assertRaisesRegex(ValueError, "Incomplete"):
            parse_cassandra_cpu_lines([line(self.metrics[0])], self.metrics)
        with self.assertRaisesRegex(ValueError, "Inconsistent static tags"):
            parse_cassandra_cpu_lines([line(m) for m in self.metrics] +
                                      [line(self.metrics[0]).replace("team=NYC", "team=SF")], self.metrics)
        with self.assertRaisesRegex(ValueError, "Conflicting duplicate"):
            parse_cassandra_cpu_lines([line(m) for m in self.metrics] +
                                      [line(self.metrics[0]).replace("7.5", "8.5")], self.metrics)

    def test_selectable_subset_and_multirow_replay(self):
        subset = ["ts0_metric_raw", "ts5_global_max_minute"]
        engine = self.new_materializer(subset)
        batch = [self.reading(host=f"host_{i}", ns=(j * NS_MINUTE), usage_user=float(i+j))
                 for i in range(3) for j in range(3)]
        actions = engine.apply(batch)
        self.assertEqual(len(actions), 9*10+3)
        self.assertEqual(engine.apply(reversed(batch)), [])
        self.assertEqual(set(engine.rows), set(subset))
        self.assertEqual(len(engine.rows["ts5_global_max_minute"]), 3)
        with self.assertRaisesRegex(ValueError, "Select valid"):
            self.new_materializer(["no_such_candidate"])


    def test_optional_cassandra_sink_explicit_opt_in_and_date_binding(self):
        class FakeSession:
            keyspace = "tsbs_stage6_isolated"
            def __init__(self):
                self.prepared, self.executed = [], []
            def prepare(self, text):
                self.prepared.append(text)
                return text
            def execute(self, statement, values):
                self.executed.append((statement, values))
        backend = FakeSession()
        engine = self.new_materializer(["ts1_metric_bucket_1d"])
        actions = engine.apply([self.reading()])
        writer = ExperimentalCassandraExecutor(backend, self.writeplans,
                                               expected_keyspace="tsbs_stage6_isolated")
        with self.assertRaises(PermissionError):
            writer.execute_mutations(actions)
        self.assertEqual(backend.executed, [])
        self.assertEqual(writer.execute_mutations(actions, allow_experimental_writes=True), 10)
        self.assertEqual(len(backend.prepared), 1)  # One prepared statement reused
        self.assertIsInstance(backend.executed[0][1][2], date)
        bad = dict(actions[0], cql="DROP TABLE ts1_cpu_metric_1d;")
        with self.assertRaisesRegex(ValueError, "CQL differs"):
            writer.execute_mutations([bad], allow_experimental_writes=True)
        with self.assertRaisesRegex(ValueError, "explicit isolated keyspace"):
            ExperimentalCassandraExecutor(backend, self.writeplans, expected_keyspace="other")


if __name__ == "__main__":
    unittest.main()
