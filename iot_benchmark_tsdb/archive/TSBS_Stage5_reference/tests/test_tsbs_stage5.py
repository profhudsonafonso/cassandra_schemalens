"""Stage 5 regression and semantic tests without changing Stages 1–4."""
from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from methodology.tsbs.stage5.planner import NS_DAY, NS_HOUR, NS_MINUTE, generate
from methodology.tsbs.stage5.runtime import bind_read_requests, evaluate_with_fetcher, execute_mock_rows

ROOT = Path(__file__).resolve().parents[1]


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Stage5Fixtures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as tmp:
            cls.result = generate(ROOT, Path(tmp))
            cls.plans = json.loads((Path(tmp) / "15_query_plans.json").read_text(encoding="utf-8"))
            cls.coverage = (Path(tmp) / "16_coverage_matrix.csv").read_text(encoding="utf-8")
        cls.lookup = {(p["query_id"], p["family"] + ":" + p["candidate_id"]): p for p in cls.plans}
        cls.metrics = json.loads((ROOT / "inputs/schema/tsbs_cpu_only_schema.json").read_text())["entities"]["CPUReading"]["temporal"]["metrics"]

    @classmethod
    def plan(cls, qid: str, candidate_id: str) -> dict:
        matches = [p for p in cls.plans if p["query_id"] == qid and p["candidate_id"] == candidate_id]
        assert len(matches) == 1, (qid, candidate_id)
        return matches[0]

    def test_all_query_candidate_links_are_preserved(self):
        self.assertEqual((self.result["queries"], self.result["candidate_count"], self.result["query_plans"]), (16, 9, 37))
        self.assertEqual(set(self.result["families"]), {"TS0", "TS1", "TS2", "TS5", "TS6"})
        self.assertTrue(self.result["per_query_has_structural_plan"])
        self.assertFalse(self.result["CQL_executed_on_server"])
        self.assertFalse(self.result["complete_write_plan"])
        self.assertIn("STRUCTURAL_WITH_RUNTIME_GUARDS", self.coverage)
        self.assertEqual(len({p["plan_id"] for p in self.plans}), len(self.plans))
        for p in self.plans:
            self.assertTrue(p["family"].startswith("TS"))
            self.assertNotIn("ALLOW FILTERING", p["read_cql"].upper())
            if p["query_kind"] in {"lastpoint", "toprollup"}:
                expected = 2 if p["query_kind"] == "lastpoint" else 3
            else:
                expected = {"raw_scalar": 4, "raw_scalar_bucketed": 5,
                            "raw_wide_bucketed": 4, "derived_rollup": 5,
                            "dimension_rollup": 2}[p["physical_strategy"]]
            self.assertEqual(p["read_cql"].count("?"), expected)

    def test_does_not_change_stage4_or_original_rules(self):
        files = ["outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
                 "outputs/tsbs_stage4/11_query_candidate_map.json",
                 "config/tsdb_activation_rules.json", "config/temporal_modifier_rules.json",
                 "outputs/tsbs_run/04_tsdb_activation_matrix.csv"]
        before = {name: digest(ROOT / name) for name in files}
        with tempfile.TemporaryDirectory() as tmp:
            generate(ROOT, Path(tmp))
        self.assertEqual(before, {name: digest(ROOT / name) for name in files})

    def test_high_cpu_binding_and_buckets(self):
        for candidate, count in [("ts0_metric_raw", 10), ("ts1_metric_bucket_1d", 10),
                                 ("ts1_metric_bucket_7d", 10), ("ts2_wide_bucket_1d", 1)]:
            plan = self.plan("Q13", candidate)
            queries = bind_read_requests(plan, hostnames=["host_1"], start_ns=NS_DAY,
                                         end_ns=NS_DAY + NS_HOUR, dense_cpu_metrics=True)
            self.assertEqual(len(queries), count, candidate)
            self.assertFalse(any("ALLOW FILTERING" in x["cql"].upper() for x in queries))
        week = bind_read_requests(self.plan("Q13", "ts1_metric_bucket_7d"),
                                  hostnames=["host_1"], start_ns=0, end_ns=NS_DAY,
                                  dense_cpu_metrics=True)
        self.assertEqual(week[0]["bucket_day"], "1970-01-01")
        with self.assertRaisesRegex(ValueError, "dense CPU"):
            bind_read_requests(self.plan("Q13", "ts0_metric_raw"), hostnames=["host_1"],
                               start_ns=NS_DAY, end_ns=2 * NS_DAY)

    def test_all_host_roster_is_mandatory(self):
        plan = self.plan("Q14", "ts1_metric_bucket_1d")
        with self.assertRaisesRegex(ValueError, "complete host roster"):
            bind_read_requests(plan, hostnames=["host_1"], start_ns=0, end_ns=NS_HOUR,
                               dense_cpu_metrics=True)
        requests = bind_read_requests(plan, hostnames=["host_1", "host_2"], start_ns=0,
                                      end_ns=NS_HOUR, complete_host_roster=True,
                                      dense_cpu_metrics=True)
        self.assertEqual(len(requests), 20)

    def test_host_count_must_match_template(self):
        with self.assertRaisesRegex(ValueError, "Expected 8"):
            bind_read_requests(self.plan("Q03", "ts0_metric_raw"), hostnames=["host_1"],
                               start_ns=0, end_ns=NS_HOUR)

    def test_rollups_reject_partial_windows(self):
        for qid, cid, n in [("Q01", "ts5_max_by_host_minute", NS_MINUTE),
                            ("Q07", "ts5_max_by_host_hour", NS_HOUR),
                            ("Q10", "ts5_sum_count_by_host_hour", NS_HOUR),
                            ("Q10", "ts6_hour_host_dimension", NS_HOUR)]:
            p = self.plan(qid, cid)
            with self.subTest(p=p["plan_id"]):
                with self.assertRaisesRegex(ValueError, "aligned"):
                    bind_read_requests(p, hostnames=["host_1"], start_ns=1, end_ns=n,
                                       complete_host_roster=True)
                good = bind_read_requests(p, hostnames=["host_1"], start_ns=0, end_ns=n,
                                          complete_host_roster=True)
                self.assertTrue(good)

    def test_latest_per_host_expands_to_metric_reads(self):
        p = self.plan("Q15", "ts0_metric_raw")
        with self.assertRaisesRegex(ValueError, "dense CPU"):
            bind_read_requests(p, hostnames=["host_0"], complete_host_roster=True)
        requests = bind_read_requests(p, hostnames=["host_0", "host_1"],
                                      complete_host_roster=True, dense_cpu_metrics=True)
        self.assertEqual(len(requests), 20)
        self.assertTrue(all("LIMIT 1" in r["cql"] for r in requests))
        self.assertTrue(all("timestamp_ns <" not in r["cql"] for r in requests))

    def test_toprollup_requires_day_traversal_and_aligned_end(self):
        p = self.plan("Q16", "ts5_global_max_minute")
        with self.assertRaisesRegex(ValueError, "aligned"):
            bind_read_requests(p, hostnames=["host_1"], end_ns=NS_DAY + 2,
                               dataset_start_ns=0, complete_host_roster=True)
        with self.assertRaisesRegex(ValueError, "dataset_start_ns"):
            bind_read_requests(p, hostnames=["host_1"], end_ns=3 * NS_DAY,
                               complete_host_roster=True)
        requests = bind_read_requests(p, hostnames=["host_1"], end_ns=3 * NS_DAY,
                                      dataset_start_ns=0, complete_host_roster=True)
        self.assertEqual(len(requests), 3)
        self.assertEqual([q["bucket_day"] for q in requests],
                         ["1970-01-03", "1970-01-02", "1970-01-01"])
        self.assertEqual(requests[0]["parameters"][-1], 3 * NS_DAY)

    def test_raw_vs_minute_rollup_produces_same_max(self):
        raw = self.plan("Q01", "ts0_metric_raw")
        rollup = self.plan("Q01", "ts5_max_by_host_minute")
        readings = [(0, 30.0), (10 * 1_000_000_000, 45.0),
                    (NS_MINUTE, 25.0), (NS_MINUTE + 10 * 1_000_000_000, 51.0)]
        raw_response = lambda request: [{"timestamp_ns": t, "value": v} for t, v in readings]
        rollup_response = lambda request: [{"minute_ns": 0, "max_value": 45.0},
                                           {"minute_ns": NS_MINUTE, "max_value": 51.0}]
        kwargs = dict(hostnames=["host_1"], start_ns=0, end_ns=2 * NS_MINUTE)
        self.assertEqual(evaluate_with_fetcher(raw, raw_response, **kwargs),
                         evaluate_with_fetcher(rollup, rollup_response, **kwargs))

    def test_raw_vs_sum_count_vs_dimension_avg(self):
        plans = [self.plan("Q10", cid) for cid in
                 ["ts0_metric_raw", "ts5_sum_count_by_host_hour", "ts6_hour_host_dimension"]]
        host_vals = {"host_1": [12.0, 24.0], "host_2": [20.0, 70.0]}

        def fake_fetch(req):
            cid = req["candidate_id"]
            if cid == "ts0_metric_raw":
                return [{"timestamp_ns": 1, "value": host_vals[req["hostname"]][0]},
                        {"timestamp_ns": NS_MINUTE, "value": host_vals[req["hostname"]][1]}]
            if cid == "ts5_sum_count_by_host_hour":
                return [{"hour_ns": 0, "value_sum": sum(host_vals[req["hostname"]]), "sample_count": 2}]
            if cid == "ts6_hour_host_dimension":
                return [{"hostname": host, "sum_usage_user": sum(vals), "count_usage_user": 2}
                        for host, vals in host_vals.items()]
            raise AssertionError(cid)

        results = [evaluate_with_fetcher(p, fake_fetch, hostnames=list(host_vals),
                                         start_ns=0, end_ns=NS_HOUR, complete_host_roster=True)
                   for p in plans]
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])
        self.assertEqual([r["avg_usage_user"] for r in results[2]], [18.0, 45.0])

    def test_high_cpu_raw_and_wide_match(self):
        raw = self.plan("Q13", "ts0_metric_raw")
        wide = self.plan("Q13", "ts2_wide_bucket_1d")
        rows = [{"timestamp_ns": NS_MINUTE, **{m: float(i) for i, m in enumerate(self.metrics)}},
                {"timestamp_ns": 2 * NS_MINUTE, **{m: (91.0 if m == "usage_user" else float(i))
                                                     for i, m in enumerate(self.metrics)}}]
        def mock(req):
            if req["candidate_id"] == "ts0_metric_raw":
                return [{"timestamp_ns": row["timestamp_ns"], "value": row[req["metric_name"]]}
                        for row in rows]
            return rows
        args = dict(hostnames=["host_1"], start_ns=0, end_ns=NS_HOUR, dense_cpu_metrics=True)
        self.assertEqual(evaluate_with_fetcher(raw, mock, **args),
                         evaluate_with_fetcher(wide, mock, **args))
        self.assertEqual(len(evaluate_with_fetcher(wide, mock, **args)), 1)

    def test_lastpoint_across_multiple_hosts_correctness(self):
        p = self.plan("Q15", "ts0_metric_raw")
        def mock(req):
            host_id = int(req["hostname"].split("_")[-1])
            ix = self.metrics.index(req["metric_name"])
            return [{"timestamp_ns": 2 * NS_MINUTE, "value": float(host_id * 10 + ix)}]
        out = evaluate_with_fetcher(p, mock, hostnames=["host_0", "host_1"],
                                    complete_host_roster=True, dense_cpu_metrics=True)
        self.assertEqual(len(out), 2)
        self.assertEqual([r["hostname"] for r in out], ["host_0", "host_1"])
        self.assertEqual(out[1]["usage_user"], 10.0)

    def test_toprollup_raw_vs_global_summary(self):
        raw = self.plan("Q16", "ts0_metric_raw")
        summary = self.plan("Q16", "ts5_global_max_minute")
        readings = {"host_1": [(i * NS_MINUTE, i + 1.0) for i in range(7)],
                    "host_2": [(i * NS_MINUTE, i + 3.0) for i in range(7)]}
        def mock(req):
            if req["candidate_id"] == "ts0_metric_raw":
                return [{"timestamp_ns": t, "value": v} for t, v in readings[req["hostname"]]]
            return [{"minute_ns": i * NS_MINUTE, "max_value": i + 3.0} for i in reversed(range(2, 7))]
        kwargs = dict(hostnames=list(readings), end_ns=7 * NS_MINUTE,
                      dataset_start_ns=0, complete_host_roster=True)
        self.assertEqual(evaluate_with_fetcher(raw, mock, **kwargs),
                         evaluate_with_fetcher(summary, mock, **kwargs))

    def test_reject_stale_sources(self):
        """Reject modified source hashes, without mutating any real source."""
        import shutil
        with tempfile.TemporaryDirectory() as temp:
            local = Path(temp) / "root"
            for rel in ["outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
                        "outputs/tsbs_stage4/11_query_candidate_map.json",
                        "outputs/tsbs_stage4/12_query_read_primitives.json",
                        "outputs/tsbs_stage4/14_stage4_manifest.json",
                        "inputs/workload/query_manifest.csv", "inputs/schema/tsbs_cpu_only_schema.json",
                        "config/tsbs_stage4_physical_policy.json"]:
                (local / rel).parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / rel, local / rel)
            (local / "config/tsbs_stage4_physical_policy.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "source changed"):
                generate(local, Path(temp) / "stage5")


if __name__ == "__main__":
    unittest.main()
