"""Stage 7 unit/integration regression tests; no Cassandra cluster required."""
from __future__ import annotations

import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path

from methodology.tsbs.stage7.estimator import (_mozaffari, _read_proxy, _scenario,
                                               _estimate_storage, _writes, generate)
from methodology.tsbs.stage7.operators import describe

ROOT = Path(__file__).resolve().parents[1]


def data(name: str):
    return json.loads((ROOT / name).read_text(encoding="utf-8"))


class Stage7Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.specs = {x["candidate_id"]: x for x in data("outputs/tsbs_stage4/10_cassandra_candidate_specs.json")}
        cls.writers = {x["candidate_id"]: x for x in data("outputs/tsbs_stage6/18_write_plans.json")}
        cls.plans = data("outputs/tsbs_stage5/15_query_plans.json")
        cls.policy = data("config/tsbs_stage7_cost_policy.json")
        cls.scene = _scenario(data("inputs/schema/tsbs_cpu_only_schema.json"), cls.policy)
        with (ROOT / "inputs/workload/query_manifest.csv").open(encoding="utf-8", newline="") as stream:
            cls.manifest = {x["query_id"]: x for x in csv.DictReader(stream)}

    def test_01_all_nine_candidates_preserved_without_reactivation(self):
        self.assertEqual(len(self.specs), 9)
        self.assertEqual({s["family"] for s in self.specs.values()}, {"TS0", "TS1", "TS2", "TS5", "TS6"})
        self.assertEqual(len(self.plans), 37)

    def test_02_bucketed_partition_proxies(self):
        ts0 = _estimate_storage(self.specs["ts0_metric_raw"], self.scene, self.policy)
        ts1 = _estimate_storage(self.specs["ts1_metric_bucket_1d"], self.scene, self.policy)
        ts7 = _estimate_storage(self.specs["ts1_metric_bucket_7d"], self.scene, self.policy)
        self.assertEqual(ts0["estimated_total_rows"], 4320000)
        self.assertEqual(ts1["estimated_total_rows"], 4320000)
        self.assertEqual(ts1["estimated_max_rows_in_one_partition"], 1440)
        self.assertLessEqual(ts7["estimated_max_rows_in_one_partition"], ts0["estimated_max_rows_in_one_partition"])

    def test_03_wide_storage_not_fake_replication(self):
        wide = _estimate_storage(self.specs["ts2_wide_bucket_1d"], self.scene, self.policy)
        self.assertEqual(wide["estimated_total_rows"], 432000)
        self.assertFalse(wide["byte_proxy_is_measured"])

    def test_04_cross_host_partition_has_explicit_risk(self):
        row = _estimate_storage(self.specs["ts6_hour_host_dimension"], self.scene, self.policy)
        self.assertEqual(row["estimated_max_rows_in_one_partition"], 100)
        self.assertIn("CROSS_HOST_PARTITION_GROWS_WITH_HOST_CARDINALITY", row["warnings"])

    def test_05_ts2_q13_fewer_read_requests_than_scalar(self):
        plans = {p["candidate_id"]: p for p in self.plans if p["query_id"] == "Q13"}
        single = _read_proxy(self.manifest["Q13"], plans["ts0_metric_raw"], self.scene, self.policy)
        wide = _read_proxy(self.manifest["Q13"], plans["ts2_wide_bucket_1d"], self.scene, self.policy)
        self.assertEqual(single["estimated_cql_requests_upper_proxy"], 10)
        self.assertLess(wide["estimated_cql_requests_upper_proxy"], single["estimated_cql_requests_upper_proxy"])
        self.assertIsNone(wide["measured_latency_ms"])

    def test_06_q10_ts6_coverage_stays_conditional(self):
        plans = {p["candidate_id"]: p for p in self.plans if p["query_id"] == "Q10"}
        dim = _read_proxy(self.manifest["Q10"], plans["ts6_hour_host_dimension"], self.scene, self.policy)
        self.assertEqual(dim["alignment_guard"], "hour")
        self.assertIn("runtime_exact_interval_alignment_required", dim["requirements"])
        self.assertIn("external_full_host_roster_not_materialized", dim["requirements"])

    def test_07_write_fanout_not_equal_to_actual_mutations(self):
        raw = _writes(self.specs["ts0_metric_raw"], self.writers["ts0_metric_raw"], self.scene)
        summary = _writes(self.specs["ts6_hour_host_dimension"], self.writers["ts6_hour_host_dimension"], self.scene)
        self.assertEqual(raw["distinct_physical_group_rows_touched_by_new_observation_proxy"], 10)
        self.assertEqual(summary["distinct_physical_group_rows_touched_by_new_observation_proxy"], 1)
        self.assertTrue(summary["canonical_source_required"])
        self.assertNotIsInstance(summary["estimated_cql_mutations_per_event"], (int, float))

    def test_08_directional_signals_not_inferred(self):
        signals = _mozaffari(data("inputs/tsbs_stage7_mozaffari_evidence.json"))
        self.assertEqual(len(signals), 2)
        self.assertFalse(any(r["evidence_complete"] for r in signals))
        self.assertTrue(all(r["cost_R"] is None and r["cost_D"] is None for r in signals))

    def test_09_complete_mozaffari_evidence_still_no_formula(self):
        obj = {"status": "SUPPLIED_INDEPENDENT_EVIDENCE_NOT_IMPLEMENTED",
               "records": [{"relationship": "r", "direction": "a_to_b", "FR": 5, "FU": 2,
                            "RC": 3, "TI": 1, "source": "measured-experiment"}]}
        item = _mozaffari(obj)[0]
        self.assertTrue(item["evidence_complete"])
        self.assertIsNone(item["cost_R"])
        obj["records"][0]["RC"] = -1
        with self.assertRaises(ValueError):
            _mozaffari(obj)

    def test_10_operators_are_descriptive_only(self):
        for spec in self.specs.values():
            op = describe(spec)
            self.assertFalse(op["has_executable_rewrite_operator"])
            self.assertFalse(op["has_published_operator_cost_model"])

    def test_11_generate_artifacts_without_overwriting(self):
        originals = [ROOT / "outputs/tsbs_stage4/10_cassandra_candidate_specs.json",
                     ROOT / "outputs/tsbs_stage5/15_query_plans.json",
                     ROOT / "outputs/tsbs_stage6/18_write_plans.json",
                     ROOT / "config/tsdb_activation_rules.json"]
        before = [p.read_bytes() for p in originals]
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "stage7"
            result = generate(ROOT, output)
            self.assertEqual(result["candidate_count"], 9)
            self.assertEqual(result["query_plan_count"], 37)
            self.assertEqual(result["query_count"], 16)
            self.assertFalse(result["TSBS_benchmarked"])
            self.assertFalse(result["relative_workload_cost_computed"])
            self.assertFalse(result["C0_C9_used"])
            self.assertEqual(len(list(output.iterdir())), 7)
            self.assertEqual([p.read_bytes() for p in originals], before)

    def test_12_block_use_of_benchmark_scores(self):
        policy = dict(self.policy, rank_candidates=True)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "policy.json"
            path.write_text(json.dumps(policy), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "rank or use benchmark"):
                generate(ROOT, Path(tmp) / "out", policy_file=path)

    def test_13_tampering_with_prior_artifact_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            clone = Path(tmp) / "project"
            for top in ("outputs", "inputs", "config"):
                shutil.copytree(ROOT / top, clone / top)
            p = clone / "outputs/tsbs_stage6/18_write_plans.json"
            original = json.loads(p.read_text(encoding="utf-8"))
            original[0]["family"] = "TS9"
            p.write_text(json.dumps(original), encoding="utf-8")
            with self.assertRaises(ValueError):
                generate(clone, Path(tmp) / "new")

    def test_14_q16_raw_scope_cannot_masquerade_as_five_row_limit(self):
        plans = {p["candidate_id"]: p for p in self.plans if p["query_id"] == "Q16"}
        raw = _read_proxy(self.manifest["Q16"], plans["ts0_metric_raw"], self.scene, self.policy)
        roll = _read_proxy(self.manifest["Q16"], plans["ts5_global_max_minute"], self.scene, self.policy)
        self.assertGreater(raw["estimated_logical_rows_returned_upper_proxy"], 100000)
        self.assertLess(roll["estimated_logical_rows_returned_upper_proxy"], raw["estimated_logical_rows_returned_upper_proxy"])


if __name__ == "__main__":
    unittest.main()
