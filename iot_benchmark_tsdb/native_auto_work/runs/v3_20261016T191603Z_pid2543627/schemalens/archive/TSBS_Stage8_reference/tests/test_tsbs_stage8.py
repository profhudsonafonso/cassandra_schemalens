"""Stage 8 correctness tests, no network and no real Cassandra installation."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from methodology.tsbs.stage8.consolidator import (
    _dominates, _make_deployment, _pareto, _policy, generate,
    load_inputs, read_score, search, validate,
)

ROOT = Path(__file__).resolve().parents[1]


class Stage8Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = load_inputs(ROOT)
        cls.state = validate(cls.raw)
        cls.policy = cls.raw["policy"]
        cls.result = search(cls.state, cls.policy)

    def test_source_counts_and_tsdb_only(self):
        self.assertEqual(len(self.state["specs"]), 9)
        self.assertEqual(len(self.state["plans"]), 37)
        self.assertEqual(len(self.state["workload"]), 16)
        self.assertTrue(all(p["family"].startswith("TS") for p in self.state["specs"].values()))

    def test_enumeration_is_exact_and_untruncated(self):
        self.assertEqual(self.result["counts"]["conservative"]["subsets_checked"], 256)
        self.assertEqual(self.result["counts"]["conditional_aligned"]["subsets_checked"], 256)

    def test_baseline_covers_all_queries(self):
        for mode in self.policy["profiles"]:
            baseline = _make_deployment(("ts0_metric_raw",), mode, self.state, self.policy)
            self.assertIsNotNone(baseline)
            self.assertEqual(len(baseline["query_routes"]), 16)
            self.assertAlmostEqual(baseline["weighted_proxy_score"], 1.0)
            self.assertTrue(all(q["candidate_id"] == "ts0_metric_raw" for q in baseline["query_routes"].values()))

    def test_conditional_vs_conservative_guards(self):
        subset = ("ts0_metric_raw", "ts6_hour_host_dimension")
        self.assertIsNone(_make_deployment(subset, "conservative", self.state, self.policy))
        conditional = _make_deployment(subset, "conditional_aligned", self.state, self.policy)
        self.assertIsNotNone(conditional)
        self.assertEqual(conditional["conditional_aligned_query_ids"], ["Q10", "Q11", "Q12"])
        self.assertTrue(all(conditional["query_routes"][qid]["alignment_guard"] == "hour" for qid in ("Q10", "Q11", "Q12")))
        self.assertFalse(conditional["all_queries_runtime_verified"])

    def test_canonical_source_and_no_idle_tables(self):
        subset = ("ts0_metric_raw", "ts1_metric_bucket_1d")
        # The Stage 7 proxy doesn't make TS1 preferable to TS0 on Q13/14;
        # no credit for an unused extra table.
        self.assertIsNone(_make_deployment(subset, "conservative", self.state, self.policy))
        self.assertIsNone(_make_deployment(("ts2_wide_bucket_1d",), "conservative", self.state, self.policy))

    def test_selected_workload_cover_and_query_roster_guards(self):
        found = self.result["selected"]
        self.assertGreaterEqual(len(found), 2)
        for dep in found:
            self.assertEqual(dep["covered_queries_structural"], 16)
            self.assertEqual(set(dep["query_routes"]), set(self.state["workload"]))
            self.assertEqual(set(dep["required_write_plan_ids"]), set(dep["candidate_ids"]))
            self.assertIn("ts0_metric_raw", dep["candidate_ids"])
            self.assertFalse(dep["benchmark_executed"])
            q14 = dep["query_routes"]["Q14"]
            self.assertIn("complete_host_roster_from_same_loaded_dataset", q14["unverified_runtime_requirements"])
            for qid, route in dep["query_routes"].items():
                self.assertIn(route["candidate_id"], dep["candidate_ids"])
                self.assertEqual(route["selected_plan_id"], f"{qid}__{route['candidate_id']}")

    def test_pareto_never_dominates_selected_result(self):
        # Independently check no exported selected row dominates another
        # in the SAME profile (a necessary condition for Pareto validity).
        for a in self.result["selected"]:
            for b in self.result["selected"]:
                if a is not b and a["profile"] == b["profile"]:
                    self.assertFalse(_dominates(a, b))

    def test_read_proxy_normalization(self):
        self.assertAlmostEqual(read_score("Q13__ts0_metric_raw", "Q13", self.state, self.policy), 1.0)
        self.assertLess(read_score("Q13__ts2_wide_bucket_1d", "Q13", self.state, self.policy), 1.0)

    def test_policy_refuses_benchmark_lookahead(self):
        invalid = copy.deepcopy(self.policy)
        invalid["benchmark_metrics_allowed_for_selection"] = True
        with self.assertRaisesRegex(ValueError, "empirical benchmark"):
            _policy(invalid)

    def test_policy_refuses_invalid_weights(self):
        invalid = copy.deepcopy(self.policy)
        invalid["deployment_proxy_weights"]["normalized_read"] = 0.7
        with self.assertRaisesRegex(ValueError, "add to exactly one"):
            _policy(invalid)

    def test_missing_stage7_cost_proxy_is_rejected(self):
        invalid = copy.deepcopy(self.raw)
        invalid["read_costs"] = invalid["read_costs"][:-1]
        with self.assertRaisesRegex(ValueError, "incomplete"):
            validate(invalid)

    def test_non_temporal_family_fails_closed(self):
        invalid = copy.deepcopy(self.raw)
        invalid["specs"][0]["family"] = "C8"
        with self.assertRaises(ValueError):
            validate(invalid)

    def test_measured_latency_leak_is_rejected(self):
        invalid = copy.deepcopy(self.raw)
        invalid["read_costs"][0]["measured_latency_ms"] = 2.1
        with self.assertRaisesRegex(ValueError, "Measured latency"):
            validate(invalid)

    def test_manifest_and_deterministic_outputs(self):
        with tempfile.TemporaryDirectory() as d:
            output = Path(d)
            result = generate(ROOT, output)
            self.assertFalse(result["cassandra_executed"])
            self.assertFalse(result["tsbs_benchmarked"])
            summary = (output / "30_hbench_summary.csv").read_bytes()
            selected = (output / "28_hbench_selected_deployments.json").read_bytes()
            result2 = generate(ROOT, output)
            self.assertEqual(summary, (output / "30_hbench_summary.csv").read_bytes())
            self.assertEqual(selected, (output / "28_hbench_selected_deployments.json").read_bytes())
            self.assertEqual(result, result2)
            routes = json.loads((output / "29_hbench_selected_query_routes.json").read_text())
            self.assertEqual(len(routes), len(self.result["selected"]) * 16)

    def test_exact_budget_refuses_silent_truncation(self):
        bad = copy.deepcopy(self.policy)
        bad["exact_enumeration_limit_candidates"] = 8
        with self.assertRaisesRegex(ValueError, "Exact search budget exceeded"):
            search(self.state, bad)

    def test_derived_dependency_is_not_free(self):
        with_rollup = _make_deployment(("ts0_metric_raw", "ts6_hour_host_dimension"), "conditional_aligned", self.state, self.policy)
        self.assertGreater(with_rollup["normalized_proxies"]["normalized_storage"], 1.0)
        self.assertGreater(with_rollup["normalized_proxies"]["normalized_touched_write_keys"], 1.0)
        self.assertEqual(with_rollup["derived_candidates_require_durable_source"], ["ts6_hour_host_dimension"])

    def test_no_data_driven_recalibration(self):
        self.assertTrue(self.raw["policy"]["assume_equal_query_frequencies"])
        self.assertTrue(all(x["query_relative_frequency"] is None for x in self.raw["read_costs"]))


if __name__ == "__main__":
    unittest.main()
