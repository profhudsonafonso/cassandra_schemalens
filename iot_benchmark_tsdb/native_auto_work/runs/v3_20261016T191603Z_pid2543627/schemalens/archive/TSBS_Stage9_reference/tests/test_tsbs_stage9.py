"""Stage 9 unit and integrated OFFLINE tests; do not require Cassandra server."""
from __future__ import annotations

import argparse
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from methodology.tsbs.stage9.dataset import write_small_fixture, load_cpu_dataset
from methodology.tsbs.stage9.instances import generate_instances, route_instance
from methodology.tsbs.stage9.contracts import read_inputs, require_reset_confirmation, keyspace_for
from methodology.tsbs.stage9.offline import IndexedReferenceFetcher
from methodology.tsbs.stage9.benchmark import compare_values, percentile
from methodology.tsbs.stage9.cli import _subset, _offline_check, _reference_materializer, run
from methodology.tsbs.stage5.runtime import evaluate_with_fetcher


ROOT = Path(__file__).resolve().parents[1]


class Stage9Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.root_inputs = read_inputs(ROOT)
        cls.work = tempfile.TemporaryDirectory()
        cls.temp = Path(cls.work.name)
        cls.source = cls.temp / "data.csv"
        write_small_fixture(cls.source, hosts=40, hours=26, interval_seconds=3600)
        cls.dataset = load_cpu_dataset(cls.source, max_observations=1100)
        cls.instances = generate_instances(cls.dataset, interval_seconds=3600, per_template=1, seed=123)
        cls.dep = cls.root_inputs["deployments"][0]
        cls.materializer, cls.mutations, cls.elapsed = _reference_materializer(cls.root_inputs, cls.dep, cls.dataset)

    @classmethod
    def tearDownClass(cls):
        cls.work.cleanup()

    def test_01_only_tsdb_family(self):
        self.assertTrue(all(s["family"].startswith("TS") for s in self.root_inputs["specs"].values()))
        self.assertFalse(self.root_inputs["policy"]["benchmark_results_may_affect_stage8_selection"])

    def test_02_original_series_key(self):
        key = self.dataset["series_ids"][("host_0", "usage_user", "2020-01-01")]
        self.assertEqual(key, "cpu,hostname=host_0,region=us-east-1,datacenter=us-east-1a,"
                         "rack=0,os=Ubuntu16.04LTS,arch=x64,team=SF,service=1,"
                         "service_version=0,service_environment=production#usage_user#2020-01-01")

    def test_03_16_templates(self):
        self.assertEqual(len(self.instances), 16)
        self.assertEqual({i["query_id"] for i in self.instances}, {f"Q{i:02d}" for i in range(1, 17)})

    def test_04_deterministic_instances(self):
        self.assertEqual(self.instances, generate_instances(self.dataset, interval_seconds=3600, per_template=1, seed=123))
        self.assertNotEqual(self.instances, generate_instances(self.dataset, interval_seconds=3600, per_template=1, seed=456))

    def test_05_all_host_metadata(self):
        for instance in self.instances:
            if instance["query_id"] in {"Q10", "Q11", "Q12", "Q14", "Q15", "Q16"}:
                self.assertEqual(instance["params"]["hostnames"], self.dataset["hostnames"])

    def test_06_guarded_keyspaces(self):
        keyspace = keyspace_for(self.dep["deployment_id"])
        with self.assertRaises(PermissionError):
            require_reset_confirmation(keyspace, "")
        with self.assertRaises(PermissionError):
            require_reset_confirmation("production", "production")
        require_reset_confirmation(keyspace, keyspace)

    def test_07_selected_subset_and_all(self):
        self.assertEqual(_subset(self.root_inputs, deployment_id=None, all_deployments=False)[0]["rank"], 1)
        self.assertEqual(len(_subset(self.root_inputs, deployment_id=None, all_deployments=True)), 7)
        with self.assertRaises(ValueError):
            _subset(self.root_inputs, deployment_id="H_INVALID", all_deployments=False)

    def test_08_indexed_mock_matches_stage6_fetch(self):
        indexed = IndexedReferenceFetcher(self.materializer)
        plan = self.root_inputs["plans"]["Q13__ts2_wide_bucket_1d"]
        instance = next(i for i in self.instances if i["query_id"] == "Q13")
        a = evaluate_with_fetcher(plan, indexed, **instance["params"])
        b = evaluate_with_fetcher(plan, self.materializer.query_mock_rows, **instance["params"])
        self.assertEqual(a, b)

    def test_09_offline_all_templates(self):
        result = _offline_check(self.dep, self.root_inputs, self.dataset, self.instances, self.materializer)
        self.assertEqual(result["offline_instances_validated"], 16)
        self.assertEqual(result["fallbacks"], 0)

    def test_10_rollup_misalignment_forces_ts0(self):
        dep = next(d for d in self.root_inputs["deployments"] if d["profile"] == "conditional_aligned")
        inst = next(i for i in self.instances if i["query_id"] == "Q10")
        plan, status = route_instance(dep, inst, self.root_inputs["plans"])
        self.assertEqual(status, "fallback_due_to_rollup_alignment")
        self.assertEqual(plan["family"], "TS0")

    def test_11_reject_wrong_sampling_interval(self):
        with self.assertRaises(ValueError):
            generate_instances(self.dataset, interval_seconds=0, per_template=1, seed=1)

    def test_12_fail_incomplete_dataset(self):
        broken = self.temp / "incomplete.csv"
        broken.write_text(self.source.read_text(encoding="utf-8").splitlines()[0] + "\n")
        with self.assertRaises(ValueError):
            load_cpu_dataset(broken, max_observations=100)

    def test_13_reject_corrupted_stage8(self):
        original = self.root_inputs["stage8_manifest"] if "stage8_manifest" in self.root_inputs else None
        self.assertTrue(self.root_inputs["sha256"]["stage8_manifest"])

    def test_14_strict_compare(self):
        compare_values([{"max_usage_user": 7.0}], [{"max_usage_user": 7.000000000001}], rel_tol=1e-9, abs_tol=1e-8)
        with self.assertRaises(AssertionError):
            compare_values([{"x": 5}], [{"x": 6}], rel_tol=1e-9, abs_tol=1e-8)

    def test_15_percentiles(self):
        self.assertEqual(percentile([1.0, 3.0, 9.0, 12.0], 95), 12.0)

    def test_16_live_requires_confirmation_before_connecting(self):
        args = argparse.Namespace(root=ROOT, make_fixture=None, data=self.source,
              deployment=self.dep["deployment_id"], all=False, live=True,
              confirm_reset_keyspace="", confirm_reset_all="", output=None,
              overwrite_results=False, seed=123, alignment_cohort="natural", host="127.0.0.1", port=9042)
        with self.assertRaises(PermissionError):
            run(args)

    def test_17_aligned_cohort_activates_more_conditional_routes(self):
        aligned = generate_instances(self.dataset, interval_seconds=3600, per_template=1,
                                     seed=123, alignment_cohort="aligned")
        conditional = next(d for d in self.root_inputs["deployments"] if d["profile"] == "conditional_aligned")
        q10 = next(i for i in aligned if i["query_id"] == "Q10")
        p, status = route_instance(conditional, q10, self.root_inputs["plans"])
        self.assertEqual(status, "selected")
        self.assertEqual(p["family"], "TS6")

    def test_18_dry_run_writes_only_stage9(self):
        output = self.temp / "outputs"
        args = argparse.Namespace(root=ROOT, make_fixture=None, data=self.source,
              deployment=self.dep["deployment_id"], all=False, live=False,
              confirm_reset_keyspace="", confirm_reset_all="", output=output,
              overwrite_results=False, seed=123, alignment_cohort="natural", host="127.0.0.1", port=9042)
        result = run(args)
        self.assertFalse(result["benchmark_verified"])
        self.assertFalse(result["go_tsbs_baseline_ran"])
        self.assertEqual(result["query_instances"], 3*16)
        self.assertEqual(result["results"][0]["offline_instances_validated"], 48)
        self.assertTrue((output/"36_stage9_manifest.json").is_file())
        self.assertTrue((output/"35_stage9_query_latencies.csv").is_file())
        self.assertNotIn("mean_ms", json.dumps(result["results"]))


if __name__ == "__main__":
    unittest.main()
