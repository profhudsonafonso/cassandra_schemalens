"""Isolated Stage 4 tests: no Cassandra server or benchmark required."""
from __future__ import annotations

import csv
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from methodology.tsbs.stage4.candidate_generator import CandidateSpec, instantiate, build_prototypes

ROOT = Path(__file__).resolve().parents[1]


class CandidateSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schema = json.loads((ROOT / "inputs/schema/tsbs_cpu_only_schema.json").read_text())
        cls.policy = json.loads((ROOT / "config/tsbs_stage4_physical_policy.json").read_text())
        cls.metrics = cls.schema["entities"]["CPUReading"]["temporal"]["metrics"]
        cls.candidates = build_prototypes(cls.metrics, cls.policy)

    def test_only_previously_defined_ts_families(self):
        self.assertEqual({c.family for c in self.candidates.values()}, {"TS0", "TS1", "TS2", "TS5", "TS6"})
        self.assertFalse(any(c.family.startswith("C") for c in self.candidates.values()))

    def test_all_cql_have_partition_and_clustering_keys(self):
        for spec in self.candidates.values():
            with self.subTest(spec=spec.candidate_id):
                ddl = spec.ddl()
                self.assertIn("PRIMARY KEY ((", ddl)
                self.assertIn("CLUSTERING ORDER BY", ddl)
                self.assertIn("CREATE TABLE IF NOT EXISTS", ddl)
                self.assertNotIn("ALLOW FILTERING", ddl)
                self.assertFalse(spec.complete_query_plan)
                self.assertFalse(spec.complete_write_plan)

    def test_variants_are_physically_distinct(self):
        self.assertIn("ts1_metric_bucket_1d", self.candidates)
        self.assertIn("ts1_metric_bucket_7d", self.candidates)
        self.assertNotEqual(self.candidates["ts1_metric_bucket_1d"].table_name,
                            self.candidates["ts1_metric_bucket_7d"].table_name)
        self.assertEqual(len([c for c in self.candidates.values() if c.family == "TS5"]), 4)

    def test_ts2_has_ten_cpu_metrics_co_located(self):
        wide = self.candidates["ts2_wide_bucket_1d"]
        self.assertTrue(set(self.metrics) <= set(wide.columns))
        self.assertEqual(wide.partition_key, ("hostname", "bucket_day"))
        self.assertEqual(wide.clustering_key, ("timestamp_ns",))

    def test_ts6_aggregation_requires_sum_and_count(self):
        spec = self.candidates["ts6_hour_host_dimension"]
        for m in self.metrics:
            self.assertEqual(spec.columns[f"sum_{m}"], "double")
            self.assertEqual(spec.columns[f"count_{m}"], "bigint")
        self.assertEqual(spec.partition_key, ("bucket_day", "hour_ns"))

    def test_invalid_policy_rejected(self):
        bad = dict(self.policy, select_winner_by_latency=True)
        with self.assertRaises(ValueError):
            build_prototypes(self.metrics, bad)
        bad = dict(self.policy, raw_bucket_days=[0])
        with self.assertRaises(ValueError):
            build_prototypes(self.metrics, bad)

    def test_illegal_filtering_rejected(self):
        from dataclasses import replace
        raw = self.candidates["ts0_metric_raw"]
        with self.assertRaises(ValueError):
            replace(raw, read_cql_template="SELECT * FROM foo ALLOW FILTERING").validate()


class EndToEndWithoutMutationTests(unittest.TestCase):
    """Run against existing Stage 3 generated input without copying/re-running it."""

    def test_outputs_do_not_mutate_upstream_and_cover_all_16_templates(self):
        upstream = ROOT / "outputs/tsbs_run/04_tsdb_activation_matrix.csv"
        before = upstream.read_bytes()
        with tempfile.TemporaryDirectory() as temp:
            out = Path(temp) / "generated"
            report = instantiate(ROOT, out)
            self.assertEqual(report["query_count"], 16)
            self.assertEqual(report["candidate_count"], 9)
            self.assertEqual(report["candidate_counts_by_family"],
                             {"TS0": 1, "TS1": 2, "TS2": 1, "TS3": 0, "TS4": 0,
                              "TS5": 4, "TS6": 1, "TS7": 0, "TS8": 0})
            self.assertEqual(report["query_family_candidate_links"], 37)
            self.assertFalse(report["query_plan_coverage_verified"])
            self.assertFalse(report["benchmark_executed"])
            self.assertEqual(len(list((out / "cql/schemas").glob("*.cql"))), 9)
            self.assertEqual(before, upstream.read_bytes())
            with (out / "13_query_candidate_map.csv").open(newline="", encoding="utf-8") as f:
                links = list(csv.DictReader(f))
            self.assertEqual(len(links), 37)
            for row in links:
                self.assertTrue(row["family"].startswith("TS"))
                self.assertIn("UNVERIFIED", row["coverage_status"])
            self.assertEqual({r["candidate_id"] for r in links if r["query_id"] == "Q15"},
                             {"ts0_metric_raw"})
            self.assertEqual({r["candidate_id"] for r in links if r["query_id"] == "Q13"},
                             {"ts0_metric_raw", "ts1_metric_bucket_1d", "ts1_metric_bucket_7d", "ts2_wide_bucket_1d"})
            self.assertEqual({r["candidate_id"] for r in links if r["query_id"] == "Q10"},
                             {"ts0_metric_raw", "ts5_sum_count_by_host_hour", "ts6_hour_host_dimension"})

    def test_fails_closed_if_unknown_ts_family_is_activated(self):
        """TS4 exists as a family, but has NO Stage4 candidate implementation yet."""
        with tempfile.TemporaryDirectory() as temp:
            temp_root = Path(temp) / "input"
            for rel in ["outputs/tsbs_run/04_tsdb_activation_matrix.csv",
                        "outputs/tsbs_run/05_temporal_modifier_matrix.csv",
                        "inputs/workload/query_manifest.csv",
                        "inputs/schema/tsbs_cpu_only_schema.json",
                        "config/tsdb_family_catalog.json", "config/tsdb_activation_rules.json",
                        "config/temporal_modifier_rules.json", "config/tsbs_stage4_physical_policy.json"]:
                target = temp_root / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / rel, target)
            p = temp_root / "outputs/tsbs_run/04_tsdb_activation_matrix.csv"
            with p.open(newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f)); cols = list(rows[0])
            rows[-1]["TS4"] = "true"
            activated = json.loads(rows[-1]["active_families"])
            activated.append("TS4")
            rows[-1]["active_families"] = json.dumps(activated)
            with p.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cols); writer.writeheader(); writer.writerows(rows)
            with self.assertRaisesRegex(ValueError, "no physical instantiator"):
                instantiate(temp_root, Path(temp) / "output")


if __name__ == "__main__":
    unittest.main()
