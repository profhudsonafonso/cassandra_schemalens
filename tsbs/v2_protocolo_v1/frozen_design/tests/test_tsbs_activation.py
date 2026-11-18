"""TS-only SchemaLens TSBS reproducibility tests. Run: python3 -m unittest discover -s tests -v"""
import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path

from methodology.common.rule_engine import activate
from methodology.tsbs.evidence_builder import build_tsbs_evidence, sql_for_evidence_only
from methodology.tsbs.run_tsbs import ROOT, build_conceptual_schema, parse_args, read_json, run
from methodology.tsbs.workload import TEMPLATES, logical_sql, validate_workload


class TSBSActivationTests(unittest.TestCase):
    def setUp(self):
        self.policy = read_json(ROOT / "config/temporal_policy.json")
        self.ts_rules = read_json(ROOT / "config/tsdb_activation_rules.json")
        self.mod_rules = read_json(ROOT / "config/temporal_modifier_rules.json")
        self.schema = build_conceptual_schema(100, 60, 3, None, False)
        self.by_type = {t.query_type: t for t in TEMPLATES}

    def evidence(self, typ):
        t = self.by_type[typ]
        return build_tsbs_evidence(t, logical_sql(t), self.schema, self.policy)

    def activated(self, typ):
        return activate(self.evidence(typ)["values"], self.ts_rules)[0]

    def test_exact_16_official_templates(self):
        self.assertEqual(len(TEMPLATES), 16)
        self.assertEqual(len({t.query_type for t in TEMPLATES}), 16)
        self.assertIn("cpu-max-all-32-24", self.by_type)
        self.assertIn("groupby-orderby-limit", self.by_type)
        self.assertIn("lastpoint", self.by_type)
        self.assertTrue(all(t.query_id.startswith("Q") for t in TEMPLATES))

    def test_upstream_original_rules_and_engine_not_edited(self):
        self.assertEqual(hashlib.sha256((ROOT / "config/tsdb_activation_rules.json").read_bytes()).hexdigest(),
                         "8c150807b56f306670902be98967966c50de76394310cc681fca6893f50e8afd")
        self.assertEqual(hashlib.sha256((ROOT / "methodology/common/rule_engine.py").read_bytes()).hexdigest(),
                         "cb04df0ae46c8856068cbcf0d6690a980aa8c7c1829d13c9094d2ce3e18cc3be")
        self.assertFalse((ROOT / "config/cassandra_activation_rules.json").exists())

    def test_main_expected_activation(self):
        self.assertEqual(self.activated("single-groupby-1-1-1"), ["TS0", "TS5"])
        self.assertEqual(self.activated("double-groupby-5"), ["TS0", "TS5", "TS6"])
        self.assertEqual(self.activated("high-cpu-1"), ["TS0", "TS1", "TS2"])
        self.assertEqual(self.activated("high-cpu-all"), ["TS0", "TS1"])
        self.assertEqual(self.activated("lastpoint"), ["TS0"])
        self.assertEqual(self.activated("groupby-orderby-limit"), ["TS0", "TS5"])

    def test_lastpoint_not_incorrectly_detected_as_limit_one(self):
        rec = self.evidence("lastpoint")
        self.assertTrue(rec["values"]["latest_per_group_semantics"])
        self.assertFalse(rec["values"]["latest_only"])
        self.assertNotIn("TS4", self.activated("lastpoint"))

    def test_no_accidental_activation_of_unsupported_ts_types(self):
        all_activated = set()
        for t in TEMPLATES:
            all_activated.update(self.activated(t.query_type))
        self.assertEqual(all_activated, {"TS0", "TS1", "TS2", "TS5", "TS6"})
        self.assertNotIn("TS7", all_activated)
        self.assertNotIn("TS8", all_activated)

    def test_half_open_range_is_normalized_for_evidence_only(self):
        s = logical_sql(self.by_type["high-cpu-1"])
        self.assertIn("c.event_ts < :end_ts", s)
        n = sql_for_evidence_only(s)
        self.assertIn("c.event_ts BETWEEN :start_ts AND :end_ts", n)
        rec = self.evidence("high-cpu-1")
        self.assertTrue(rec["values"]["has_time_range"])
        self.assertIn("c.event_ts < :end_ts", rec["source_sql"])

    def test_no_retention_or_backfill_assumed(self):
        ev = self.evidence("high-cpu-1")["values"]
        self.assertIsNone(ev["retention_days"])
        self.assertIsNone(self.schema["entities"]["CPUReading"]["temporal"]["late_arrival_allowed"])
        self.assertFalse(ev["hot_series"])
        self.assertTrue(ev["append_oriented"])

    def test_explicit_retention_and_backfill_scenario(self):
        s = build_conceptual_schema(100, 60, 3, 730, True)
        t = self.by_type["high-cpu-1"]
        values = build_tsbs_evidence(t, logical_sql(t), s, self.policy)["values"]
        active = activate(values, self.ts_rules)[0]
        mod_active = []
        for rule in self.mod_rules["rules"]:
            from methodology.common.rule_engine import evaluate_rule
            if evaluate_rule(values, rule, active + mod_active).passed:
                mod_active.append(rule["family"])
        self.assertEqual(mod_active, ["M1", "M2", "M3", "M5"])

    def test_invalid_host_count_guard(self):
        with self.assertRaisesRegex(ValueError, "requires >= 32 hosts"):
            validate_workload(20, 60, 3)

    def test_run_outputs_only_tsdb_and_modifier_files(self):
        with tempfile.TemporaryDirectory() as d:
            args = parse_args(["--output-dir", d])
            summary = run(args)
            self.assertEqual(summary["template_count"], 16)
            self.assertEqual(summary["families_activated_counts"],
                             {"TS0": 16, "TS1": 2, "TS2": 1, "TS5": 13, "TS6": 3})
            self.assertEqual(summary["modifier_activated_counts"], {"M1": 15, "M3": 16})
            self.assertFalse((Path(d) / "03_cassandra_activation_matrix.csv").exists())
            self.assertTrue((Path(d) / "06_full_activation_trace.json").exists())
            with (Path(d) / "04_tsdb_activation_matrix.csv").open(newline="") as f:
                rows = list(csv.DictReader(f))
            self.assertEqual(len(rows), 16)
            full = json.loads((Path(d) / "06_full_activation_trace.json").read_text())
            self.assertTrue(all("cassandra_rule_trace" not in x for x in full))
            self.assertTrue(all(len(x["tsdb_rule_trace"]) == 9 for x in full))
            self.assertTrue(all(len(x["modifier_rule_trace"]) == 5 for x in full))


if __name__ == "__main__":
    unittest.main()
