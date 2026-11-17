"""Offline safety tests for s3_physical.py (no Cassandra, no Docker needed).

Run from the project root:
    python -m unittest -v experiments.rev3_s3_physical_storage.tests.test_s3
"""
import re
import sys
import tempfile
import unittest
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT))
import s3_physical as s3  # noqa: E402

SOURCES = [PROJECT / 's3_physical.py', PROJECT / 's3_run_all.py']


class SourceSafety(unittest.TestCase):
    def test_no_destructive_statements_in_source(self):
        banned = [r'DROP\s+KEYSPACE', r'DROP\s+TABLE', r'TRUNCATE\s+\w', r'_create_keyspace\s*\(',
                  r'run_live\s*\(', r"'compact'", r'nodetool\s+compact', r'shutil\.rmtree', r'\.unlink\((?!missing_ok)',
                  r'IF\s+NOT\s+EXISTS\s+\'?\s*\+?\s*ks']
        for src in SOURCES:
            text = src.read_text(encoding='utf8')
            for pat in banned:
                self.assertIsNone(re.search(pat, text), f'{pat} found in {src.name}')

    def test_s2_artifacts_never_opened_for_writing(self):
        text = (PROJECT / 's3_physical.py').read_text(encoding='utf8')
        # every write helper uses exclusive-create mode
        self.assertNotRegex(text, r"open\(\s*['\"]w")
        self.assertNotRegex(text, r"\.open\(\s*['\"]w")
        self.assertNotRegex(text, r"write_text\(")


class Naming(unittest.TestCase):
    def test_twelve_names(self):
        all_names = [ks for f in s3.FACTORS for ks in s3.names(f).values()]
        self.assertEqual(len(set(all_names)), 12)
        self.assertTrue(all(s3.KS_RE.fullmatch(n) for n in all_names))
        self.assertEqual(s3.names(8)[s3.D], 'sl_s3_r1_d8_d')

    def test_regex_rejects_foreign(self):
        for bad in ('sl_s2_d1_ref', 'sl_s3_r2_d1_ref', 'sl_s3_r1_d3_u', 'sl_conf_r3_a927efbd66_reference',
                    'sl_s3_r1_d1_ref;DROP', 'system', ''):
            self.assertIsNone(s3.KS_RE.fullmatch(bad), bad)


class NodetoolWhitelist(unittest.TestCase):
    def test_refused(self):
        for args in (('compact',), ('drain',), ('flush',), ('flush', 'sl_s2_d1_u'), ('truncatehints',),
                     ('flush', 'sl_s3_r1_d1_u', 'system'), ('tablestats', 'sl_s2_d1_u'), ('clearsnapshot',),
                     ('invalidatekeycache',), ('disableautocompaction',)):
            with self.assertRaises(PermissionError, msg=str(args)):
                s3.nodetool(*args)


class Guard(unittest.TestCase):
    def test_guarded_session(self):
        calls = []

        class Fake:
            def execute(self, q, *a, **k):
                calls.append(q)

            def prepare(self, q, *a, **k):
                calls.append(q)

            def set_keyspace(self, ks):
                calls.append(ks)
        g = s3.GuardedSession(Fake())
        for q in ('DROP KEYSPACE sl_s3_r1_d1_u', ' truncate t', 'ALTER TABLE x', 'DELETE FROM t', 'UPDATE t SET',
                  'CREATE KEYSPACE sl_s2_d1_u WITH x', 'CREATE KEYSPACE IF NOT EXISTS other WITH x'):
            with self.assertRaises(PermissionError, msg=q):
                g.execute(q)
        with self.assertRaises(PermissionError):
            g.set_keyspace('sl_s2_d1_u')
        g.execute("CREATE KEYSPACE sl_s3_r1_d1_u WITH replication = {}")
        g.execute('CREATE TABLE IF NOT EXISTS t (a int PRIMARY KEY)')
        g.prepare('INSERT INTO t (a) VALUES (?)')
        self.assertEqual(len(calls), 3)


class Helpers(unittest.TestCase):
    def test_nearest_rank(self):
        self.assertEqual(s3.percentile(list(range(1, 61)), 95), 57)
        self.assertEqual(s3.percentile(list(range(1, 61)), 99), 60)

    def test_write_new_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'x.json'
            s3.write_new(p, {'a': 1})
            with self.assertRaises(FileExistsError):
                s3.write_new(p, {'a': 2})

    def test_parse_and_gate_on_real_s2_snapshot(self):
        txt = (PROJECT / 'experiments/rev3_s2_partition_growth/storage/sl_s2_d8_ref_tablestats.txt').read_text()
        m = s3.parse_tablestats(txt)['iot_reference_table']
        self.assertEqual(m['sstable_count'], 1)
        self.assertEqual(m['space_live_bytes'], 3430775)
        self.assertEqual(m['partition_mean_bytes'], 943127)
        self.assertEqual(s3.gate_table(m), [])
        txt_u = (PROJECT / 'experiments/rev3_s2_partition_growth/storage/sl_s2_d8_u_tablestats.txt').read_text()
        mu = s3.parse_tablestats(txt_u)['ts2_iot_wide_unbucketed']
        self.assertTrue(s3.gate_table(mu))  # memtable-only S2 state must FAIL the S3 gate

    def test_protocol_constants(self):
        self.assertEqual((s3.ROUNDS, s3.WARMUPS, s3.PAGE_SIZE, s3.SEED), (60, 5, 16, 20261127))
        self.assertEqual(s3.FACTOR_ORDER, (1, 2, 4, 8))


if __name__ == '__main__':
    unittest.main()
