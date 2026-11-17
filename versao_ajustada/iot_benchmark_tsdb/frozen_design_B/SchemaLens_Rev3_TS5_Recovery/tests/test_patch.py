import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from fixture_patch import OLD_TEST, NEW_TEST, patched_text

class T(unittest.TestCase):
    def test_fixture_original_replaced_exactly_once(self):
        orig='before\n'+OLD_TEST+'after\n'
        new=patched_text(orig)
        self.assertIn(NEW_TEST,new)
        self.assertNotIn(OLD_TEST,new)
        self.assertEqual(new.replace(NEW_TEST,OLD_TEST),orig)
    def test_double_or_missing_refused(self):
        with self.assertRaises(ValueError):patched_text('x')
        with self.assertRaises(ValueError):patched_text(OLD_TEST+OLD_TEST)
    def test_source_v3_fixture_supported(self):
        import zipfile
        candidate=Path('/mnt/data/SchemaLens_IoT_Cassandra_V3_Automatico.zip')
        if not candidate.is_file():self.skipTest('Reference source not present')
        with zipfile.ZipFile(candidate) as z:
            content=z.read(next(n for n in z.namelist() if n.endswith('/tests/test_iot_stages_4_to_9.py'))).decode()
        self.assertNotEqual(content,patched_text(content))
    def test_new_tests_are_nonvacuous(self):
        self.assertIn("self.assertTrue(res, 'TS5 positive fixture must produce nonempty results')",NEW_TEST)
        self.assertIn("self.assertEqual(stage5.route_family(native_q,'TS5',p)[0], 'INELIGIBLE')",NEW_TEST)
        self.assertIn("self.assertEqual(stage5.route_family(q,'TS5',p)[0], 'CONDITIONAL')",NEW_TEST)
    def test_only_fixture_data(self):
        self.assertNotIn('CREATE TABLE', NEW_TEST)
        self.assertNotIn('DROP KEYSPACE', NEW_TEST)
        self.assertNotIn('INSERT INTO', NEW_TEST)

if __name__=='__main__':unittest.main()
