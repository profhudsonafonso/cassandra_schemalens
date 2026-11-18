from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import recover_test_contract as R

class ContractTests(unittest.TestCase):
    def test_replaces_exact_one_test(self):
        old='HEADER\n'+R.OLD_TEST+'TAIL\n'
        patched=R.replacement(old)
        self.assertEqual(patched,'HEADER\n'+R.NEW_TEST+'TAIL\n')
        self.assertNotEqual(old,patched)
    def test_refuses_drifted_source(self):
        with self.assertRaisesRegex(ValueError,'Unexpected'):
            R.replacement('no test here')
    def test_refuses_second_attempt(self):
        with self.assertRaisesRegex(ValueError,'Unexpected'):
            R.replacement('H\n'+R.NEW_TEST+'T')
    def test_refuses_duplicate_old_test(self):
        with self.assertRaisesRegex(ValueError,'Unexpected'):
            R.replacement(R.OLD_TEST*2)
    def test_new_test_checks_native_and_positive_case(self):
        self.assertIn("'INELIGIBLE'",R.NEW_TEST)
        self.assertIn("'CONDITIONAL'",R.NEW_TEST)
        self.assertIn('self.assertTrue(eligible',R.NEW_TEST)
        self.assertIn('self.assertTrue(res',R.NEW_TEST)
        self.assertNotIn('q[\'group_by_ms\']=3600000',R.NEW_TEST)

if __name__=='__main__':unittest.main()
