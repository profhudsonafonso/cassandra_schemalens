import json
import unittest
from pathlib import Path
import sys
import tempfile
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import recover_rev3 as R
class Rev3RegistrationTests(unittest.TestCase):
 def setUp(self):
  self.a=R.PRESETS['A'];self.b=R.PRESETS['B']
 def prereg(self,scenario='A'):
  p=R.PRESETS[scenario]
  return dict(devices=p['devices'],loops=p['loops'],batch_size=p['batch_size'],max_cells=p['max_cells'],
          generation_settings={k:p[k] for k in ('DATA_SEED','QUERY_SEED','POINT_STEP','QUERY_INTERVAL','GROUP_BY_TIME_UNIT')},
          sql_replay=False,operation_frequency_replay=False)
 def test_campaign_A_preregistration(self):
  self.assertEqual(R.assert_campaign_preregistration(self.prereg(),'A'),self.a)
 def test_campaign_B_preregistration(self):
  self.assertEqual(R.assert_campaign_preregistration(self.prereg('B'),'B'),self.b)
 def test_A_reject_B_seeds(self):
  with self.assertRaisesRegex(ValueError,'Preregistered'):R.assert_campaign_preregistration(self.prereg('B'),'A')
 def test_groupby_must_stay_preregistered(self):
  p=self.prereg();p['generation_settings']['GROUP_BY_TIME_UNIT']=20000
  with self.assertRaisesRegex(ValueError,'GROUP_BY_TIME_UNIT'):R.assert_campaign_preregistration(p,'A')
 def test_disallow_sql_replay_claim(self):
  p=self.prereg();p['sql_replay']=True
  with self.assertRaisesRegex(ValueError,'one-sample'):R.assert_campaign_preregistration(p,'A')
 def test_refuse_existing_output_and_existing_alias(self):
  with tempfile.TemporaryDirectory() as t:
   ws=Path(t);run=ws/'java_work/runs/v3_test';run.mkdir(parents=True)
   (run/'run_status.json').write_text(json.dumps(dict(generate='OK',capture='OK',isolate='OK',prebenchmark='FAILED')))
   d=R.ensure_fresh(ws,run,'A')
   self.assertEqual(d[0],run/'schemalens_rev3_groupby_aligned_A')
   (ws/'HOLDOUT_GENERATION_OUTPUT.json').write_text('{}')
   with self.assertRaisesRegex(FileExistsError,'already exists'):R.ensure_fresh(ws,run,'A')
   (ws/'HOLDOUT_GENERATION_OUTPUT.json').unlink()
   (run/'native_queries_q12fixed_existing.jsonl').write_text('sample')
   with self.assertRaisesRegex(FileExistsError,'alias'):R.ensure_fresh(ws,run,'A')
 def test_standard_result_and_registry_shape(self):
  with tempfile.TemporaryDirectory() as t:
   root=Path(t);p=self.prereg()
   out=R.output_record(root/'exp',root,'abcd',root/'alias')
   self.assertEqual(out['status'],'JAVA_GENERATOR_COMPLETE_NO_CASSANDRA')
   self.assertTrue(out['alias_has_identical_bytes'])
   reg=R.registry_record(root/'exp',root,root/'project',root/'oldv3',root/'oldr2',p,dict(new=dict(data='a',queries='b')))
   self.assertEqual(reg['scale']['group_by_ms'],3600000)
   self.assertEqual(reg['generator_seeds']['data_seed'],531007)
   self.assertEqual(reg['status'],'NEW_JAVA_GENERATION_UNMEASURED')
if __name__=='__main__':unittest.main()
