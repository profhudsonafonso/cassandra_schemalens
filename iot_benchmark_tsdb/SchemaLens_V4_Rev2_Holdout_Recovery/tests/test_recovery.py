from pathlib import Path
import tempfile
import unittest
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import recover_holdout as R

class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        base=Path(self.tmp.name)
        self.ref=base/'frozen_old';self.ws=base/'holdout'
        old=self.ref/'outputs/stage8/28_hbench_frozen.json'
        old.parent.mkdir(parents=True)
        old.write_text('[{"example":"old"}]',encoding='utf-8')
        self.old_sha=R.ORIGINAL_HBENCH_SHA
        R.ORIGINAL_HBENCH_SHA=R.digest(old)
        for rel,content in [('phase_a1/outputs/normalized_double_readings.csv','OLD DATA'),
            ('phase_a1/outputs/query_instances_pilot.json','OLD QUERIES')]:
            p=self.ref/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(content)
        self.run=self.ws/'java_work/runs/v3_20300101T000000Z_pid101'
        ops=['PRECISE_QUERY','RANGE_QUERY','VALUE_RANGE_QUERY','AGG_RANGE_QUERY','AGG_VALUE_QUERY',
             'AGG_RANGE_VALUE_QUERY','GROUP_BY_QUERY','LATEST_POINT_QUERY','RANGE_QUERY_ORDER_BY_TIME_DESC',
             'VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC','GROUP_BY_QUERY_ORDER_BY_TIME_DESC','SET_OP_QUERY']
        capture='\n'.join(json.dumps({'origin':'iot_benchmark_generate_query_workload', 'operation':op,
             'query':({'childRangeQueries':[{'deviceSchema':[{'device':'d0','sensors':[{'name':'s_0'}]}]},
                                          {'deviceSchema':[{'device':'d0','sensors':[{'name':'s_0'}]}]}],
                        'setOpType':'union'} if i==12 else
                       {'deviceSchema':[{'device':'d0','sensors':[{'name':'s_0'}]}]})})
             for i,op in enumerate(ops,1))
        for rel,content in [('native_queries.jsonl',capture),
                            ('native_data/schema.txt','SCHEMA'),('native_data/info.txt','INFO'),
                            ('native_data/d_0/BigBatch_0.csv','CSV'),
                            ('java_config/config.properties','DATA_SEED=77\nQUERY_SEED=88\nPOINT_STEP=600000\n')]:
            p=self.run/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(content)
        src=self.ws/'schema_project_isolated'
        runner=src/'scripts/run_native_prebenchmark.sh';runner.parent.mkdir(parents=True)
        runner.write_text('#!/bin/bash\nset -e\npython3 - <<\'PY2\'\n'
              'from pathlib import Path\nimport hashlib,json\n'
              "p=Path('outputs/stage8/28_hbench_frozen.json');p.parent.mkdir(parents=True,exist_ok=True);p.write_text('[{\"id\": \"new\"}]')\n"
              "for name,content in [('phase_a1/outputs/normalized_double_readings.csv','NEW DATA'),('phase_a1/outputs/query_instances_pilot.json','NEW QUERIES')]:\n"
              ' q=Path(name);q.parent.mkdir(parents=True,exist_ok=True);q.write_text(content)\n'
              'sh=lambda x:hashlib.sha256(x.read_bytes()).hexdigest()\n'
              "m={'status':'FROZEN_PREBENCH_SELECTION','benchmark_measured':False,'frozen_hbench_sha256':sh(p),'inputs_sha256':{}}\n"
              "Path('outputs/stage8/32_frozen_manifest.json').write_text(json.dumps(m))\n"
              'PY2\n')
        generator=src/'scripts/auto_official_v3.py';generator.write_text('# mock driver')
        reference=src/'iot_pipeline/native_reference_v3.py';reference.parent.mkdir(parents=True);reference.write_text('# mock reference')
        val_src=Path('/mnt/data/_rev2_integrated_smoke/schemalens_q12fixed_smoke/phase_a1/tools/official_query_capture.py')
        validator=src/'phase_a1/tools/official_query_capture.py';validator.parent.mkdir(parents=True,exist_ok=True)
        # Minimal stand-in for the original validator is sufficient for orchestration tests.
        validator.write_text("def validate(path,expected=None):\n import json\n lines=[json.loads(x) for x in path.read_text().splitlines()]\n assert len(lines)==12\n return {x:[] for x in expected},{x:1 for x in expected}\n")
        prereg={'java_upstream_commit':'62e0dc100e56443978c8b4c66472b1d8a7ef0238',
                'reference_v3_hbench_sha256':R.ORIGINAL_HBENCH_SHA,
                'generation_settings':{'DATA_SEED':77,'QUERY_SEED':88,'POINT_STEP':600000},
                'original_generator_script_sha256':R.digest(generator),
                'copied_reference_adapter_sha256':R.digest(reference),'max_cells':100000}
        self.ws.mkdir(exist_ok=True);(self.ws/'HOLDOUT_GENERATION_PREREGISTRATION.json').write_text(json.dumps(prereg))
    def tearDown(self):
        R.ORIGINAL_HBENCH_SHA=self.old_sha
        self.tmp.cleanup()
    def test_inspection_run_selection(self):
        self.assertEqual(R.choose_run(self.ws,None),self.run)
        self.assertFalse((self.run/R.RUN_SUFFIX).exists())
    def test_dry_run_no_write(self):
        R.recover(self.ws,self.run,self.ref,dry_run=True)
        self.assertFalse((self.run/R.RUN_SUFFIX).exists())
        self.assertFalse((self.run/R.ALIAS).exists())
    def test_recovery_separate_and_hash_verifiable(self):
        exp=R.recover(self.ws,self.run,self.ref)
        self.assertEqual(exp,self.run/R.RUN_SUFFIX)
        self.assertTrue((exp/'outputs/stage8/32_frozen_manifest.json').is_file())
        self.assertTrue((self.run/R.ALIAS).is_file())
        meta=R.read_json(self.run/'REV2_RECOVERY_MANIFEST.json')
        self.assertEqual(meta['status'],'RECOVERED_FRESH_JAVA_V3_STAGE8_READY_FOR_REV2')
        with self.assertRaises(FileExistsError):R.recover(self.ws,self.run,self.ref)
    def test_missing_java_source_fails_cleanly(self):
        (self.run/'native_queries.jsonl').unlink()
        with self.assertRaises(FileNotFoundError):R.capture_check(self.ws,self.run,self.ref)
        self.assertFalse((self.run/R.RUN_SUFFIX).exists())
    def test_wrong_java_seeds_fails(self):
        (self.run/'java_config/config.properties').write_text('DATA_SEED=666\nQUERY_SEED=88\nPOINT_STEP=600000\n')
        with self.assertRaises(ValueError):R.capture_check(self.ws,self.run,self.ref)
    def test_preregister_driver_hash_guard(self):
        (self.ws/'schema_project_isolated/scripts/auto_official_v3.py').write_text('modified')
        with self.assertRaises(ValueError):R.capture_check(self.ws,self.run,self.ref)
if __name__=='__main__':unittest.main()
