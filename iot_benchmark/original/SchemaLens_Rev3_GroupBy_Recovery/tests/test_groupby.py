from pathlib import Path
import tempfile
import unittest
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import recover_groupby as R

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
                            ('java_config/config.properties','DATA_SEED=77\nQUERY_SEED=88\nPOINT_STEP=600000\nGROUP_BY_TIME_UNIT=3600000\nQUERY_DEVICE_NUM=1\nQUERY_SENSOR_NUM=2\nQUERY_LOWER_VALUE=10.0\nQUERY_AGGREGATE_FUN=count\n')]:
            p=self.run/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(content)
        src=self.ws/'schema_project_isolated'
        scen=src/'phase_a1/inputs/scenario.json';scen.parent.mkdir(parents=True,exist_ok=True)
        scen.write_text(json.dumps({'group_by_time_unit_ms':20000,'query_device_num':1,'query_sensor_num':2,'query_lower_value':10.0,'query_aggregate_fun':'count','distinct':'unchanged'}))
        runner=src/'scripts/run_native_prebenchmark.sh';runner.parent.mkdir(parents=True)
        runner.write_text('#!/bin/bash\nset -e\npython3 - <<\'PY2\'\n'
              'assert __import__("json").loads(open("phase_a1/inputs/scenario.json").read())["group_by_time_unit_ms"] == 3600000\n'
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
        validator=src/'phase_a1/tools/official_query_capture.py';validator.parent.mkdir(parents=True,exist_ok=True)
        # Minimal stand-in for the original validator is sufficient for orchestration tests.
        validator.write_text("def validate(path,expected=None):\n import json\n lines=[json.loads(x) for x in path.read_text().splitlines()]\n assert len(lines)==12\n return {x:[] for x in expected},{x:1 for x in expected}\n")
        prereg={'java_upstream_commit':'62e0dc100e56443978c8b4c66472b1d8a7ef0238',
                'reference_v3_hbench_sha256':R.ORIGINAL_HBENCH_SHA,
                'generation_settings':{'DATA_SEED':77,'QUERY_SEED':88,'POINT_STEP':600000,'GROUP_BY_TIME_UNIT':3600000},
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
        meta=R.read_json(self.run/R.META_NAME)
        self.assertEqual(meta['status'],'RECOVERED_FRESH_JAVA_STAGE8_GROUPBY_ALIGNED_READY_FOR_REV2')
        self.assertEqual(meta['scenario_alignment']['scenario_original_ms'],20000)
        self.assertEqual(meta['scenario_alignment']['scenario_corrected_ms'],3600000)
        self.assertEqual(R.read_json(exp/'phase_a1/inputs/scenario.json')['distinct'],'unchanged')
        self.assertEqual(R.read_json(self.ws/'schema_project_isolated/phase_a1/inputs/scenario.json')['group_by_time_unit_ms'],20000)
        with self.assertRaises(FileExistsError):R.recover(self.ws,self.run,self.ref)
    def test_missing_java_source_fails_cleanly(self):
        (self.run/'native_queries.jsonl').unlink()
        with self.assertRaises(FileNotFoundError):R.capture_check(self.ws,self.run,self.ref)
        self.assertFalse((self.run/R.RUN_SUFFIX).exists())
    def test_wrong_java_seeds_fails(self):
        (self.run/'java_config/config.properties').write_text('DATA_SEED=666\nQUERY_SEED=88\nPOINT_STEP=600000\nGROUP_BY_TIME_UNIT=3600000\n')
        with self.assertRaises(ValueError):R.capture_check(self.ws,self.run,self.ref)
    def test_preregister_driver_hash_guard(self):
        (self.ws/'schema_project_isolated/scripts/auto_official_v3.py').write_text('modified')
        with self.assertRaises(ValueError):R.capture_check(self.ws,self.run,self.ref)
    def test_reject_java_groupby_changed_after_prereg(self):
        p=self.run/'java_config/config.properties'
        p.write_text(p.read_text().replace('GROUP_BY_TIME_UNIT=3600000','GROUP_BY_TIME_UNIT=900000'))
        with self.assertRaises(ValueError):R.capture_check(self.ws,self.run,self.ref)
        self.assertFalse((self.run/R.RUN_SUFFIX).exists())
    def test_reject_second_scenario_mismatch(self):
        p=self.ws/'schema_project_isolated/phase_a1/inputs/scenario.json'
        data=R.read_json(p);data['query_sensor_num']=1;p.write_text(json.dumps(data))
        with self.assertRaisesRegex(ValueError,'Non-group-by'):
            R.recover(self.ws,self.run,self.ref,dry_run=True)
        self.assertFalse((self.run/R.RUN_SUFFIX).exists())
    def test_reject_duplicate_java_groupby(self):
        p=self.run/'java_config/config.properties'
        p.write_text(p.read_text()+'GROUP_BY_TIME_UNIT=3600000\n')
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            R.recover(self.ws,self.run,self.ref,dry_run=True)
        self.assertFalse((self.run/R.RUN_SUFFIX).exists())
    def test_dry_run_keeps_pristine_scenario(self):
        orig=self.ws/'schema_project_isolated/phase_a1/inputs/scenario.json'
        expected=R.digest(orig)
        R.recover(self.ws,self.run,self.ref,dry_run=True)
        self.assertEqual(R.digest(orig),expected)
if __name__=='__main__':unittest.main()
