"""Offline tests for safe V3 automation orchestration. No network, Maven, or Cassandra."""
import importlib.util
from pathlib import Path
import tempfile
import unittest

PROJECT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT / 'scripts/auto_official_v3.py'
spec = importlib.util.spec_from_file_location('auto_official_v3', SCRIPT)
auto = importlib.util.module_from_spec(spec)
spec.loader.exec_module(auto)

class TestOfficialAutoV3(unittest.TestCase):
    def test_config_contains_native_generator_mode(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td)
            cfg = auto.build_config(auto.PROFILE, p / 'config.properties',p / 'data',devices=8,loops=15,batch_size=5)
            values={line.split('=',1)[0]:line.split('=',1)[1] for line in cfg.read_text().splitlines() if line and not line.startswith('#') and '=' in line}
            self.assertEqual(values['BENCHMARK_WORK_MODE'],'generateDataMode')
            self.assertEqual(values['DEVICE_NUMBER'],'8')
            self.assertEqual(values['LOOP'],'15')
            self.assertEqual(values['SENSOR_NUMBER'],'2')
            self.assertEqual(values['QUERY_SET_OP_NUM'],'2')
            self.assertEqual(values['QUERY_SET_OP_TYPE'],'union')
            self.assertEqual(values['INSERT_DATATYPE_PROPORTION'],'0:0:0:0:1:0:0:0:0:0:0')
            self.assertEqual(values['NULL_RATIO'],'0')
            self.assertEqual(values['FILE_PATH'],str((p/'data').resolve()))
            self.assertEqual(values['TIMESTAMP_PRECISION'],'ms')
            self.assertEqual(values['IoTDB_DIALECT_MODE'],'table')
            self.assertEqual(values['QUERY_SENSOR_NUM'],'2')
            self.assertEqual(values['OPERATION_PROPORTION'].count(':'),12)
    def test_reject_invalid_scales(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(auto.SetupError):
                auto.build_config(auto.PROFILE,Path(td)/'x',Path(td)/'data',devices=1,loops=10,batch_size=3)
            with self.assertRaises(auto.SetupError):
                auto.build_config(auto.PROFILE,Path(td)/'x',Path(td)/'data',devices=3,loops=0,batch_size=3)
    def test_fresh_pipeline_copy_excludes_old_outputs(self):
        with tempfile.TemporaryDirectory() as td:
            dst=Path(td)/'fresh_project'
            auto.isolate_pipeline(dst)
            self.assertFalse((dst/'outputs').exists())
            self.assertFalse((dst/'phase_a1/outputs').exists())
            self.assertTrue((dst/'scripts/run_native_prebenchmark.sh').exists())
            self.assertTrue((dst/'java_bridge/cn/edu/tsinghua/iot/benchmark/tools/SchemaLensQueryCapture.java').exists())
    def test_dry_run_does_not_write_files(self):
        with tempfile.TemporaryDirectory() as td:
            work=Path(td)/'does_not_exist'
            code=auto.main(['--dry-run','--workdir',str(work),'--devices','3'])
            self.assertEqual(code,0)
            self.assertFalse(work.exists())
    def test_guard_against_importing_truncated_dataset(self):
        with self.assertRaises(auto.SetupError):
            auto.main(['--dry-run','--devices','100', '--loops','10000', '--max-cells','1'])
    def test_sha256_streamed(self):
        with tempfile.TemporaryDirectory() as td:
            p=Path(td)/'test';p.write_bytes(b'abc')
            self.assertEqual(auto.sha256(p),'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad')

if __name__ == '__main__':
    unittest.main()
