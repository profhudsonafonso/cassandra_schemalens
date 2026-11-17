"""Tests for the V3 generator's configuration and failure diagnostic patch.

No network, Java compiler, Maven or Cassandra required.
"""
import importlib.util
from pathlib import Path
import tempfile
import unittest

# Run this test AFTER the patched script has been copied into the V3 project.
ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / 'scripts' / 'auto_official_v3.py'
SPEC = importlib.util.spec_from_file_location('auto_official_v3', SCRIPT)
auto = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(auto)


class TestGeneratorDiagnostics(unittest.TestCase):
    def test_java_jvm_system_property_is_set_before_main(self):
        args = auto.native_java_command('a:b', Path('/tmp/config_profile'))
        self.assertEqual(args[0], 'java')
        self.assertIn('-Dbenchmark-conf=/tmp/config_profile', args)
        self.assertEqual(args[-3:], [auto.APP_CLASS, '-cf', '/tmp/config_profile'])
        self.assertLess(args.index('-Dbenchmark-conf=/tmp/config_profile'), args.index('-cp'))

    def test_log_tail_includes_failure_cause(self):
        with tempfile.TemporaryDirectory() as folder:
            log = Path(folder) / 'java.log'
            log.write_text('start\nWARNING\nERROR: wrong mode\n')
            self.assertEqual(auto.log_tail(log, lines=2), 'WARNING\nERROR: wrong mode')

    def test_build_config_preserves_native_mode_and_absolute_data(self):
        with tempfile.TemporaryDirectory() as folder:
            output = Path(folder) / 'conf/config.properties'
            dataset = Path(folder) / 'data'
            auto.build_config(auto.PROFILE, output, dataset,
                              devices=3, loops=60, batch_size=5)
            data = output.read_text()
            self.assertIn('BENCHMARK_WORK_MODE=generateDataMode', data)
            self.assertIn(f'FILE_PATH={dataset.resolve()}', data)


if __name__ == '__main__':
    unittest.main()
