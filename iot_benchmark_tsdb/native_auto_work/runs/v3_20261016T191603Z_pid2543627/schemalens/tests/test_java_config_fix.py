"""Regression tests for pinned IoT Benchmark's required native auxiliary config files.

Run after installing scripts/auto_official_v3.py into the V3 project.
No network, Maven or Cassandra is required.
"""
import importlib.util
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('auto_official_v3', ROOT / 'scripts/auto_official_v3.py')
auto = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(auto)


class TestNativeSupportConfig(unittest.TestCase):
    def test_copies_function_xml_and_logging_without_overwriting_properties(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source = root / 'upstream/configuration/conf'
            source.mkdir(parents=True)
            (source / 'function.xml').write_text('<functions/>')
            (source / 'log4j.properties').write_text('log4j.rootLogger=INFO, CONSOLE')
            (source / 'logback.xml').write_text('<configuration/>')
            output = root / 'run/java_config'
            output.mkdir(parents=True)
            (output / 'config.properties').write_text('BENCHMARK_WORK_MODE=generateDataMode')
            names = auto.copy_upstream_support_files(source, output)
            self.assertEqual(names, ['function.xml', 'log4j.properties', 'logback.xml'])
            self.assertEqual((output / 'function.xml').read_text(), '<functions/>')
            self.assertEqual((output / 'config.properties').read_text(), 'BENCHMARK_WORK_MODE=generateDataMode')

    def test_missing_function_xml_is_fatal(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            source = root / 'upstream'
            source.mkdir()
            (source / 'log4j.properties').write_text('ok')
            with self.assertRaisesRegex(auto.SetupError, 'function.xml'):
                auto.copy_upstream_support_files(source, root / 'java_config')

    def test_both_jvm_invocations_receive_same_configuration(self):
        config = Path('/tmp/native/java_config')
        expected = '-Dbenchmark-conf=/tmp/native/java_config'
        app = auto.native_java_command('core.jar:deps', config)
        cap = auto.capture_java_command('sampler.jar:core.jar', config,
                                        Path('/tmp/q.jsonl'), 3)
        for args in [app, cap]:
            self.assertIn(expected, args)
            self.assertIn('-Dlog4j.configuration=file:///tmp/native/java_config/log4j.properties', args)
            self.assertLess(args.index(expected), args.index('-cp'))
        self.assertEqual(app[-3:], [auto.APP_CLASS, '-cf', str(config)])
        self.assertEqual(cap[-3:], [auto.CAPTURE_CLASS, '/tmp/q.jsonl', '3'])

    def test_native_output_log_diagnostics_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'native_generation.log'
            path.write_text('some information\nERROR failure in initInnerFunction\n')
            self.assertIn('initInnerFunction', auto.log_tail(path))


if __name__ == '__main__':
    unittest.main()
