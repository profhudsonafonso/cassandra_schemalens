"""Regression tests for diagnostic retention when nodetool storage collection fails."""
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from methodology.tpcx_iot.stage91.experiment import capture_storage, parse_nodetool_bytes
from methodology.tpcx_iot.stage91.storage_diagnostic import main as diagnostic_main


class StorageCollectionFixTests(unittest.TestCase):
    def test_parser_plain_bytes_and_human_units(self):
        self.assertEqual(parse_nodetool_bytes('Space used (live): 1,024\n'), 1024)
        self.assertEqual(parse_nodetool_bytes('\t\tSpace used (live): 3 KiB\n'), 3072)
        self.assertEqual(parse_nodetool_bytes('Space used (live): 1.5 MiB\n'), 1572864)
        self.assertEqual(parse_nodetool_bytes('Space used (live): 51.62 MB\n'), 51620000)
        self.assertIsNone(parse_nodetool_bytes('Space used (live): 1\nSpace used (live): 2\n'))

    def test_success_saves_raw_output_and_diagnostics(self):
        success=[SimpleNamespace(returncode=0,stdout='',stderr=''),
                 SimpleNamespace(returncode=0,stdout='Table: raw\n  Space used (live): 7,321\n',stderr='')]
        with tempfile.TemporaryDirectory() as td, patch(
                'methodology.tpcx_iot.stage91.experiment.subprocess.run',side_effect=success) as proc:
            result=capture_storage({'tables':['raw']},'sl_test',container='demo',output_dir=Path(td))
            self.assertEqual(result['live_bytes'],7321)
            self.assertEqual(result['status'],'MEASURED_NODETOOL_LIVE_BYTES')
            self.assertEqual(len(result['commands']),2)
            self.assertTrue(Path(result['diagnostic_path']).exists())
            self.assertIn('Space used (live): 7,321', (Path(td)/'nodetool_sl_test_raw.txt').read_text())
            self.assertEqual(proc.call_args_list[1].args[0],['docker','exec','demo','nodetool','tablestats','sl_test.raw'])

    def test_flush_failure_preserves_stderr(self):
        with tempfile.TemporaryDirectory() as td, patch(
                'methodology.tpcx_iot.stage91.experiment.subprocess.run',return_value=
                SimpleNamespace(returncode=1,stdout='',stderr='Cannot contact JMX')):
            result=capture_storage({'tables':['raw']},'sl_test',container='demo',output_dir=Path(td))
            self.assertEqual(result['status'],'FLUSH_FAILED')
            self.assertIsNone(result['live_bytes'])
            self.assertIn('Cannot contact JMX',result['error'])
            saved=json.loads(Path(result['diagnostic_path']).read_text())
            self.assertIn('Cannot contact JMX',saved['commands'][0]['stderr_tail'])

    def test_flush_failure_shows_stdout_when_stderr_empty(self):
        with tempfile.TemporaryDirectory() as td, patch(
                'methodology.tpcx_iot.stage91.experiment.subprocess.run',return_value=
                SimpleNamespace(returncode=1,stdout='nodetool: Unknown keyspace: sl_test',stderr='')):
            result=capture_storage({'tables':['raw']},'sl_test',container='demo',output_dir=Path(td))
            self.assertEqual(result['status'],'FLUSH_FAILED')
            self.assertIn('Unknown keyspace',result['error'])

    def test_table_failure_preserves_table_and_raw_error(self):
        with tempfile.TemporaryDirectory() as td, patch(
                'methodology.tpcx_iot.stage91.experiment.subprocess.run',return_value=
                SimpleNamespace(returncode=3,stdout='',stderr='Unknown table: raw')):
            result=capture_storage({'tables':['raw']},'sl_test',container='demo',output_dir=Path(td),flush=False)
            self.assertEqual(result['status'],'TABLESTATS_FAILED')
            self.assertEqual(result['table'],'raw')
            self.assertIn('Unknown table',Path(td,'nodetool_sl_test_raw.txt').read_text())

    def test_unparseable_is_not_zero_and_saves_output(self):
        with tempfile.TemporaryDirectory() as td, patch(
                'methodology.tpcx_iot.stage91.experiment.subprocess.run',return_value=
                SimpleNamespace(returncode=0,stdout='Table: raw\nNo metric available\n',stderr='')):
            result=capture_storage({'tables':['raw']},'sl_test',container='demo',output_dir=Path(td),flush=False)
            self.assertEqual(result['status'],'TABLESTATS_UNPARSEABLE')
            self.assertIsNone(result['live_bytes'])
            self.assertIn('No metric available',Path(td,'nodetool_sl_test_raw.txt').read_text())

    def test_missing_docker_is_captured_as_oserror(self):
        with tempfile.TemporaryDirectory() as td, patch(
                'methodology.tpcx_iot.stage91.experiment.subprocess.run',side_effect=FileNotFoundError('docker binary missing')):
            result=capture_storage({'tables':['raw']},'sl_test',container='demo',output_dir=Path(td),flush=False)
            self.assertEqual(result['status'],'TABLESTATS_FAILED')
            self.assertIn('docker binary missing',result['error'])
            self.assertEqual(result['commands'][0]['exception'],'FileNotFoundError')

    def test_diagnostic_cli_reports_status_without_reset(self):
        with tempfile.TemporaryDirectory() as td, patch(
                'methodology.tpcx_iot.stage91.experiment.subprocess.run',return_value=
                SimpleNamespace(returncode=0,stdout='Space used (live): 800\n',stderr='')):
            code=diagnostic_main(['--container','demo','--keyspace','sl_test',
                                  '--table','raw','--output',td])
            self.assertEqual(code,0)
            self.assertTrue(Path(td,'storage_diagnostic.json').exists())

if __name__=='__main__':
    unittest.main()
