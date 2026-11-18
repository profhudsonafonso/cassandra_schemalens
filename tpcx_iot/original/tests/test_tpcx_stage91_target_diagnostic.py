"""Offline checks: target identity must be validated without changing a cluster."""
from __future__ import annotations
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from methodology.tpcx_iot.stage91.target_diagnostic import inspect_target

ID1 = 'cb4e5df9-6ae5-4c7a-867f-6f687cd196d2'
ID2 = 'bcb10b59-d9c1-40d0-88ac-532a4015fa5d'

class FakeSession:
    def execute(self, query):
        assert query == 'SELECT host_id, cluster_name, release_version FROM system.local'
        return SimpleNamespace(one=lambda: SimpleNamespace(host_id=ID1, cluster_name='C1', release_version='4.1.12'))

class TargetDiagnosticTests(unittest.TestCase):
    def fake_client(self):
        return SimpleNamespace(session=FakeSession())

    def test_target_match(self):
        with tempfile.TemporaryDirectory() as td, patch(
            'methodology.tpcx_iot.stage91.target_diagnostic.subprocess.run',
            return_value=SimpleNamespace(returncode=0, stdout=f' host_id\n {ID1}\n', stderr='')):
            out=Path(td)/'identity.json'
            r=inspect_target(self.fake_client(), 'schemalens_tpcx_cassandra', output_path=out)
            self.assertEqual(r['status'], 'MATCH')
            self.assertEqual(json.loads(out.read_text())['container_host_id'], ID1)

    def test_different_nodes_are_rejected(self):
        with tempfile.TemporaryDirectory() as td, patch(
            'methodology.tpcx_iot.stage91.target_diagnostic.subprocess.run',
            return_value=SimpleNamespace(returncode=0, stdout=f' {ID2}', stderr='')):
            r=inspect_target(self.fake_client(), 'schemalens_tpcx_cassandra', output_path=Path(td)/'id.json')
            self.assertEqual(r['status'], 'TARGET_MISMATCH')
            self.assertIn('different Cassandra nodes', r['message'])
            self.assertFalse(r['destructive_actions'])

    def test_docker_cqlsh_error_is_persisted(self):
        with tempfile.TemporaryDirectory() as td, patch(
            'methodology.tpcx_iot.stage91.target_diagnostic.subprocess.run',
            return_value=SimpleNamespace(returncode=1, stdout='', stderr='Unavailable')):
            out=Path(td)/'id.json'
            r=inspect_target(self.fake_client(), 'schemalens_tpcx_cassandra', output_path=out)
            self.assertEqual(r['status'], 'DIAGNOSTIC_FAILED')
            self.assertEqual(json.loads(out.read_text())['docker_stderr_tail'], 'Unavailable')

    def test_invalid_container_name_does_not_execute(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(ValueError):
                inspect_target(self.fake_client(), 'bad;command', output_path=Path(td)/'id.json')

if __name__ == '__main__':
    unittest.main()
