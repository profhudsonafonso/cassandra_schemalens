"""Offline regression tests for the Q12 Fastjson shared-object serialization issue."""
import json
import tempfile
import unittest
from pathlib import Path
from phase_a1.tools.official_query_capture import validate, representative_queries

ROOT = Path(__file__).resolve().parents[1]
JAVA = ROOT / 'java_bridge/cn/edu/tsinghua/iot/benchmark/tools/SchemaLensQueryCapture.java'


def event(sensor2):
    return {'origin':'iot_benchmark_generate_query_workload','operation':'SET_OP_QUERY',
            'sample_index':0,'query':{'setOpType':'union','childRangeQueries':[
                {'deviceSchema':[{'device':'device_0','sensors':[{'name':'s0'},{'name':'s1'}]}],
                 'startTimestamp':1000,'endTimestamp':4000},
                {'deviceSchema':[{'device':'device_1','sensors':sensor2}],
                 'startTimestamp':1000,'endTimestamp':4000}]}}

class TestQ12ReferenceFix(unittest.TestCase):
    def save_and_validate(self, e):
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/'queries.jsonl'
            path.write_text(json.dumps(e)+'\n',encoding='utf-8')
            return validate(path)

    def test_shared_sensor_fastjson_ref_fails_explictly(self):
        with self.assertRaisesRegex(ValueError,r'Fastjson \$ref'):
            self.save_and_validate(event([{'$ref':'$..sensors[0]'}, {'$ref':'$..sensors[1]'}]))

    def test_shared_list_ref_fails_explicitly(self):
        e=event([{'name':'s0'}, {'name':'s1'}])
        e['query']['childRangeQueries'][1]['deviceSchema'][0]['sensors']={'$ref':'$[0].sensors'}
        with self.assertRaisesRegex(ValueError,'no complete sensors list'):
            self.save_and_validate(e)

    def test_complete_second_child_is_accepted_without_substitution(self):
        e=event([{'name':'s0'}, {'name':'s1'}])
        byq,counts=self.save_and_validate(e)
        self.assertEqual(counts,{'Q12':1})
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/'queries.jsonl';path.write_text(json.dumps(e)+'\n')
            r=representative_queries(byq,
                {'devices':['device_0','device_1'],'sensors':['s0','s1'],
                 'min_timestamp':1000,'max_timestamp':4000}, {},
                [{'id':'Q12','name':'SetOp','aggregate':False}],path)
            self.assertEqual(r[0]['set_children'][1]['sensors'],['s0','s1'])
            self.assertEqual(r[0]['end_ms_exclusive'],4001)

    def test_unknown_sensor_not_fabricated(self):
        e=event([{'name':'fake_s0'},{'name':'s1'}])
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/'queries.jsonl';path.write_text(json.dumps(e)+'\n')
            byq,_=validate(path)
            with self.assertRaisesRegex(ValueError,'unknown sensor: fake_s0'):
                representative_queries(byq,{'devices':['device_0','device_1'],
                    'sensors':['s0','s1'],'min_timestamp':1000,'max_timestamp':4000}, {},
                    [{'id':'Q12','name':'SetOp','aggregate':False}],path)

    def test_exporter_disables_ref_substitution_for_query_tree(self):
        source=JAVA.read_text()
        self.assertIn('SerializerFeature.DisableCircularReferenceDetect',source)
        self.assertIn('validateSensorObjects(fullQuery, op)',source)

if __name__=='__main__':unittest.main()
