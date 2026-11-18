"""V3 semantic contracts and input provenance unit tests (no database, no Java runtime)."""
import json
import tempfile
import unittest
from pathlib import Path
from phase_a1.core.bridge import _result
from phase_a1.tools.official_query_capture import validate,representative_queries,OPERATIONS

class OfficialBridgeV3Tests(unittest.TestCase):
    DATA=[
      {'device_id':'device_0','sensor_name':'s0','timestamp':1000,'value_double':15.0},
      {'device_id':'device_0','sensor_name':'s1','timestamp':1000,'value_double':3.0},
      {'device_id':'device_0','sensor_name':'s0','timestamp':2000,'value_double':12.0},
      {'device_id':'device_0','sensor_name':'s1','timestamp':2000,'value_double':14.0},
      {'device_id':'device_1','sensor_name':'s0','timestamp':3000,'value_double':12.0},
      {'device_id':'device_1','sensor_name':'s1','timestamp':3000,'value_double':14.0},
    ]
    BASE={'query_id':'Q3','devices':['device_0'],'sensors':['s0','s1'],
      'start_ms':1000,'end_ms_exclusive':4000,'point_ms':1000,
      'value_lower':10.0,'aggregation':None,'group_by_ms':None,'set_op':None}

    def test_value_filter_uses_AND_on_wide_rows(self):
        got=_result(self.DATA,self.BASE)
        self.assertEqual([(r['sensor_name'],r['timestamp']) for r in got],[('s0',2000),('s1',2000)])

    def test_latest_is_global_and_ignores_query_range(self):
        q={**self.BASE,'query_id':'Q8','value_lower':None,'end_ms_exclusive':1001}
        got=_result(self.DATA,q)
        self.assertEqual({x['timestamp'] for x in got},{2000})

    def test_set_ops_use_only_projected_field_values(self):
        q={**self.BASE,'query_id':'Q12','devices':['device_0','device_1'],
           'value_lower':None,'set_op':'intersect','set_children':[
               {'devices':['device_0'],'sensors':['s0','s1'],'start_ms':1000,'end_ms_exclusive':4000},
               {'devices':['device_1'],'sensors':['s0','s1'],'start_ms':1000,'end_ms_exclusive':4000}]}
        self.assertEqual(_result(self.DATA,q),[(12.0,14.0)])
        self.assertEqual(_result(self.DATA,{**q,'set_op':'except'}),[(15.0,3.0)])
        self.assertEqual(_result(self.DATA,{**q,'set_op':'union'}),[(12.0,14.0),(15.0,3.0)])

    def test_date_bin_uses_epoch_alignment_not_query_start(self):
        q={**self.BASE,'query_id':'Q7','value_lower':None,'aggregation':'count',
           'group_by_ms':2000,'start_ms':1000,'end_ms_exclusive':4000}
        got=_result(self.DATA,q)
        self.assertEqual(sorted({r['window_start_ms'] for r in got}),[0,2000])

    def test_native_capture_validates_full_set_and_projected_operands(self):
        native={'deviceSchema':[{'device':'device_0','sensors':[{'name':'s0'},{'name':'s1'}]}],
            'startTimestamp':1000,'endTimestamp':4000,'timestamp':1000,'aggFun':'count',
            'valueThreshold':10.0,'granularity':2000}
        events=[]
        for operation,qid in OPERATIONS.items():
            params=native.copy()
            if qid=='Q12':
                params={'setOpType':'INTERSECT','childRangeQueries':[
                    {**native,'deviceSchema':[{'device':'device_0','sensors':native['deviceSchema'][0]['sensors']}]},
                    {**native,'deviceSchema':[{'device':'device_1','sensors':native['deviceSchema'][0]['sensors']}]}]}
            events.append({'origin':'iot_benchmark_generate_query_workload',
                           'operation':operation,'sample_index':0,'query':params})
        with tempfile.TemporaryDirectory() as t:
            path=Path(t)/'capture.jsonl'
            path.write_text('\n'.join(json.dumps(e) for e in events)+'\n')
            records, counts=validate(path,[f'Q{i}' for i in range(1,13)])
            self.assertEqual(len(records),12)
            self.assertEqual(sum(counts.values()),12)
            data={'devices':['device_0','device_1'], 'sensors':['s0','s1'],
                'min_timestamp':1000,'max_timestamp':3000}
            ops=[{'id':f'Q{i}','name':f'Q{i}','aggregate':i in {4,5,6,7,11}} for i in range(1,13)]
            sampled=representative_queries(records,data,{},ops,path)
            self.assertEqual(len(sampled),12)
            self.assertEqual(sampled[-1]['set_op'],'intersect')
            self.assertEqual(sampled[1]['end_ms_exclusive'],4001)
            self.assertEqual(sampled[-1]['set_children'][0]['end_ms_exclusive'],4001)
            self.assertEqual(len(sampled[-1]['set_children']),2)
            self.assertEqual(sampled[7]['parameter_origin'].startswith('sample from official'),True)
            # This is a fabricated test DTO; the API origin field is not cryptographic proof.
            events.pop()
            path.write_text('\n'.join(json.dumps(e) for e in events)+'\n')
            with self.assertRaisesRegex(ValueError,'Missing official query classes'):
                validate(path,[f'Q{i}' for i in range(1,13)])

    def test_no_silent_external_device(self):
        bad={'origin':'iot_benchmark_generate_query_workload','operation':'RANGE_QUERY',
             'query':{'deviceSchema':[{'device':'fake','sensors':[{'name':'s0'}]}],
                      'startTimestamp':0,'endTimestamp':1000}}
        with tempfile.TemporaryDirectory() as t:
            f=Path(t)/'x.jsonl';f.write_text(json.dumps(bad)+'\n')
            events,_=validate(f)
            data={'devices':['device_0'],'sensors':['s0'],'min_timestamp':0,'max_timestamp':1}
            with self.assertRaisesRegex(ValueError,'absent from imported native CSV'):
                representative_queries(events,data,{},[{'id':'Q2','name':'Q2','aggregate':False}],f)

if __name__=='__main__':unittest.main()
