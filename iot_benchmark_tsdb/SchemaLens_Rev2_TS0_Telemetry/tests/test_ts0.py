"""Pure tests for TS0 read-only telemetry guards and counter semantics."""
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from ts0_telemetry import (ReadOnlyCountingSession, choose_ts0, safe_output, summarize)

class FakeSession:
    def __init__(self, records):
        self.records = records
        self.calls = []
    def execute(self, stmt, params):
        self.calls.append((stmt, params))
        return iter(self.records)

SQL='SELECT timestamp_ms,value_double FROM ts0_iot_raw WHERE device_id=%s AND sensor_name=%s'
SQL_RANGE=SQL+' AND timestamp_ms >= %s AND timestamp_ms < %s'

class Tests(unittest.TestCase):
    def test_counts_calls_and_rows(self):
        s=FakeSession([1,2,3]); p=ReadOnlyCountingSession(s)
        self.assertEqual(list(p.execute(SQL,('d0','s0'))),[1,2,3])
        self.assertEqual(p.calls,1)
        self.assertEqual(p.iterated_rows,3)
    def test_partial_iterator_counts_consumed_not_fetched(self):
        s=FakeSession([1,2,3]);p=ReadOnlyCountingSession(s)
        it=iter(p.execute(SQL,('d0','s0')))
        self.assertEqual(next(it),1)
        self.assertEqual(p.iterated_rows,1)
    def test_multiple_queries_accumulate(self):
        s=FakeSession([1,2]);p=ReadOnlyCountingSession(s)
        list(p.execute(SQL,('d0','s0')))
        list(p.execute(SQL_RANGE,('d0','s1',0,100)))
        self.assertEqual((p.calls,p.iterated_rows),(2,4))
    def test_dangerous_statement_rejected_before_driver(self):
        s=FakeSession([]);p=ReadOnlyCountingSession(s)
        for stmt in ('DROP KEYSPACE x','DELETE FROM ts0_iot_raw WHERE device_id=%s',
                     'SELECT * FROM ts0_iot_raw','SELECT timestamp_ms,value_double FROM other_table',
                     SQL+'; DROP KEYSPACE test'):
            with self.subTest(stmt=stmt),self.assertRaises(ValueError):p.execute(stmt,('d0','s0'))
        self.assertFalse(s.calls)
    def test_wrong_bind_count_rejected(self):
        s=FakeSession([]);p=ReadOnlyCountingSession(s)
        with self.assertRaises(ValueError):p.execute(SQL_RANGE,('d0','s0'))
    def test_choose_frozen_ts0(self):
        h={'canonical_source':'ts0_iot_raw','deployment_id':'R2H_123456789abc',
           'candidates':['ts0_iot_raw'],
           'query_routes':{f'Q{i}':{'family':'TS0','candidate_id':'ts0_iot_raw','route_status':'DIRECT'} for i in range(1,13)}}
        m={'selected_ids':['R2H_123456789abc'],'keyspaces':{
            'R2H_123456789abc':'sl_conf_r2_ffffffff_r2h_123456789abc'}}
        self.assertEqual(choose_ts0([h],m)[1],m['keyspaces']['R2H_123456789abc'])
    def test_unknown_keyspace_rejected(self):
        h={'canonical_source':'ts0_iot_raw','deployment_id':'R2H_123456789abc',
           'candidates':['ts0_iot_raw'],
           'query_routes':{f'Q{i}':{'family':'TS0','candidate_id':'ts0_iot_raw','route_status':'DIRECT'} for i in range(1,13)}}
        m={'selected_ids':['R2H_123456789abc'],'keyspaces':{'R2H_123456789abc':'production'}}
        with self.assertRaises(ValueError):choose_ts0([h],m)
    def test_summary_detects_stage7_ts0_q8_underestimate(self):
        pred={'request_count_proxy':2,'physical_rows_proxy':2}
        obs=[{'query_id':'Q8','deployment_id':'R2H_abc','execute_calls':2,'iterated_rows':3000}]*3
        r=summarize(pred,obs)
        self.assertEqual(r['status'],'MISMATCH')
        self.assertEqual(r['delta_rows'],2998)
    def test_summary_agrees(self):
        pred={'request_count_proxy':2,'physical_rows_proxy':1500}
        obs=[{'query_id':'Q5','deployment_id':'R2H_abc','execute_calls':2,'iterated_rows':1500}]*3
        self.assertEqual(summarize(pred,obs)['status'],'MATCH')
    def test_summary_flags_variability(self):
        pred={'request_count_proxy':2,'physical_rows_proxy':1500}
        obs=[{'query_id':'Q5','deployment_id':'R2H_abc','execute_calls':2,'iterated_rows':1500},
             {'query_id':'Q5','deployment_id':'R2H_abc','execute_calls':2,'iterated_rows':1499}]
        self.assertEqual(summarize(pred,obs)['status'],'VARIABLE_OBSERVED_COUNTS')
    def test_output_must_be_fresh_and_external(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);exp=root/'exp';results=exp/'results';addon=root/'addon'
            exp.mkdir();results.mkdir();addon.mkdir()
            with self.assertRaises(ValueError):safe_output(exp/'analysis',exp,results,addon)
            with self.assertRaises(ValueError):safe_output(results/'audit',exp,results,addon)
            with self.assertRaises(ValueError):safe_output(addon/'audit',exp,results,addon)
            path=root/'fresh';self.assertEqual(safe_output(path,exp,results,addon),path)
            path.mkdir()
            with self.assertRaises(ValueError):safe_output(path,exp,results,addon)

if __name__=='__main__':unittest.main()
