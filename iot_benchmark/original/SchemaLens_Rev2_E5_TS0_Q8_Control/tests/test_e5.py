"""Offline E5 tests with a fake Cassandra session; no DB or stage outputs touched."""
import tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from e5_q8 import (SQL_FULL, SQL_LATEST, CountingSession, optimized_q8,
                   balanced_orders, percentile_nearest_rank, check_paths,
                   evaluate_plan)


def q8_result(rows, query):
    latest = {}
    for row in rows:
        if row['device_id'] not in query['devices'] or row['sensor_name'] not in query['sensors']:
            continue
        k = row['device_id'], row['sensor_name']
        if k not in latest or row['timestamp'] > latest[k]['timestamp']:
            latest[k] = row
    return [latest[k] for k in sorted(latest)]


class FakeCassandra:
    def __init__(self, values):
        # Values are (timestamp, value). None denotes an anomalous null-valued row.
        self.values = values
        self.calls = []

    def execute(self, statement, params):
        self.calls.append((statement, tuple(params)))
        d, s = params
        raw = sorted(self.values.get((d, s), []), reverse=True)
        if statement == SQL_LATEST:
            raw = raw[:1]
        elif statement != SQL_FULL:
            raise AssertionError('Unsafe SQL')
        return iter([SimpleNamespace(timestamp_ms=t, value_double=v) for t, v in raw])


def fake_full(counted, q, route=None, sensors=None):
    records = []
    for d in q['devices']:
        for s in q['sensors']:
            for row in counted.execute(SQL_FULL, (d, s)):
                records.append({'device_id':d, 'sensor_name':s,
                                'timestamp':row.timestamp_ms, 'value_double':float(row.value_double)})
    return q8_result(records, q)


class E5Tests(unittest.TestCase):
    def setUp(self):
        self.q = {'query_id':'Q8','devices':['d0'], 'sensors':['s0','s1'],
                  'value_lower':None, 'start_ms':0, 'end_ms_exclusive':10000}
        self.values = {('d0','s0'):[(10,0.5),(20,1.5),(30,2.5)],
                       ('d0','s1'):[(10,4.0),(25,9.0)]}
        self.pairs = {(d,s) for d in self.q['devices'] for s in self.q['sensors']}

    def test_dense_result_and_counts(self):
        mock = FakeCassandra(self.values)
        counted = CountingSession(mock,self.pairs)
        got, fallbacks = optimized_q8(counted,self.q,q8_result)
        self.assertEqual(got,[{'device_id':'d0','sensor_name':'s0','timestamp':30,'value_double':2.5},
                              {'device_id':'d0','sensor_name':'s1','timestamp':25,'value_double':9.0}])
        self.assertEqual((counted.execute_calls,counted.iterated_rows,fallbacks),(2,2,0))
        self.assertEqual(counted.sql_counts['LIMIT1'],2)

    def test_old_executor_semantics_reproduced(self):
        counted = CountingSession(FakeCassandra(self.values),self.pairs)
        got = fake_full(counted,self.q)
        self.assertEqual(got,q8_result([{'device_id':d,'sensor_name':s,'timestamp':t,'value_double':v}
                    for (d,s),items in self.values.items() for t,v in items],self.q))
        self.assertEqual((counted.execute_calls,counted.iterated_rows),(2,5))

    def test_sparse_timestamps_and_absent_sensor(self):
        data={('d0','s0'):[(10,2.),(40,3.)]}
        c=CountingSession(FakeCassandra(data),self.pairs)
        got,n=optimized_q8(c,self.q,q8_result)
        self.assertEqual(len(got),1)
        self.assertEqual((c.execute_calls,c.iterated_rows,n),(2,1,0))

    def test_sparse_all_missing(self):
        c=CountingSession(FakeCassandra({}),self.pairs)
        got,n=optimized_q8(c,self.q,q8_result)
        self.assertEqual(got,[])
        self.assertEqual((c.execute_calls,c.iterated_rows,n),(2,0,0))

    def test_unexpected_null_fallback(self):
        data={('d0','s0'):[(10,2.),(20,3.),(30,None)],('d0','s1'):[(5,7.)]}
        c=CountingSession(FakeCassandra(data),self.pairs)
        got,n=optimized_q8(c,self.q,q8_result)
        self.assertEqual(n,1)
        self.assertEqual((c.execute_calls,c.iterated_rows),(3,5))
        self.assertEqual([x['timestamp'] for x in got],[20,5])

    def test_multiple_devices_and_different_last_times(self):
        q={**self.q,'devices':['d0','d1']}
        pairs={(d,s) for d in q['devices'] for s in q['sensors']}
        data={**self.values,('d1','s0'):[(4,1.)],('d1','s1'):[(9,5.),(100,9.)]}
        c=CountingSession(FakeCassandra(data),pairs)
        got,n=optimized_q8(c,q,q8_result)
        self.assertEqual(len(got),4)
        self.assertEqual((c.execute_calls,c.iterated_rows,n),(4,4,0))

    def test_newest_timestamp_wins_not_value(self):
        c=CountingSession(FakeCassandra({('d0','s0'):[(1,900.),(2,-100.)]}),self.pairs)
        got,_=optimized_q8(c,self.q,q8_result)
        self.assertEqual(got[0]['timestamp'],2)
        self.assertEqual(got[0]['value_double'],-100.)

    def test_guard_rejects_sql_mutations(self):
        fake=FakeCassandra({})
        c=CountingSession(fake,self.pairs)
        for sql in ('DROP KEYSPACE production','SELECT * FROM ts0_iot_raw',
                    SQL_FULL+'; DELETE FROM x','UPDATE ts0_iot_raw SET x=1'):
            with self.subTest(sql=sql),self.assertRaises(ValueError):
                list(c.execute(sql,('d0','s0')))
        self.assertEqual(fake.calls,[])

    def test_guard_rejects_changed_scope(self):
        fake=FakeCassandra({})
        c=CountingSession(fake,self.pairs)
        with self.assertRaises(ValueError):
            list(c.execute(SQL_FULL,('production','s0')))
        with self.assertRaises(ValueError):
            list(c.execute(SQL_FULL,('d0',)))
        self.assertEqual(fake.calls,[])

    def test_balanced_orders_reproducible(self):
        a=balanced_orders(200,42)
        self.assertEqual((a.count('AB'),a.count('BA')),(100,100))
        self.assertEqual(a,balanced_orders(200,42))
        self.assertNotEqual(a,balanced_orders(200,43))
        for value in (0,1,3,-2):
            with self.assertRaises(ValueError):balanced_orders(value,42)

    def test_p95_nearest_rank_for_200_samples(self):
        self.assertEqual(percentile_nearest_rank(list(range(1,201)),95),190)
        self.assertEqual(percentile_nearest_rank(list(range(1,41)),95),38)

    def test_nested_outputs_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);exp=root/'exp';res=exp/'results';addon=root/'addon';out=root/'fresh'
            exp.mkdir();res.mkdir();addon.mkdir()
            (addon/'v4_rev2').mkdir()
            (addon/'v4_rev2/prebenchmark.py').write_text('')
            check_paths(exp,res,addon,out)
            with self.assertRaises(ValueError):check_paths(exp,res,addon,exp/'out')
            with self.assertRaises(ValueError):check_paths(exp,res,addon,res/'out')
            with self.assertRaises(ValueError):check_paths(exp,res,addon,addon/'out')
            out.mkdir()
            with self.assertRaises(ValueError):check_paths(exp,res,addon,out)

    def test_evaluate_plan_preflight_both_paths(self):
        exp=q8_result([{'device_id':d,'sensor_name':s,'timestamp':t,'value_double':v}
                 for (d,s),items in self.values.items() for t,v in items],self.q)
        for plan,nrows in (('A_FULL_SCAN',5),('B_DESC_LIMIT1',2)):
            with self.subTest(plan=plan):
                obs=evaluate_plan(plan,FakeCassandra(self.values),self.q,{},exp,
                                  q8_result,lambda a,b:a==b,fake_full)
                self.assertEqual(obs['oracle'],'PASS')
                self.assertEqual(obs['execute_calls'],2)
                self.assertEqual(obs['iterated_rows'],nrows)
                self.assertEqual(obs['fallback_count'],0)

    def test_bad_query_kind_rejected(self):
        c=CountingSession(FakeCassandra(self.values),self.pairs)
        with self.assertRaises(ValueError):optimized_q8(c,{**self.q,'query_id':'Q7'},q8_result)
        with self.assertRaises(ValueError):optimized_q8(c,{**self.q,'value_lower':5},q8_result)


if __name__ == '__main__':unittest.main()
