"""Read-only tests for the independent translated IoTDB-table reference."""
from __future__ import annotations
import csv
import tempfile
from pathlib import Path
import unittest
from iot_pipeline.reference_baseline import (columns_for, table_ddl, build_wide_records,
    _physical_to_raw, offline_checks,ensure_pilot_invariants)
from iot_pipeline.shared import inputs,read,read_rows,sha,path4,write,csv_write
from iot_pipeline.stage9 import confirm_frozen
from iot_pipeline.compare_baseline import summarize
from iot_pipeline.reference_baseline import LABEL


class IoTTableReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows=read_rows(inputs()['native'])
        cls.queries=read(inputs()['queries'])
        cls.sensors=sorted({r['sensor_name'] for r in cls.rows})

    def test_01_table_like_structure_is_not_ts0(self):
        ddl=table_ddl(self.sensors)
        self.assertIn('PRIMARY KEY ((device_id), timestamp_ms)',ddl)
        self.assertNotIn('sensor_name text',ddl)
        self.assertNotIn('bucket_day',ddl)
        self.assertIn('m_s0 double',ddl)
        self.assertIn('m_s1 double',ddl)

    def test_02_one_row_per_device_timestamp(self):
        wide=build_wide_records(self.rows,self.sensors)
        self.assertEqual(len(wide),18)
        self.assertEqual(sum(v is not None for rec in wide for v in rec[2:]),35)
        self.assertEqual(len(set((x[0],x[1]) for x in wide)),len(wide))

    def test_03_all_pilot_queries_equal_in_memory(self):
        ensure_pilot_invariants(self.rows,self.queries,self.sensors)
        _,tested=offline_checks(self.rows,self.queries,self.sensors)
        self.assertEqual(tested,12)

    def test_04_sparse_cell_and_q12(self):
        rows=[{'device_id':'d1','timestamp':1000,'sensor_name':'s0','value_double':2.0},
              {'device_id':'d1','timestamp':1000,'sensor_name':'s1','value_double':5.0},
              {'device_id':'d2','timestamp':1000,'sensor_name':'s0','value_double':3.0}]
        wide=build_wide_records(rows,['s0','s1'])
        self.assertEqual(len(wide),2)
        self.assertIsNone(next(x for x in wide if x[0]=='d2')[3])
        q=next(q for q in self.queries if q['query_id']=='Q12').copy()
        q['devices']=['d1','d2'];q['start_ms']=0;q['end_ms_exclusive']=2000
        self.assertEqual(len(_physical_to_raw(wide,['s0','s1'],q)),3)

    def test_05_reject_unsafe_sensors_and_duplicates(self):
        with self.assertRaises(ValueError):columns_for(['ok','x;DROP'])
        with self.assertRaises(ValueError):build_wide_records(self.rows+[self.rows[0]],self.sensors)
        with self.assertRaises(ValueError):ensure_pilot_invariants(self.rows,[{'query_id':'Q1'}],self.sensors)

    def test_06_does_not_modify_frozen_hbench(self):
        before=sha(path4(8,'28_hbench_frozen.json'))
        confirm_frozen()
        offline_checks(self.rows,self.queries,self.sensors)
        self.assertEqual(before,sha(path4(8,'28_hbench_frozen.json')))

    def test_07_comparator_rejects_wrong_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);h=root/'h';b=root/'b';h.mkdir();b.mkdir()
            write(b/'37_manifest.json',{'deployment_id':LABEL,'official_iot_benchmark_score':False,
                  'hbench_sha256':'incorrect','source_rows_sha256':sha(inputs()['native'])})
            write(h/'37_manifest.json',{'hbench_sha256':sha(path4(8,'28_hbench_frozen.json')),
                  'source_rows_sha256':sha(inputs()['native']),'official_iot_benchmark_score':False})
            with self.assertRaisesRegex(ValueError,'does not match'):
                summarize(h,b)

    def test_08_comparator_good_synthetic_matches(self):
        """Synthetic CSVs test arithmetic; NOT a manufactured Cassandra result."""
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);h=root/'h';b=root/'b';h.mkdir();b.mkdir()
            frozen=confirm_frozen();base_manifest={'deployment_id':LABEL,'official_iot_benchmark_score':False,
                  'hbench_sha256':sha(path4(8,'28_hbench_frozen.json')),
                  'source_rows_sha256':sha(inputs()['native'])}
            write(b/'37_manifest.json',base_manifest)
            write(h/'37_manifest.json',{'official_iot_benchmark_score':False,
                  'hbench_sha256':base_manifest['hbench_sha256'],
                  'source_rows_sha256':base_manifest['source_rows_sha256']})
            cols=['deployment_id','query_id','family','p95_ms','successful_operations','correctness']
            csv_write(b/'35_query_p95.csv',[
                {'deployment_id':LABEL,'query_id':f'Q{i}','family':'REFERENCE_TABLE',
                 'p95_ms':2,'successful_operations':20,'correctness':'PASS'}
                 for i in range(1,13)],cols)
            csv_write(h/'35_query_p95.csv',[
                {'deployment_id':item['deployment_id'],'query_id':f'Q{i}',
                 'family':item['query_routes'][f'Q{i}']['family'],
                 'p95_ms':1,'successful_operations':20,'correctness':'PASS'}
                for item in frozen for i in range(1,13)],cols)
            details,summ=summarize(h,b)
            self.assertEqual(len(details),12*len(frozen))
            self.assertEqual(len(summ),1+len(frozen))
            self.assertAlmostEqual(details[0]['relative_difference_pct'],-50)
            self.assertAlmostEqual(summ[1]['mean_query_p95_ms'],1)

if __name__=='__main__':unittest.main()
