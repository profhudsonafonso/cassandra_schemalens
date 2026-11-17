import json
import sys
import tempfile
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import collect_storage as C

EXAMPLE='''Total number of tables: 1
----------------
Keyspace : sl_conf_r2_15eadb0f_reference
\tRead Count: 12
\tTable: iot_reference_table
\tSSTable count: 2
\tSpace used (live):  13456
\tSpace used (total): 13777
\tMemtable data size:  42
\tNumber of partitions (estimate): 6
\tSSTable Compression Ratio: 0.24
'''

class TestStorage(unittest.TestCase):
    def test_parse_version_gapped(self):
        stats=C.parse_tablestats(EXAMPLE,'sl_conf_r2_15eadb0f_reference')
        self.assertEqual(len(stats),1)
        self.assertEqual(stats[0]['sstable_live_bytes'],13456)
        self.assertEqual(stats[0]['partition_count_estimate'],6)
        self.assertEqual(stats[0]['sstable_compression_ratio'],.24)
        self.assertIsNone(stats[0].get('snapshot_total_bytes'))
    def test_reject_unknown_keyspace(self):
        self.assertFalse(C.KS_PATTERN.fullmatch('system'))
        self.assertFalse(C.KS_PATTERN.fullmatch('production'))
        self.assertTrue(C.KS_PATTERN.fullmatch('sl_conf_r2_15eadb0f_r2h_6355f4e57096'))
    def test_missing_metric_not_zero(self):
        ks='sl_conf_r2_15eadb0f_reference'
        stats=C.summarize([{'keyspace':ks,'table':'foo','sstable_live_bytes':None}],
                          {'BASELINE_ORIGINAL':ks,'BASELINE_Q8_OPTIMIZED':ks},
                          [{'variant':'BASELINE_ORIGINAL','physical_write_operations':'9000','metric_cells':'18000'}])
        self.assertIsNone(stats[0]['sstable_live_bytes'])
        self.assertEqual(stats[0]['physical_write_operations'],9000)
        self.assertEqual(stats[1]['keyspace'],ks)
    def test_reject_output_inside_frozen(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)
            with self.assertRaises(ValueError):C.safe_output_dir(p/'experiment/out',p/'experiment',p/'results')
            with self.assertRaises(ValueError):C.safe_output_dir(p/'results/out',p/'experiment',p/'results')
    def test_check_frozen_hashes(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp);exp=p/'exp';results=p/'results';results.mkdir()
            stage=exp/'outputs/v4_rev2_prebenchmark/stage8';stage.mkdir(parents=True)
            frozen=stage/'28_hbench_v4_rev2_frozen.json';frozen.write_text(json.dumps([{'deployment_id':'R2H_aaaaaaaaaaaa'}]))
            fm=stage/'manifest.json';fm.write_text('{}')
            ks='sl_conf_r2_01234567_'
            man={'status':'CONFIRMATORY_CASSANDRA_COMPLETE','frozen_HBench_sha256':C.digest(frozen),
             'frozen_manifest_sha256':C.digest(fm),'selected_ids':['R2H_aaaaaaaaaaaa'],
             'keyspaces':{'BASELINE_ORIGINAL':ks+'reference','BASELINE_Q8_OPTIMIZED':ks+'reference','R2H_aaaaaaaaaaaa':ks+'r2h_aaaaaaaaaaaa'}}
            (results/'manifest.json').write_text(json.dumps(man))
            (results/'load.csv').write_text('variant,keyspace,physical_write_operations,metric_cells\nBASELINE_ORIGINAL,'+ks+'reference,3,6\nR2H_aaaaaaaaaaaa,'+ks+'r2h_aaaaaaaaaaaa,3,6\n')
            C.validate_inputs(exp,results)
            fm.write_text('{"changed":true}')
            with self.assertRaises(ValueError):C.validate_inputs(exp,results)

if __name__=='__main__':unittest.main()
