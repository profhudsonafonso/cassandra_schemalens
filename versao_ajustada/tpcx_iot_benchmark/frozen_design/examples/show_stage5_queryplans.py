"""Explain each Stage5 bound Cassandra query (no server required).

Run: python3 -m examples.show_stage5_queryplans
"""
from pathlib import Path
from methodology.tpcx_iot.stage4.candidates import ROOT,read_json
from methodology.tpcx_iot.stage5.runtime import ClosedWindow, bind_read_requests
from methodology.tpcx_iot.stage5.offline import event,probe


def main():
    data=read_json(ROOT/'outputs/tpcx_iot_stage5/20_queryplans.json')
    recent=ClosedWindow(59_000,64_000)
    historical=ClosedWindow(0,5_000)
    data_points=[event('ST01','temperature',0,20),event('ST02','temperature',60_000,30)]
    for plan in data:
        print('\n'+'='*75)
        print(plan['candidate_id'],f"[{plan['family']}]",plan['access_mode'])
        requests=bind_read_requests(plan,sensor_name='temperature',recent=recent,historical=historical,
                                    stations=['ST01','ST02'],complete_station_roster=True,
                                    roster_provenance='DEMO_FIXED_FIXTURE_ONLY')
        print('Total CQL requests:',len(requests))
        for r in requests[:3]:
            print('  ',r['window_id'],'station=',r['station_id'],'bucket=',r['bucket_start_ms'])
            print('   CQL:',r['cql'])
            print('   Values:',r['params'])
        if len(requests)>3:print('   ...',len(requests)-3,'additional requests')
        print('Coverage: offline conditional; Stage6 + live validation pending')
    print('\nNOTE: No Cassandra access or official TPC performance run occurred.')

if __name__=='__main__':main()
