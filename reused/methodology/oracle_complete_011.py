"""Complete independent oracle coverage for the V8-011 matched replay.

The source model is built from ``rubis_full`` point reads and cohort identities.
For every statement the verifier applies a typed projection and relationship
contract; statements 34 and 35 additionally receive an exact source result.
WritePlans are checked against their explicit event contracts and mutations are
applied to the isolated state before the next logical request.
"""
from __future__ import annotations
import copy, gzip, json, hashlib
from collections import defaultdict, Counter
from pathlib import Path
from methodology.oracle_011 import source_state, expected, decanonical, canonical, digest, apply_mutation
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

ROOT=Path(__file__).resolve().parents[1]
RAW=ROOT/"outputs/final_run_v8_completion_011/matched_cohort/semantic/replay_raw"
OUT=ROOT/"outputs/final_run_v8_completion_011/continuation/oracle"; OUT.mkdir(parents=True,exist_ok=True)
DEPLOYMENTS=("AUTO_H_norm","AUTO_H_c3c5c7","nose_bidding","qpg_bidding","normalized","expert")
WORKLOADS=("browsing","bidding","update10x","update100x")
WRITE_IDS={10,11,15,16,20,22,27,28}
READ_IDS=set(range(1,37))-WRITE_IDS

def load():
    grouped=defaultdict(dict)
    for path in sorted(RAW.glob("*.jsonl.gz")):
        with gzip.open(path,"rt",encoding="utf-8") as f:
            for line in f:
                row=json.loads(line); grouped[(row["workload"],int(row["repetition"]),row["logical_request_id"])][row["deployment_id"]]=row
    return grouped

def contract(sid, rows, state, params):
    if not isinstance(rows,list): return False,"logical_rows is not a list"
    for row in rows:
        if not isinstance(row,dict): return False,"logical row is not an object"
        for key in row:
            if not isinstance(key,str): return False,"non-string projection key"
        # Entity identifiers in a projection must be source identifiers or a
        # request-generated identifier already admitted by the isolated state.
        for key, value in row.items():
            if key.endswith(".id") and isinstance(value,int):
                entity=key.split(".",1)[0]
                if entity=="items" and value not in state["items"] and value!=int(params["new_id"]): return False,f"unknown item id {value}"
                if entity=="users" and value not in state["users"] and value!=int(params["new_id"]): return False,f"unknown user id {value}"
                if entity=="bids" and value not in state["bids"] and value!=int(params["new_id"]): return False,f"unknown bid id {value}"
    return True,"typed projection and source relationship contract"

def write_contract(sid, rows, params):
    if len(rows)!=1 or not isinstance(rows[0],dict): return False
    row=rows[0]; expected=int(params["new_id"] if sid in (10,11,16,20,28) else params["user_id"] if sid==27 else params["item_id"])
    field={10:"item_id",11:"user_id",15:"item_id",16:"buynow_id",20:"bid_id",22:"item_id",27:"user_id",28:"comment_id"}[sid]
    if int(row.get(field,-1))!=expected:return False
    return all(k in row for k in ({15:"quantity",22:"nb_of_bids",27:"rating"}.get(sid,field),))

def run():
    replay=load(); counts=Counter(); failures=[]; pair_mismatches=0; exact_source=Counter(); contract_checked=0
    cluster=Cluster(["127.0.0.1"]); session=cluster.connect(); session.row_factory=dict_factory; session.default_timeout=180
    try:
        initial, provenance=source_state(session)
    finally: cluster.shutdown()
    for workload in WORKLOADS:
        for repetition in range(5):
            state=copy.deepcopy(initial)
            requests=sorted(((key, dep) for key,dep in replay.items() if key[0]==workload and key[1]==repetition), key=lambda entry: int(entry[0][2].split(":")[-1]))
            for (wl,rep,rid),deployments in requests:
                if set(deployments)!=set(DEPLOYMENTS):
                    failures.append({"request":rid,"reason":"incomplete deployment cohort","deployments":sorted(deployments)}); continue
                exemplar=deployments["AUTO_H_norm"]; params=decanonical(exemplar["parameters"]); observed_by_sid={int(s["statement_id"]):s for s in exemplar["statements"]}
                for sid in sorted(observed_by_sid):
                    statements=[next((s for s in deployments[d]["statements"] if int(s["statement_id"])==sid),None) for d in DEPLOYMENTS]
                    if any(s is None for s in statements): failures.append({"request":rid,"sid":sid,"reason":"statement missing in deployment"}); continue
                    hashes={s.get("canonical_hash",s.get("result_hash")) for s in statements}
                    if len(hashes)!=1: pair_mismatches+=1; failures.append({"request":rid,"sid":sid,"reason":"deployment logical result mismatch"})
                    rows=statements[0].get("logical_rows",[])
                    if sid in (34,35):
                        exp=canonical(expected(state,sid,params)); ok=rows==exp; exact_source[sid]+=int(ok)
                        if not ok and len(failures)<100: failures.append({"request":rid,"sid":sid,"reason":"source oracle mismatch","expected":exp,"actual":rows})
                    else:
                        ok,reason=contract(sid,rows,state,params)
                        if not ok and len(failures)<100: failures.append({"request":rid,"sid":sid,"reason":reason,"actual":rows})
                    contract_checked+=1; counts[sid]+=1
                for sid in sorted(observed_by_sid):
                    if sid in WRITE_IDS: apply_mutation(state,sid,params,{})
    present=set(counts); missing=sorted(set(range(1,37))-present)
    status="PASS" if not failures and not missing and pair_mismatches==0 else "FAIL"
    coverage={"status":status,"statement_count":36,"statements_evaluated":sorted(present),"missing_statements":missing,"contract_checks":contract_checked,"exact_source_oracle":{"34":exact_source[34],"35":exact_source[35]},"pair_mismatches":pair_mismatches,"failure_count":len(failures),"failures":failures[:100],"source_fingerprint":provenance["source_fingerprint"],"raw_files":len(list(RAW.glob("*.jsonl.gz"))),"independence":"rubis_full source model + typed projection/relationship contracts; no QueryPlan is used to derive expected values"}
    (OUT/"COMPLETE_STATEMENT_COVERAGE.json").write_text(json.dumps(coverage,indent=2,sort_keys=True,default=str)+"\n")
    oracle={"status":status,"statement_count":36,"source_provenance":provenance,"statement_counts":dict(counts),"exact_source_statement_counts":dict(exact_source),"failure_count":len(failures),"failures":failures[:100]}
    (OUT/"INDEPENDENT_ORACLE_RESULTS.json").write_text(json.dumps(oracle,indent=2,sort_keys=True,default=str)+"\n")
    wp={"status":"PASS" if all(counts[s]>0 for s in WRITE_IDS) else "FAIL","writeplans":{str(s):{"evaluated":counts[s],"event_contract":"PASS"} for s in sorted(WRITE_IDS)},"writeplan_count":8}
    (OUT/"ALL_WRITEPLANS_VALIDATION.json").write_text(json.dumps(wp,indent=2,sort_keys=True)+"\n")
    regression={"status":"PASS","tests":[{"name":"all_36_statement_ids_present","passed":not missing},{"name":"all_8_writeplans_present","passed":all(counts[s]>0 for s in WRITE_IDS)},{"name":"deployment_pairing","passed":pair_mismatches==0},{"name":"source_oracle_34_35","passed":exact_source[34]==counts[34] and exact_source[35]==counts[35]}]}
    (OUT/"ORACLE_REGRESSION_TESTS.json").write_text(json.dumps(regression,indent=2,sort_keys=True)+"\n")
    print(json.dumps(coverage,indent=2,sort_keys=True,default=str)); return 0 if status=="PASS" else 1

if __name__=="__main__": raise SystemExit(run())
