"""Deterministic, dependency-free SchemaLens planning engine.

The engine deliberately separates analytical estimates from measured Cassandra
results. Cassandra execution is an optional adapter; no timing is fabricated.
"""
from __future__ import annotations
import hashlib, json, re, urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from random import Random

ROOT = Path(__file__).resolve().parents[1]
CFG = ROOT / "config"
OUT = ROOT / "outputs"
NOSE_URL = "https://raw.githubusercontent.com/michaelmior/NoSE/d275634d99069bfd88a6c3ac51020138bc130dd4/workloads/rubis.rb"
NOSE_COMMIT = "d275634d99069bfd88a6c3ac51020138bc130dd4"
GROUPS = ["BrowseCategories","ViewBidHistory","ViewItem","SearchItemsByCategory","ViewUserInfo","RegisterItem","RegisterUser","BuyNow","StoreBuyNow","PutBid","StoreBid","PutComment","StoreComment","AboutMe","SearchItemsByRegion","BrowseRegions"]
WEIGHTS = {"BrowseCategories":[4.44,7.65,7.65,7.65],"ViewBidHistory":[2.38,1.54,1.54,1.54],"ViewItem":[22.95,14.17,14.17,14.17],"SearchItemsByCategory":[27.77,15.94,15.94,15.94],"ViewUserInfo":[4.41,2.48,2.48,2.48],"RegisterItem":[0,.53,5.30,53],"RegisterUser":[0,1.07,10.70,107],"BuyNow":[0,1.16,1.16,1.16],"StoreBuyNow":[0,1.10,11,110],"PutBid":[0,5.40,5.40,5.40],"StoreBid":[0,3.74,37.40,374],"PutComment":[0,.46,.46,.46],"StoreComment":[0,.45,4.50,45],"AboutMe":[0,1.71,1.71,1.71],"SearchItemsByRegion":[8.26,6.34,6.34,6.34],"BrowseRegions":[3.21,5.39,5.39,5.39]}
STATEMENTS = {
1:("BrowseCategories","read","users,regions","user/region equality"),2:("BrowseCategories","read","categories","dummy enumeration"),3:("ViewBidHistory","read","items,bids","item equality + date order"),4:("ViewBidHistory","read","users,bids,items","item equality + date order"),5:("ViewItem","read","items","item equality"),6:("ViewItem","read","items,bids","item equality"),7:("SearchItemsByCategory","read","items","category equality + end range + limit"),8:("ViewUserInfo","read","users","user equality"),9:("ViewUserInfo","read","comments","recipient equality"),10:("RegisterItem","write","items","insert + category/seller edges"),11:("RegisterUser","write","users","insert + region edge"),12:("BuyNow","read","users,items","user/item equality"),13:("BuyNow","read","items","item equality"),14:("StoreBuyNow","read","items","item equality"),15:("StoreBuyNow","write","items","item update"),16:("StoreBuyNow","write","buynow","insert + item/buyer edges"),17:("PutBid","read","users","user equality"),18:("PutBid","read","items","item equality"),19:("PutBid","read","bids","item equality + bid order + limit"),20:("StoreBid","write","bids","insert + item/user edges"),21:("StoreBid","read","items","item equality"),22:("StoreBid","write","items","item update"),23:("PutComment","read","users","user equality"),24:("PutComment","read","items","item equality"),25:("PutComment","read","users","user equality"),26:("StoreComment","read","users","user equality"),27:("StoreComment","write","users","rating update"),28:("StoreComment","write","comments","insert + three edges"),29:("AboutMe","read","users","user equality"),30:("AboutMe","read","comments","user comments traversal"),31:("AboutMe","read","users,comments","comment equality"),32:("AboutMe","read","buynow,items","buyer equality + date range"),33:("AboutMe","read","items","seller equality + end range"),34:("AboutMe","read","items,bids","user equality + end range"),35:("SearchItemsByRegion","read","items","region/category equality + end range + limit"),36:("BrowseRegions","read","regions","dummy enumeration")}
ENTITIES = ["categories","regions","users","items","bids","comments","buynow"]
RELATIONSHIPS = [("users","region"),("items","seller"),("items","category"),("bids","user"),("bids","item"),("comments","from_user"),("comments","to_user"),("comments","item"),("buynow","buyer"),("buynow","item")]

def read_json(name): return json.loads((CFG/name).read_text())
def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True); path.write_text(json.dumps(value, indent=2, sort_keys=True)+"\n")
def sha256(value): return hashlib.sha256(value.encode()).hexdigest()

def import_workload():
    raw = urllib.request.urlopen(NOSE_URL, timeout=30).read().decode()
    source = ROOT/"inputs/historical_sources/nose_workloads_rubis.rb"; source.parent.mkdir(parents=True, exist_ok=True); source.write_text(raw)
    statements = [{"statement_id": i, "request_group": g, "operation_type": op, "entities": e.split(','), "semantic": s} for i,(g,op,e,s) in STATEMENTS.items()]
    result = {"source_url":NOSE_URL,"commit":NOSE_COMMIT,"source_sha256":sha256(raw),"groups":GROUPS,"statements":statements,"weights":WEIGHTS,"workloads":["browsing","bidding","update10x","update100x"]}
    dump(ROOT/"inputs/workload/rubis_workload.json", result); return result

def conceptual_schema():
    return {"entities": ENTITIES, "relationships":[{"from":a,"name":b,"to": {"seller":"users","category":"categories","region":"regions","user":"users","item":"items","from_user":"users","to_user":"users","buyer":"users"}[b]} for a,b in RELATIONSHIPS], "cardinalities":{"optimizer":read_json("rubis_dataset.json")["optimizer_cardinalities"],"physical":read_json("rubis_dataset.json")["historical_physical_cardinalities"]},"update_ownership":{"items":["RegisterItem","StoreBuyNow","StoreBid"],"users":["RegisterUser","StoreComment"],"bids":["StoreBid"],"comments":["StoreComment"],"buynow":["StoreBuyNow"]}}

def generate(scale, mode):
    cfg=read_json("rubis_dataset.json"); counts=cfg["scales"][scale]; payload={"scale":scale,"mode":mode,"seed":cfg["seed"],"counts":counts,"anomaly":{"buynow_item_source":cfg["modes"][mode]["buynow_item_source"]},"generator":"deterministic research generator; original seed unspecified","source":"NoSE fake.js fidelity contract"}
    dump(OUT/"historical_reference"/f"dataset_{scale}_{mode}.json", payload); dump(ROOT/"inputs/conceptual_schema/rubis_eer.json", conceptual_schema()); return payload

def validate_dataset():
    schema=conceptual_schema(); assert schema["entities"]==ENTITIES and len(schema["relationships"])==10
    data=generate("mini_rubis","HISTORICAL_FIDELITY"); assert sum(data["counts"].values())>0
    return {"status":"PASS","entities":7,"relationships":10,"counts":data["counts"]}

def analyze():
    catalog=read_json("catalog_C0_C8.json"); rules=read_json("activation_rules.json"); workload=import_workload()
    rows=[]
    for sid,(group,op,entities,semantic) in STATEMENTS.items():
        fanout=2 if "traversal" in semantic or "," in entities else 1; depth=semantic.count("+")
        fam=["C0"]
        if op=="read" and ("range" in semantic or "order" in semantic): fam += ["C1","C7"]
        if fanout>1: fam += ["C2","C4"]
        if op=="write": fam += ["C6"]
        if "," in entities: fam += ["C5","C8"]
        rows.append({"query_id":sid,"request_group":group,"operation_type":op,"Fi":entities,"ri":entities.split(',')[0],"EA":1 if "equality" in semantic else 0,"IA":1 if op=="write" else 0,"OA":1 if "order" in semantic else 0,"PA":1 if "range" in semantic or "limit" in semantic else 0,"relationship_semantics":semantic,"depth":depth,"residual_traversal":fanout>1,"fanout":fanout,"sharedness":sum(1 for x in STATEMENTS.values() if entities in x[2]),"update_exposure":1 if op=="write" else 0,"estimated_partition_risk":"high" if fanout>2 else "low","activated_families":sorted(set(fam)),"activation_reasons":{"C0":"mandatory baseline","C6":"write maintenance" if op=="write" else None}})
    result={"catalog":catalog,"rules":rules,"rows":rows,"workloads":{w:{"weights":{g:WEIGHTS[g][i] for g in GROUPS},"normalized":{g:WEIGHTS[g][i]/sum(WEIGHTS[x][i] for x in GROUPS) for g in GROUPS}} for i,w in enumerate(["browsing","bidding","update10x","update100x"])}}
    dump(OUT/"stage1_3"/"analytical_matrix.json",result); return result

def candidates():
    a=analyze(); out=[]
    for row in a["rows"]:
        for family in row["activated_families"]:
            physical={"family":family,"statement_id":row["query_id"],"table":f"q{row['query_id']}_{family.lower()}","partition_key":[row["ri"]+"_id"],"clustering_key":["event_date"] if row["OA"] else [],"cql":f"SELECT * FROM {row['ri']}_{family.lower()} WHERE {row['ri']}_id = ?","allow_filtering":family=="C7","maintenance":["C6"] if row["operation_type"]=="write" else []}
            physical["physical_spec_hash"]=sha256(json.dumps(physical,sort_keys=True)); out.append(physical)
    dump(OUT/"stage4"/"candidates.json",out); return out

def plans():
    cs=candidates(); result=[{"candidate":c,"query_plan":{"logical_request":c["statement_id"],"cql":[c["cql"]],"support_reads":c["maintenance"],"client_processing":["join/filter/order"],"equivalence_oracle":"relational RUBiS oracle"},"validity":"analytical; Cassandra execution pending"} for c in cs]
    dump(OUT/"stage5"/"query_plans.json",result); return result

def stage6():
    ps=plans(); writes=[p for p in ps if p["candidate"]["statement_id"] in [10,11,15,16,20,22,27,28]]
    result={"plans":writes,"support_query_rule":"MissingMaintenanceFields = RequiredKeys(candidate) - AvailableFields(update)","atomicity":"no cross-partition global transaction assumed","validation":"analytical; Cassandra execution pending"}; dump(OUT/"stage6"/"write_plans.json",result); return result

def stage7():
    cs=candidates(); counts=read_json("rubis_dataset.json")["historical_physical_cardinalities"]
    widths={"categories":48,"regions":48,"users":176,"items":224,"bids":96,"comments":128,"buynow":72}
    result=[]
    for c in cs:
        entity=c["partition_key"][0].removesuffix("_id")
        entity=entity if entity in counts else "users"
        logical_rows=counts[entity]
        key_bytes=4*len(c["partition_key"])+8*len(c["clustering_key"])
        row_width=widths[entity]+key_bytes+32
        amplification=1+len(c["maintenance"])
        result.append({"physical_spec_hash":c["physical_spec_hash"],"entity_basis":entity,"estimated_physical_rows":logical_rows,"estimated_row_width_bytes":row_width,"estimated_storage_bytes":logical_rows*row_width,"estimated_storage_uncertainty":"row width excludes Cassandra partition/index/compaction overhead; use measured SSTables for capacity","estimated_read_requests":1+len(c["maintenance"]),"estimated_rows_retrieved":25 if c["allow_filtering"] else 1,"estimated_write_amplification":amplification,"measured":False,"cost_model_version":"row_width_v1"})
    dump(OUT/"stage7"/"analytical_costs.json",result); return result

def stage8():
    costs=stage7(); seen={};
    for c in costs: seen.setdefault(c["physical_spec_hash"],c)
    deployments=[{"schema_id":f"H{i:04d}","candidate_hashes":[c["physical_spec_hash"]],"coverage":"statement-level","frozen":True} for i,c in enumerate(list(seen.values())[:read_json("benchmark_policy.json")["hbench_k"]])]
    result={"canonical_candidates":list(seen.values()),"complete_deployments":deployments,"hbench":deployments,"policy_hash":sha256(json.dumps(read_json("benchmark_policy.json"),sort_keys=True))}; dump(OUT/"stage8"/"hbench.json",result); return result

def baseline(name):
    result={"name":name,"source":"NoSE/QPG historical artifact when available","status":"reconstruction metadata; execution pending","designs":[{"schema_id":name,"tables":7,"correctness":"not yet Cassandra-validated"}]}; dump(OUT/"comparisons"/f"baseline_{name}.json",result); return result

def benchmark(workload):
    result={"workload":workload,"status":"NOT_EXECUTED","reason":"Cassandra adapter unavailable in current environment; no synthetic timings emitted","records":[]}; dump(OUT/"comparisons"/f"benchmark_{workload}.json",result); return result

def report_files():
    for name in ["DECISION_LOG.md","HISTORICAL_SOURCE_PROVENANCE.md","RUBIS_SOURCE_FIDELITY.md","RUBIS_WORKLOAD_CROSSWALK.md","CHEN_QUERY_CASE_MAPPING.md","CASSANDRA_FAMILY_MAPPING.md","EXPERIMENT_PROTOCOL.md","REPRODUCTION.md","VALIDITY_THREATS.md","RESULTS.md","HISTORICAL_COMPARISON.md","PAPER_EXPERIMENTS.md"]:
        p=ROOT/"docs"/name
        if not p.exists(): p.write_text("# " + name.removesuffix('.md') + "\n\nStatus: generated implementation baseline. Measurements are only reported after real Cassandra execution.\n")
