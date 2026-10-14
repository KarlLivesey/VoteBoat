from pathlib import Path
import json
j=Path(__file__).resolve().parent;source=json.loads((j/'SOURCE.json').read_text());art=json.loads((j/'ARTIFACTS.json').read_text());baseline=j.parent/'matched430-linux';old=json.loads((baseline/'SOURCE.json').read_text())
for name in ['cargo.txt','rustc.txt','protoc.txt','tool-binaries.sha256','matched-release-profile.txt']:
 assert (j/'logs'/name).read_bytes()==(baseline/'logs'/name).read_bytes(),name
changed=sorted(p for p,h in source['sha256'].items() if old['sha256'].get(p)!=h);assert changed==['src/raft/membership_tests.rs','src/raft/owned_append_tests.rs','src/raft/receive.rs']
for p,h in old['sha256'].items():
 if p not in changed:assert source['sha256'][p]==h,p
rows=[]
for name in ['off','on','openraft']:
 v=json.loads((j/'logs'/(name+'.json')).read_text());rows.append({'case':name,'completed_originals':v['completed_originals'],'completed_requests':v['completed_requests'],'frontiers':v['applied_frontiers'],'PUTS_per_second':v['operations_per_second'],'p99_ms':v['request_p99_ns']/1e6,'scope':'functional control only; no paired/sustained capacity'})
proof={'commit':source['commit'],'source_inputs':source['inputs'],'changed_inputs':changed,'new_input':'src/raft/owned_append_tests.rs','unchanged_existing_inputs':len(old['sha256'])-2,'source_inventory_difference':'4301426→4311427 adds one owned Append test module; private receive implementation and module registration change, dependencies/profiles unchanged','original_build_handle':41231,'original_terminal_exit':0,'original_stage':'DONE','fresh_records':'four root/library/executable/pinned upstream records plus root buildscript fresh:false actualCargoJSON','profile':'unchanged430 opt3/debug2/assertions/overflow/thinLTO/codegen1/packed flagswrappersunset','within_host_compiler_cargo_protoc_profile_identical430':True,'release_gate':'actualPROFILErelease omits only referencecfg; ordinaryassertions/overflowon','artifacts':[{k:r[k] for k in ['target','original','copy','sha256']} for r in art],'symbols':json.loads((j/'SYMBOLS.json').read_text()),'controls':rows,'cli_exit':0,'capacity_campaign':'not authorized/run; waitrootquiet'}
(j/'PROOF.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(rows));print('exe',next(r['sha256'] for r in art if r['target']=='voteboat-matched-framework'))
