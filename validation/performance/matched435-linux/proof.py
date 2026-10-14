from pathlib import Path
import json
j=Path(__file__).resolve().parent;source=json.loads((j/'SOURCE.json').read_text());art=json.loads((j/'ARTIFACTS.json').read_text());baseline=j.parent/'matched434-linux';old=json.loads((baseline/'SOURCE.json').read_text())
for name in ['cargo.txt','rustc.txt','protoc.txt','tool-binaries.sha256','matched-release-profile.txt']:
 assert (j/'logs'/name).read_bytes()==(baseline/'logs'/name).read_bytes(),name
changed=sorted(p for p,h in source['sha256'].items() if old['sha256'].get(p)!=h);assert len(changed)==7 and {'src/raft/completion.rs','src/raft/completion_tests.rs','benchmarks/matched/src/voteboat_engine/actor/tests/completion.rs'} <= set(changed)
for p,h in old['sha256'].items():
 if p not in changed:assert source['sha256'][p]==h,p
rows=[]
for name in ['off','on','openraft']:
 v=json.loads((j/'logs'/(name+'.json')).read_text());rows.append({'case':name,'completed_originals':v['completed_originals'],'completed_requests':v['completed_requests'],'frontiers':v['applied_frontiers'],'PUTS_per_second':v['operations_per_second'],'p99_ms':v['request_p99_ns']/1e6,'scope':'functional control only; no paired/sustained capacity'})
proof={'commit':source['commit'],'source_inputs':source['inputs'],'changed_inputs':changed,'new_inputs':sorted(set(source['sha256'])-set(old['sha256'])),'unchanged_existing_inputs':sum(source['sha256'][p]==h for p,h in old['sha256'].items()),'source_inventory_difference':'4341430→4351433: additive scoped completion API/common transition, guarded matched actor consumption, three new completion test/implementation modules, component contract; dependencies/profile unchanged','original_build_handle':int((j/'BUILD_HANDLE').read_text()),'original_terminal_exit':0,'original_stage':'DONE','fresh_records':'four root/library/executable/pinned upstream records plus root buildscript fresh:false actualCargoJSON','profile':'unchanged434 opt3/debug2/assertions/overflow/thinLTO/codegen1/packed flagswrappersunset','within_host_compiler_cargo_protoc_profile_identical434':True,'release_gate':'actualPROFILErelease omits only referencecfg; ordinaryassertions/overflowon','artifacts':[{k:r[k] for k in ['target','original','copy','sha256']} for r in art],'symbols':json.loads((j/'SYMBOLS.json').read_text()),'controls':rows,'cli_exit':0,'capacity_campaign':'not authorized/run; waitrootquiet'}
(j/'PROOF.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(rows));print('exe',next(r['sha256'] for r in art if r['target']=='voteboat-matched-framework'))
