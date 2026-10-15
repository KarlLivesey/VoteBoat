from pathlib import Path
import hashlib,json,subprocess
root=Path.cwd();job=root/'target/p9/native484-ingress/linux';h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();read=lambda p:json.loads(Path(p).read_text());ready=read(job/'READY.json');source=read(job/'SOURCE.json');qualified=read(root/'target/p9/native483-ingress/final/INPUTS.json')['files'];commit='efd383dc36b4b9edf8f66021b90115824440b468'
assert ready['source_commit']==source['commit']==commit and len(source['sha256'])==1503 and len(qualified)==1502
for n,digest in ready['proof_sha256'].items():assert h(job/n)==digest,n
for n,digest in source['sha256'].items():
 p=job/'source'/n;assert h(p)==digest and oct(p.stat().st_mode&0o777)==source['physical_modes'][n],n
for n,v in qualified.items():assert source['sha256'][n]==v['sha256'] and source['physical_modes'][n]==v['mode'],n
assert set(source['sha256'])-set(qualified)=={'validation/performance/native483-ingress/SCHEMA.md'}
for n in ['EXPORT_TERMINAL.json','BUILD_TERMINAL.json','controls/TERMINAL.json']:
 d=read(job/n);assert d.get('actual_exit',d.get('exit'))==0,(n,d)
rows=[json.loads(x)for x in (job/'logs/native-release.jsonl').read_text().splitlines() if x.startswith('{')];records=read(job/'ARTIFACTS.json');assert len(records)==2
for r in records:
 c=r['compiler_record'];assert c['fresh'] is False and c['profile']['opt_level']=='3' and c['profile']['debuginfo']==0 and c['profile']['debug_assertions'] and c['profile']['overflow_checks'] and c['features']==['default','native','quic','tls']
 assert h(r['original'])==h(r['copy'])==r['sha256'];assert c in rows
 assert Path(c['manifest_path'])==job/'source/Cargo.toml'
script=[r for r in rows if r.get('reason')=='compiler-artifact'and r.get('target',{}).get('name')=='build-script-build'and r.get('package_id','').endswith('#voteboat@0.1.0')];assert len(script)==1 and script[0]['fresh'] is False
executed=[r for r in rows if r.get('reason')=='build-script-executed'and r.get('package_id','').endswith('#voteboat@0.1.0')];assert len(executed)==1 and 'voteboat_checkpoint_debug_oracle'not in executed[0]['cfgs']
lines=(job/'logs/native-root-rustc-lines.txt').read_text().splitlines();assert len(lines)==2
for line in lines:
 for x in ['-C opt-level=3','-C debug-assertions=on']:assert x in line
 for x in ['-C lto=','-C codegen-units=','-C linker-plugin-lto','--cfg voteboat_checkpoint_debug_oracle','-C overflow-checks=off']:assert x not in line
assert (job/'logs/tool-binaries.sha256').read_bytes()==(root/'target/p9/native480-admission/linux/logs/tool-binaries.sha256').read_bytes()
assert h(root/'target/p9/native480-admission/linux/artifacts/native/native_benchmark')==ready['baseline480_artifact_sha256']=='8d3530a6e801ebb857d8dff3fc1a0fb4ef5af865c3aed89d1450271f2054b84d'
assert read(job/'controls/BEFORE.json')==read(job/'controls/AFTER.json')
orig=read(job/'ORIGINALS.json');assert all(x['actual_exit']==0 for x in orig['originals'].values())
for case in orig['controls']:
 assert case['applied']=='512'and case['recovered_value']=='576'and case['retry_verified']=='true'and case['workers_joined']=='true'and case['checkpoints']=='1'and case['reclaims']=='3'and case['capacity_readbacks']==6
 assert case['publication_readbacks']==(6 if case['selection']=='witness'else 0)
for p in sorted((job/'controls').glob('*.exit.json')):assert read(p)['exit']==0,p
report={'exit':0,'scope':'Independent root read of actual CargoJSON/vv/source/byte/mode/compiler/baseline/control/original records. No build/workload/validator was repeated; no speed/capacity claim.','commit':commit,'export_files':1503,'qualified_files':1502,'extra_committed_source':'validation/performance/native483-ingress/SCHEMA.md','all_ready_bound_proofs_exact':len(ready['proof_sha256']),'fresh_library_executable_buildscript':True,'native_profile':'opt3/debug0/assertions+overflow/native-defaultLTO+codegen; actual JSON plusvv checked','baseline480_tool_bytes_exact':True,'originals':orig['originals'],'controls':orig['controls'],'ready_sha256':h(job/'READY.json'),'artifact_hashes':{r['target']:r['sha256']for r in records}}
(job/'INDEPENDENT_ROOT_REVIEW.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
