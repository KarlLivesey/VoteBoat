from pathlib import Path
import hashlib,json,re
root=Path.cwd();job=root/'target/p9/native486-capacity-bound/mac';raw=job/'collection/raw';h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();read=lambda p:json.loads(Path(p).read_text());collected=read(raw/'COLLECTED.json');ready=read(raw/'READINESS.json');source=read(raw/'SOURCE.json');qualified=read(job/'ROOT_FINAL_INPUTS.json')['files'];commit='dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69'
assert collected['qualification_status']==ready['exit']==0 and collected['qualification_stage']=='DONE' and ready['source_commit']==source['commit']==commit
assert set(collected['files'])=={p.relative_to(raw).as_posix()for p in raw.rglob('*')if p.is_file()and p.name!='COLLECTED.json'}
for n,v in collected['files'].items():
 p=raw/n;assert h(p)==v['sha256']and p.stat().st_size==v['bytes']and oct(p.stat().st_mode&0o7777)==v['mode'],n
assert len(source['sha256'])==len(qualified)==1496 and set(source['sha256'])==set(qualified)
for n,v in qualified.items():assert source['sha256'][n]==v['sha256']and source['modes'][n]==v['mode']
keys=[n for n in qualified if n.startswith('tests/fixtures/')and n.endswith('key.der')];assert len(keys)==7 and all(source['modes'][n]=='0o600'for n in keys)
expected={'application_all':23,'application_core':23,'client_all':28,'client_core':28,'unit_application_all':39,'unit_application_core':39,'provider_all':42,'provider_core':27,'counter_service':2,'matched_tests':70}
assert set(expected)<=set(ready['checks'])
for n,v in ready['checks'].items():
 assert v['exit']==0 and int((raw/'logs'/(n+'.exit')).read_text())==0,n
 if n in expected:assert [int(x)for x in re.findall(r'test result: ok\. (\d+) passed; 0 failed',(raw/'logs'/(n+'.stdout')).read_text())]==[expected[n]]
for n in ['strict_default','strict_all','strict_core','strict_native','matched_strict']:assert 'warning:'not in(raw/'logs'/(n+'.stderr')).read_text()
tap=(raw/'logs/provider_metadata_controls.stdout').read_text();assert '# tests 42' in tap and '# pass 42'in tap and '# fail 0'in tap
records=read(raw/'ARTIFACTS.json');rows=[json.loads(x)for x in(raw/'logs/native-release.jsonl').read_text().splitlines()if x.startswith('{')];assert len(records)==2
for r in records:
 c=r['compiler_record'];assert c in rows and c['fresh']is False and c['profile']['opt_level']=='3' and c['profile']['debuginfo']==0 and c['profile']['debug_assertions']and c['profile']['overflow_checks']and c['features']==['default','native','quic','tls'];assert r['sha256']==ready['artifact_sha256'][r['target']]
script=[x for x in rows if x.get('reason')=='compiler-artifact'and x.get('target',{}).get('name')=='build-script-build'and x.get('package_id','').endswith('#voteboat@0.1.0')];assert len(script)==1 and script[0]['fresh']is False
symbol=read(raw/'SYMBOLS.json');assert symbol['root_buildscript_fresh_record']==script[0]and 'voteboat_checkpoint_debug_oracle'not in symbol['root_buildscript_record']['cfgs'];assert symbol['uuid']==ready['native_uuid']and symbol['uuid']in(raw/'logs/macho-uuid.log').read_text()
for k,f in [('uuid_output_sha256','macho-uuid.log'),('macho_sections_sha256','macho-sections.log'),('symbol_table_sha256','symbol-table.log')]:assert h(raw/'logs'/f)==symbol[k]
lines=(raw/'logs/native-root-rustc-lines.txt').read_text().splitlines();assert len(lines)==2
for line in lines:
 for x in ['-C opt-level=3','-C debug-assertions=on']:assert x in line
 for x in ['-C lto=','-C codegen-units=','-C linker-plugin-lto','--cfg voteboat_checkpoint_debug_oracle','-C overflow-checks=off']:assert x not in line
baseline=root/'target/p9/native480-admission/mac/collection/raw/logs'
for n in ['tool-sha256.txt','rustc.txt','cargo.txt','protoc.txt']:assert(raw/'logs'/n).read_bytes()==(baseline/n).read_bytes()
controls=read(raw/'CONTROLS.json');assert controls['exit']==0 and controls['artifact_unchanged']and len(controls['cases'])==4
for case in controls['cases']:
 assert case['process']['exit']==case['validator']['exit']==0
 s=case['summary'];assert s['applied']=='512'and s['recovered_value']=='576'and s['retry_verified']=='true'and s['workers_joined']=='true';assert all(s[n]=='0'for n in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending']);assert case['ledger_status_counts']=={'applied':512}
 args=case['process']['command'];assert args[args.index('--ready-items')+1]=='64' and args[5]=='2'
assert ready['original_terminal']['actual_exit']==0 and ready['original_terminal']['original_handle']==60577
report={'exit':0,'scope':'Independent root read of all post-terminal original payload hashes/bytes/modes, actual check outputs/counts/exits,1496 qualifiedsource records, freshCargoJSON/vv root/exe/buildscript, symbol/tool identities and fourraw control records. No build, workload or suite was rerun; binaries remain onMac with actual guard hash proof. No performance claim.','commit':commit,'actual_original':ready['original_terminal'],'collection_originals':len(collected['files']),'source_files':1496,'source_directories':len(source['directory_modes']),'private_keys_0600':7,'actual_rust_test_counts':expected,'tap_tests':42,'all_checks_exit0':True,'fresh_native_root_executable_buildscript':True,'native_profile':'opt3/debug0/assertions+overflow/nativeDEFAULTLTO+codegen','native_uuid':symbol['uuid'],'artifact_sha256':ready['artifact_sha256'],'within_host480_tool_bytes_exact':True,'controls':{'count':4,'applied_each':512,'recovered_each':576,'all_refusals_unknown_pending':0,'quantum':64,'groups':2,'default_scope':'ProviderFileLogIo omittedpublication selection, explicitready64; no claimdefaultquantum16 was omitted onMac.'},'readiness_sha256':h(raw/'READINESS.json')}
(job/'INDEPENDENT_ROOT_REVIEW.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
