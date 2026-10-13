import hashlib,json,re,shutil
from pathlib import Path
root=Path('/tmp/voteboat-mac419-7511e5e')
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text())
for name,digest in source['sha256'].items():assert h(root/'source'/name)==digest,name
assert 'clippy.toml' in source['sha256']
artifacts=[]
for kind,target in [('matched','voteboat-matched-framework'),('native','native_benchmark')]:
 events=[json.loads(s) for s in (root/f'logs/{kind}-release.jsonl').read_text().splitlines() if s.startswith('{')]
 for name in ['voteboat',target]:
  selected=[x for x in events if x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')==name]
  assert len(selected)==1,(kind,name,len(selected))
  record=selected[0];profile=record['profile']
  compiled_source=Path(record['target']['src_path'])
  relative=str(compiled_source.resolve().relative_to((root/'source').resolve()))
  assert relative in source['sha256'] and h(compiled_source)==source['sha256'][relative],(kind,name,relative)
  assert record['fresh'] is False and profile['opt_level']=='3' and profile['debug_assertions'] and profile['overflow_checks'],record
  original=Path(next(x for x in record['filenames'] if x.endswith('.rlib'))) if name=='voteboat' else Path(record['executable'])
  copy=root/'artifacts'/kind/original.name;copy.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(original,copy)
  digest=h(original);assert h(copy)==digest
  if name!= 'voteboat':copy.chmod(0o555)
  artifacts.append({'kind':kind,'target':name,'original':str(original),'copy':str(copy),'sha256':digest,'compiler_record':record})
 stderr=(root/f'logs/{kind}-release.stderr').read_text().splitlines()
 names=['voteboat',target.replace('-', '_')]
 lines=[s for s in stderr if any(f'--crate-name {n} ' in s for n in names)]
 assert len(lines)==2,(kind,len(lines))
 if kind=='native':assert all('-C lto=' not in s and '-C codegen-units=' not in s and '-C linker-plugin-lto' not in s for s in lines)
 else:assert all('-C codegen-units=1' in s for s in lines) and any('-C lto=thin' in s for s in lines)
 (root/f'logs/{kind}-root-rustc-lines.txt').write_text('\n'.join(lines)+'\n')
counts={}
for name in ['lib_incoming_all','lib_incoming_core','lib_log_all','lib_accepted_all','lib_raft_all','integration_all','lib_log_core','lib_raft_core','integration_core','native_original_recovery','native_maintenance','matched_tests']:
 text=(root/f'logs/{name}.log').read_text();values=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;',text)
 assert values and all(int(f)==0 for _,f in values),name
 counts[name]=[int(n) for n,_ in values]
assert counts['native_original_recovery']==[2] and counts['native_maintenance']==[6]
assert counts['matched_tests']==[11]
assert counts['lib_incoming_all']==[12] and counts['lib_incoming_core']==[9],counts
cli=[]
for profile in ['upstream-default','common64']:
 for engine in ['voteboat','openraft']:
  v=json.loads((root/f'logs/cli-{engine}-{profile}.json').read_text())
  assert v['settings']['attribution'] is False and v.get('attribution') is None
  assert v['engine']==engine and v['completed_originals']==16384 and v['completed_requests']==4096
  assert v['applied_frontiers']==[17409]*3 and v['durability']=='volatile_no_disk'
  limits=[v[x] for x in ['api_entry_batch_capacity','append_entry_batch_capacity','replication_entry_capacity']]
  assert limits==([4096,4096,1024] if profile=='upstream-default' else [64,64,64])
  cli.append({'engine':engine,'profile':profile,'originals':v['completed_originals'],'requests':v['completed_requests'],'frontiers':v['applied_frontiers'],'limits':limits})
(root/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
(root/'QUALIFICATION.json').write_text(json.dumps({'commit':source['commit'],'source_inputs':source['inputs'],'source_unchanged':True,'clippy_config_sha256':source['sha256']['clippy.toml'],'scope':'Focused frozen7511e5e Mac419 incoming/owned/codec-first plus417/418 compact/log/native/snapshot/Core qualification; no unrelated full Mac suite or timings','test_counts':counts,'cli_original_controls':cli,'artifacts':[{k:v for k,v in x.items() if k!='compiler_record'} for x in artifacts],'profiles':{'matched':'opt3/assertions/overflow/thinLTO/codegen1','native':'opt3/assertions/overflow/defaultLTO/defaultCodegen'},'fresh_root_library_and_executable_each':True},indent=2)+'\n')
print('all qualification/source/profile/original CLI/artifact guards passed')
