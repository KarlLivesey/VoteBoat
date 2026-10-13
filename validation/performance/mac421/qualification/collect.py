import hashlib,json,re,shutil,csv
from pathlib import Path
root=Path('/tmp/voteboat-mac421-1091610')
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text())
for name,digest in source['sha256'].items():assert h(root/'source'/name)==digest,name
assert 'clippy.toml' in source['sha256']
artifacts=[]
for kind,target in [('native','native_benchmark')]:
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
for name in ['lib_ready_all','owner_runtime_log_ready_all','startup_member_maintenance_all','owner_runtime_ready_core','native_original_recovery','native_maintenance','native_cli_ready','matched_tests']:
 text=(root/f'logs/{name}.log').read_text();values=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;',text)
 assert values and all(int(f)==0 for _,f in values),name
 counts[name]=[int(n) for n,_ in values]
assert counts['lib_ready_all']==[17] and counts['native_cli_ready']==[3],counts
assert counts['native_original_recovery']==[2] and counts['native_maintenance']==[6]
assert counts['matched_tests']==[11]
functional=[]
for assembly in ['startup','shared']:
 for protocol in ['tcp','quic']:
  for quantum in [32,64]:
   name=f'functional-{assembly}-{protocol}-q{quantum}'
   text=(root/f'logs/{name}.log').read_text()
   summary=dict(re.findall(r'(\w+)=([^\s]+)',text))
   expected={'operations':'64','warmup':'64','recovered_value':'128','retry_verified':'true','workers_joined':'true','assembly':assembly,'groups':('1' if assembly=='startup' else '2'),'ready_visit_items_requested':str(quantum),'ready_visit_items_effective':str(quantum),'core_proposal_entries':'64','ready_visit_bytes':'1048576','ready_visit_ms':'2','ready_batch_capable':'true'}
   for key,value in expected.items():assert summary[key]==value,(name,key,summary)
   rows=sorted(csv.DictReader((root/name/'samples.csv').open()),key=lambda s:int(s['operation']))
   assert len(rows)==64 and len({s['operation'] for s in rows})==64,name
   for i,row in enumerate(rows):
    operation=65+i;groups=int(expected['groups']);value=(operation+groups-1)//groups;group=(operation-1)%groups+1
    assert int(row['operation'])==operation and int(row['value'])==value and int(row['group'])==group,(name,row)
    assert int(row['latency_ns'])==int(row['completed_ns'])-int(row['submitted_ns'])>=0
    assert int(row['applied_index'])==value+1,(name,row)
   functional.append({'name':name,'exact_originals':64,'settings':expected,'samples':str(root/name/'samples.csv'),'performance_claim':False})
(root/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
(root/'QUALIFICATION.json').write_text(json.dumps({'commit':source['commit'],'source_inputs':source['inputs'],'source_unchanged':True,'clippy_config_sha256':source['sha256']['clippy.toml'],'scope':'Frozen1091610 Mac421 boundedstartupready policy correctness/cleanup and native artifact only; no timedperformance or fairfloorclaim','test_counts':counts,'functional_original_controls':functional,'artifacts':[{k:v for k,v in x.items() if k!='compiler_record'} for x in artifacts],'profiles':{'native':'opt3/assertions/overflow/defaultLTO/defaultCodegen'},'fresh_root_library_and_executable_each':True},indent=2)+'\n')
print('all qualification/source/profile/original functional/artifact guards passed')
