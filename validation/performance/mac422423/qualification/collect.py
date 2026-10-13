import hashlib,json,re,shutil
from pathlib import Path
root=Path('/tmp/voteboat-mac422423-82bce5f')
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text())
for name,digest in source['sha256'].items():assert h(root/'source'/name)==digest,name
assert 'clippy.toml' in source['sha256']
artifacts=[]
for kind,target in [('matched','voteboat-matched-framework')]:
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
for name in ['library_all','library_core','model_contract_all','contract_core','matched_tests']:
 values=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;',(root/f'logs/{name}.log').read_text())
 assert values and all(int(f)==0 for _,f in values),name
 counts[name]=[int(n) for n,_ in values]
assert counts=={'library_all':[372],'library_core':[296],'model_contract_all':[17,29],'contract_core':[14],'matched_tests':[18]},counts
cli=[]
def check_report(name,engine,profile,batch,on=False):
 v=json.loads((root/f'logs/{name}.log').read_text());settings=v['settings']
 assert settings['attribution'] is on and settings['clients']==4096 and settings['batch']==batch and settings['operations']==settings['requested_operations']==16384 and settings['warmup']==1024
 assert v['engine']==engine and v['completed_originals']==16384 and v['completed_requests']==16384//batch and v['applied_frontiers']==[17409]*3
 assert v['durability']=='volatile_no_disk' and v['completion']=='every_original_locally_applied_after_quorum_commit'
 assert settings['maintenance']=='default_retention' and settings['envelopes']==profile.replace('-','_')
 requests=4096 if profile=='upstream-default' else 64;replication=1024 if profile=='upstream-default' else 64
 cap=requests*batch if engine=='voteboat' else requests
 assert [v[x] for x in ['api_entry_batch_capacity','append_entry_batch_capacity','replication_entry_capacity']]==[cap,cap,replication]
 contract=v['capacity_contract'];assert contract['api_merge_original_bound_for_fixed_request_batch']==requests*batch
 assert contract['api_capacity_unit']==('original_entries' if engine=='voteboat' else 'client_write_request_messages')
 if engine=='voteboat':
  envelope=contract['voteboat_empty_owned_envelope'];assert envelope['original_capacity']==cap and envelope['selected_log_batch_bytes']>=envelope['complete_empty_envelope_bytes']
  leader=v['diagnostics'][0];assert leader['proposed_originals']==17408
  assert sum(x['original_entries']*x['batches'] for x in leader['proposal_widths'])==17408
  assert leader['maximum_proposal_width']<=cap
 else:assert v['diagnostics'] is None and contract.get('voteboat_empty_owned_envelope') is None
 if not on:assert v.get('attribution') is None
 else:
  replicas=v['attribution'];assert len(replicas)==3 and {x['replica_slot'] for x in replicas}=={0,1,2}
  for replica in replicas:
   assert 'provider_maxima_whole_provider_lifetime_including_setup' in replica['scope'] and 'do_not_sum' in replica['nesting'] and 'not_pure_Core' in replica['residual']
   assert replica['clock']=='monotonic_elapsed_wall_intervals_not_cpu_or_critical_path'
   phases=replica['phases'];assert len(phases)==27 and len({p['phase'] for p in phases})==27
   lookup={p['phase']:p for p in phases}
   for phase in phases:
    assert phase['failures']==0 and all(isinstance(phase[k],int) and phase[k]>=0 for k in ['calls','total_elapsed_ns','maximum_elapsed_ns','items'])
    assert phase['maximum_elapsed_ns']<=phase['total_elapsed_ns']
    if phase['calls']==0:assert phase['total_elapsed_ns']==phase['maximum_elapsed_ns']==phase['items']==0
   assert lookup['actor_lifetime_inclusive']['calls']==1
   assert lookup['send']['calls']==lookup['send']['items']
   if replica['replica_slot']==0:
    for p in ['host_admission','core_direct','application','results']:assert lookup[p]['items']==17408
   scopes=replica['checkpoint_provider_scopes'];assert len(scopes)==4 and len({s['scope'] for s in scopes})==4
   for scope in scopes:
    methods=scope['methods'];assert len(methods)==13 and len({m['method'] for m in methods})==13
    for method in methods:
     assert method['failures']==0 and all(isinstance(method[k],int) and method[k]>=0 for k in ['calls','total_elapsed_ns','items'])
     phase=lookup[method['method']]
     assert all(method[k]<=phase[k] for k in ['calls','total_elapsed_ns','items'])
     if method['calls']==0:assert method['total_elapsed_ns']==method['items']==0
 return {'name':name,'engine':engine,'profile':profile,'batch':batch,'originals':16384,'requests':16384//batch,'frontiers':[17409]*3,'limits':[cap,cap,replication],'capacity_contract':contract,'attribution':on,'performance_claim':False}
for engine in ['voteboat','openraft']:
 for profile in ['upstream-default','common64']:
  for batch in [1,4]:cli.append(check_report(f'cli-{engine}-{profile}-b{batch}',engine,profile,batch))
cli.append(check_report('cli-attribution','voteboat','upstream-default',4,True))
(root/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
(root/'QUALIFICATION.json').write_text(json.dumps({'commit':source['commit'],'guarded_inputs':source['inputs'],'compiled_inputs':source['compiled_inputs'],'supplemental_inputs':source['supplemental_inputs'],'source_unchanged':True,'clippy_config_sha256':source['sha256']['clippy.toml'],'scope':'Frozen82bce5f Mac422/423 all/core library+contract and standalone/equivalent-envelope/checkpoint observer qualification; freshmatched artifact only, no native/timing/floorclaim','test_counts':counts,'cli_original_controls':cli,'artifacts':[{k:v for k,v in x.items() if k!='compiler_record'} for x in artifacts],'profiles':{'matched':'opt3/assertions/overflow/thinLTO/codegen1'},'fresh_root_library_and_executable_each':True},indent=2)+'\n')
print('all qualification/source/profile/original capacity/histogram/phase/artifact guards passed')
