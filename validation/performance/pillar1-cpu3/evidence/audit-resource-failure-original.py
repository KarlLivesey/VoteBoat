import os
os.setpriority(os.PRIO_PROCESS,0,19);os.sched_setaffinity(0,{8,9})
import pathlib,json,hashlib,statistics,math,subprocess,time
from linux_runner import audit
D=pathlib.Path(__file__).resolve().parent;R=D/'raw';P=D.parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def read(n):return json.loads((R/n).read_text())
def save(n,x):(D/n).write_text(json.dumps(x,indent=2)+'\n')
receipt=read('collection-manifest.json')
for n,h in receipt['files'].items():assert sha(R/n)==h,n
summary=json.loads((D/'summary.json').read_text());assert summary['error'] is None and summary['stage1_pass'] and summary['driver_children']==18 and summary['canonical_children']==64
protocol=json.loads((D/'protocol.json').read_text());observations=[];driver=[];canonical=[]
required={'replicas','expected_count','reply_count','reply_ids','unreplied','commit_frontiers','applied_frontiers','reported_frontiers','nonfenced','propagated_error'}
binary={'driver':{'A':'c05c3c718ab6d9ac5a72f2e04cfce29fb35f6618a76b3e0debdda10ff7f5fa6a','B':read('build-diagnostic.after.json')['binary_sha256']},'canonical':{'A':'0c6998bc63614f43faaba36dfff64f9291b7a14336853f4d901c4068c2c69208','B':read('build-canonical.after.json')['binary_sha256']}}
expected=[]
for pair,order in enumerate(protocol['orders'],1):
 for spec in protocol['specs']:
  for v in order:expected.append(('driver',f'r{pair:02d}-w{spec.split(",")[1]}-{v}',v,spec))
for cell in protocol['canonical_schedule']:expected.append(('canonical',cell['id'],cell['label'],cell))
for scope,n,v,spec in expected:
 row=read(n+'.row.json');before=read(n+'.before.json');after=read(n+'.after.json');result=read(n+'.result.json');launch=read(n+'.launch.json');doc=json.loads((R/(n+'.stdout')).read_text());checks=read(n+'.checks.json')
 assert before==after,(n,'identity changed');assert before['binary_sha256']==binary[scope][v]
 if v=='B':assert before['source']['source_mode']==('driver' if scope=='driver' else 'pinned') and before['source']['files']==804 and not any(before['source'][k] for k in ['mismatches','extras','missing'])
 assert result['rc']==0 and result['wait_status']==0 and not result['timed_out'] and not result['competing'];assert not (R/(n+'.stderr')).read_bytes();assert all(checks.values())
 assert launch['argv']==result['argv'] and launch['argv'][0].endswith('/target/release/voteboat-matched-framework')
 assert launch['env']['CARGO_BUILD_JOBS']=='4' and launch['env']['CARGO_INCREMENTAL']=='0' and launch['env']['CARGO_PROFILE_RELEASE_DEBUG']=='0' and launch['env']['RUSTFLAGS']==''
 assert before['controller_nice']==19
 assert all(int(line.split()[2])==19 for sample in result['samples'] for line in sample.get('descendants',[]))
 assert all(not o['competing'] for o in launch['before']['quiet_observations'])
 assert launch['before']['hardware']['stdout']=='Mac14,5\n12\nApple M2 Max\n'
 completed=doc['completed_originals'];cpu=result['rusage']['ru_utime']+result['rusage']['ru_stime'];assert math.isclose(row['cpu_ns'],cpu*1e9/completed,rel_tol=1e-15)
 record={'scope':scope,'id':n,'label':v,'actual_completed':completed,'whole_child_user_s':result['rusage']['ru_utime'],'whole_child_sys_s':result['rusage']['ru_stime'],'cpu_ns_per_actual_completed_original':cpu*1e9/completed,'binary_sha256':before['binary_sha256'],'all_individual_checks':checks,'rusage':result['rusage'],'load_before':launch['before']['load'],'load_after':result['after']['load'],'time_before':launch['before']['time'],'time_after':result['after']['time'],'budget':launch['before']['budget'],'context_switches_are_descriptive_only':True}
 if scope=='driver':
  rounds,width,per=map(int,spec.split(','));frontier=(rounds+1024)*width+1
  assert completed==rounds*width and doc['replied_including_warmup']==doc['submitted_including_warmup']==(rounds+1024)*width
  assert doc['commit_frontiers']==doc['applied_frontiers']==doc['reported_frontiers']==[frontier]*3
  assert set(doc['checks'])==required and all(doc['checks'][key] is True for key in required)
  assert doc['unreplied']==0 and not doc['wrong_reply'] and doc['propagated_error'] is None and doc['fenced']==[False]*3 and not doc['failed_conjuncts']
  assert len(doc['diagnostics'])==3 and all(x['checkpoint_count']>0 and x['retained_entries']<=6000+width for x in doc['diagnostics'])
  record.update(width=width,rounds=rounds,actual_core_commit=doc['commit_frontiers'],actual_application=doc['applied_frontiers'],actual_published=doc['reported_frontiers'],actual_total_replied=doc['replied_including_warmup'],internal_elapsed_ns=doc['internal_elapsed_ns'],internal_rate=completed*1e9/doc['internal_elapsed_ns'],checkpoint_counts=[x['checkpoint_count'] for x in doc['diagnostics']],retained_entries=[x['retained_entries'] for x in doc['diagnostics']],request_p99='unavailable',ordinary_owned_shutdown='not exercised: original diagnostic process::exit')
  driver.append(record)
 else:
  assert completed==spec['completed'] and doc['completed_requests']==completed//spec['batch']
  assert doc['applied_frontiers']==[completed+1025]*3 and doc['completion']=='every_original_locally_applied_after_quorum_commit'
  assert doc['settings']['warmup']==1024 and doc['settings']['campaign_timeout_secs']==600 and not doc['settings']['attribution']
  assert doc['settings']['maintenance']=='default_retention' and doc['settings']['envelopes']=='upstream_default'
  assert len(doc['diagnostics'])==3 and all(x['checkpoint_count']>0 for x in doc['diagnostics'])
  assert doc['diagnostics'][0]['proposed_originals']==completed+1024
  assert math.isfinite(doc['operations_per_second']) and doc['operations_per_second']>0 and doc['request_p99_ns']>0
  record.update(cell=spec['cell'],pair=spec['round'],rate=doc['operations_per_second'],request_p99_ns=doc['request_p99_ns'],completed_requests=doc['completed_requests'],actual_applied_prefixes=doc['applied_frontiers'],exact_commit_counter='unavailable in canonical attributionfalse JSON; no fabricated values',normal_owned_shutdown='source cleanup before successful JSON/exit0',checkpoint_counts=[x['checkpoint_count'] for x in doc['diagnostics']])
  canonical.append(record)
 observations.append(record)
# Verify per-pair settings and build configuration equality, with only expected per-root target path difference.
for scope,rows in [('driver',driver),('canonical',canonical)]:
 for row in rows:
  n=row['id'];other=n[:-1]+('B' if row['label']=='A' else 'A')
  ea=read(n+'.launch.json')['env'];eb=read(other+'.launch.json')['env'];assert {k:v for k,v in ea.items() if k!='CARGO_TARGET_DIR'}=={k:v for k,v in eb.items() if k!='CARGO_TARGET_DIR'}
  assert read(n+'.before.json')['toolchain']==read(other+'.before.json')['toolchain']
  assert read(n+'.before.json')['cargo']==read(other+'.before.json')['cargo']
assert sha(R/'artifacts/candidate-diagnostic.bin')==binary['driver']['B'];assert sha(R/'artifacts/candidate-canonical.bin')==binary['canonical']['B']
assert sha(P/'cpu1-driver-r3/artifacts/diagnostic-A.bin')==binary['driver']['A'];assert sha(P/'cpu1-perf/artifacts/canonical-A.bin')==binary['canonical']['A']
linux=[];vanished=[];guard_records=[]
for f in sorted(D.glob('*.controller.json')):
 j=json.loads(f.read_text());tasks=[t for a in j['audit'] for t in a['tasks']];assert all(t['ok'] and t['nice']==19 and t['affinity']==[8,9] for t in tasks),f
 linux+=tasks;vanished+=j['vanished_process_observations']
 if '-guard.' in f.name:
  text=(D/f.name.replace('.json','.stdout')).read_text();assert 'mac_loop_gb=' in text and 'OK: disk within budget' in text and j['rc']==0
  guard_records.append({'file':f.name,'stdout_sha256':sha(D/f.name.replace('.json','.stdout'))})
cleanup=json.loads((D/'cleanup.controller.stdout').read_text());assert cleanup['parent_absent'] and not cleanup['live_owned_jobs'] and not cleanup['cwd_audit']['owned_root_references']
# Audit only readable current-user proc surfaces; unknown/foreign infrastructure is preserved.
proc_records=[];gaps=[];foreign=0
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  if p.stat().st_uid!=os.getuid():foreign+=1;continue
  cmd=(p/'cmdline').read_bytes().replace(b'\0',b' ').decode(errors='replace');cwd=os.readlink(p/'cwd');exe=os.readlink(p/'exe')
  proc_records.append({'pid':int(p.name),'exe':exe,'cwd':cwd,'task_named_root_cwd':'pillar1-cpu3-6c-20261023' in cwd,'deleted_cwd':' (deleted)' in cwd,'command':cmd[:1000]})
 except (OSError,ValueError) as ex:gaps.append({'pid':p.name,'kind':type(ex).__name__})
assert not any(x['task_named_root_cwd'] for x in proc_records)
current=audit(os.getpid());assert all(t['ok'] for t in current)
save('final-linux-process-audit.json',{'current_controller':current,'processes':proc_records,'foreign_uid_exclusions':foreign,'inaccessible_or_vanished':gaps,'known_owned_running_jobs':[],'scope':'All explicit prior controllers awaited; no source/build/worktree/Linux scratch created. Unknown CUA/T3/model/shared hosts are not owned or certified and were untouched. Current report auditor/tool excluded from no-known-prior-job claim.'})
drift={}
for cell in ['bulk16','single16','bulk1','single1']:
 drift[cell]={}
 for v in ['A','B']:
  rows=[x for x in canonical if x['cell']==cell and x['label']==v]
  drift[cell][v]={'first_last_cpu_ratio':rows[-1]['cpu_ns_per_actual_completed_original']/rows[0]['cpu_ns_per_actual_completed_original'],'cpu_ns_range':[min(x['cpu_ns_per_actual_completed_original'] for x in rows),max(x['cpu_ns_per_actual_completed_original'] for x in rows)],'rate_range':[min(x['rate'] for x in rows),max(x['rate'] for x in rows)],'p99_ns_range':[min(x['request_p99_ns'] for x in rows),max(x['request_p99_ns'] for x in rows)]}
save('driver-audit-all18.json',driver);save('canonical-audit-all64.json',canonical);save('drift.json',{'canonical_by_cell_and_label':drift,'load1_before_range':[min(x['load_before'][0] for x in observations),max(x['load_before'][0] for x in observations)],'budget_max_own_bytes':max(x['budget']['own_bytes'] for x in observations),'budget_max_loop_bytes':max(x['budget']['loop_bytes'] for x in observations),'minimum_free_bytes':min(x['budget']['free_bytes'] for x in observations),'scope':'Descriptive raw rusage/load/power/thermal drift only; no parking, allocation, instruction, scheduler or thermal cause inferred; no profiler launched.'})
save('AUDIT.json',{'driver_children':len(driver),'driver_complete_pairs':9,'canonical_children':len(canonical),'canonical_complete_pairs':32,'all_raw_sha256_verified':len(receipt['files']),'all_functional_checks_pass':True,'unpaired':0,'failed_measured_children':0,'driver_stage_gate_pass':True,'linux_observed_task_records':len(linux),'linux_live_violations':0,'vanished_observation_records':len(vanished),'stock_actual_Mac_guards':len(guard_records),'guards':guard_records,'binary_sha256':binary,'cleanup_parent_absent':cleanup['parent_absent'],'Mac_cwd_owned_root_references':cleanup['cwd_audit']['owned_root_references'],'no_known_prior_owned_jobs':True,'limits':'Initial read-only inventory/reads preceded first stockguard/sample audit; absolute Linux affinity/nice readback was set first. Shell/tool/model/T3 and remote interpreter entry startup are not retroactively certified. Foreign/inaccessible/shared/deleted-cwd infrastructure untouched; not a zero-infrastructure claim. No Mac affinity/frequency guarantee or fullqualification claim.'})
print(json.dumps({'audit':'PASS','driver':len(driver),'canonical':len(canonical),'raw_verified':len(receipt['files']),'linux_task_records':len(linux),'guards':len(guard_records),'B_binary':binary,'drift':json.loads((D/'drift.json').read_text())},indent=2))
