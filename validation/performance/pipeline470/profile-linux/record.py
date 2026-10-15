from pathlib import Path
import os,json,hashlib,subprocess,time,datetime
repo=Path.cwd();job=repo/'target/p9/slice470/profile16'
perf=repo/'target/p9/hotspot-tools/perf-linux/extracted/usr/bin/perf'
env=os.environ.copy();env['LD_LIBRARY_PATH']=str(perf.parent.parent/'lib')
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
def settings():return {n:int(Path('/proc/sys/kernel',n).read_text()) for n in ('perf_event_paranoid','kptr_restrict','perf_event_mlock_kb')}
def write(n,value):(job/n).write_text(json.dumps(value,indent=2)+'\n')
assert settings()=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
assert json.loads((repo/'target/p9/slice470/diagnostic15/TERMINAL.json').read_text())['same_selected_inputs']
selected=json.loads((repo/'target/p9/slice470/diagnostic15/BEFORE.json').read_text())['files']
assert all(sha(repo/n)==v['sha256'] and oct((repo/n).stat().st_mode&0o7777)==v['mode'] for n,v in selected.items())
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines()
assert not any(n.strip() in ('cargo','rustc','perf','samply','native_benchmar') for n in active)
baseline=repo/'target/p9/slice470/baseline14/target/debug/deps/effect_owner-3b9732cae5c45d5b'
candidate=repo/'target/p9/slice470/integration-checks/debug/deps/effect_owner-3b9732cae5c45d5b'
record={'scope':'CPU sampling diagnostic of one failing hundred-group snapshot control, whole debug test launch; NOT throughput/acceptance or an unperturbed progress test. Profile I/O and sampling may alter elapsed outcomes. Both original15s deadlines remain. No builds/checks/copies during recording.','event':'cycles:u','frequency':199,'call_graph':'dwarf,16384','mmap_pages':128,'settings_before':settings(),'perf':str(perf),'perf_sha256':sha(perf),'perf_version':subprocess.check_output([str(perf),'--version'],text=True,env=env).strip(),'cases':[],'artifacts':{name:{'path':str(p),'sha256':sha(p)} for name,p in [('baseline',baseline),('candidate',candidate)]}}
write('RECORDING.json',record)
(job/'host.stdout').write_text(subprocess.check_output(['lscpu','-J'],text=True))
(job/'filesystems.stdout').write_text(subprocess.check_output(['df','-T','/tmp',str(job)],text=True))
(job/'STATUS').write_text('RECORDING\n')
complete=False
try:
 for name,binary in [('baseline',baseline),('candidate',candidate)]:
  tmp=job/(name+'-tmp');tmp.mkdir()
  case_env={**env,'TMPDIR':str(tmp)}
  command=[str(perf),'record','-e','cycles:u','-F','199','--call-graph','dwarf,16384','-m','128','-o',str(job/(name+'.data')),'--','/usr/bin/env','-u','LD_LIBRARY_PATH',str(binary),'native::automatic_hundred_group_checkpoints_reclaim_and_reopen','--exact','--nocapture','--test-threads=1']
  write(name+'.command.json',{'args':command,'TMPDIR':str(tmp),'LD_LIBRARY_PATH':case_env['LD_LIBRARY_PATH']})
  print('START '+name,flush=True);start=time.monotonic()
  with (job/(name+'.stdout')).open('wb') as out,(job/(name+'.stderr')).open('wb') as err: result=subprocess.run(command,stdout=out,stderr=err,env=case_env)
  entry={'name':name,'exit':result.returncode,'elapsed_s':time.monotonic()-start,'data_bytes':(job/(name+'.data')).stat().st_size if (job/(name+'.data')).exists() else 0}
  record['cases'].append(entry);write(name+'.exit.json',entry);write('RECORDING.json',record)
  print('END '+name+' exit='+str(result.returncode),flush=True)
  if result.returncode not in (0,101) or entry['data_bytes']==0:raise SystemExit(1)
 complete=True
finally:
 record['settings_after']=settings();record['recording_complete']=complete
 record['all_workloads_passed']=complete and all(c['exit']==0 for c in record['cases'])
 record['artifact_hashes_unchanged']=all(sha(Path(v['path']))==v['sha256'] for v in record['artifacts'].values())
 record['source_hashes_unchanged']=all(sha(repo/n)==v['sha256'] and oct((repo/n).stat().st_mode&0o7777)==v['mode'] for n,v in selected.items())
 write('RECORDING.json',record)
 write('TERMINAL.json',{'recording_complete':complete,'all_workloads_passed':record['all_workloads_passed'],'settings_unchanged':record['settings_after']==record['settings_before'],'source_hashes_unchanged':record['source_hashes_unchanged'],'artifact_hashes_unchanged':record['artifact_hashes_unchanged']})
 (job/'STATUS').write_text('RECORDING_COMPLETE\n' if complete else 'RECORDING_FAILED\n')
 print('RECORDING_COMPLETE='+str(complete),flush=True)
