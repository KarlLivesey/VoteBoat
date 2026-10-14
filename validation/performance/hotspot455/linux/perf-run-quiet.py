from pathlib import Path
import hashlib,json,subprocess,time,os
root=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat')
out=root/'target/p9/profile455/linux/perf-quiet';assert not out.exists(), 'Refusing overwrite/restart of prior recording';out.mkdir()
binary=root/'target/p9/matched455-linux/artifacts/matched/voteboat-matched-framework'
perf=root/'target/p9/hotspot-tools/perf-linux/extracted/usr/bin/perf'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
dwp=root/'target/p9/matched455-linux/artifacts/matched/voteboat-matched-framework.dwp'
assert sha(dwp)=='2c337bc1a579a5e7150c70346ee23f76ab02f02abfc9b286f7bcd8efadbcde75'
assert sha(binary)=='d567532226a5e7fa7d150b9fd905ffcff4c97905143e4a15ed8e477cd032755f'
settings=lambda:{name:int(Path('/proc/sys/kernel/'+name).read_text()) for name in ('perf_event_paranoid','kptr_restrict','perf_event_mlock_kb')}
assert settings()=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines()
assert not any(x.strip() in ('cargo','rustc','voteboat-matched','voteboat-matche','samply','perf') for x in active)
env=os.environ.copy();env['LD_LIBRARY_PATH']=str(root/'target/p9/hotspot-tools/perf-linux/extracted/usr/lib')
record={'binary':str(binary),'binary_sha256':sha(binary),'dwp':str(dwp),'dwp_sha256':sha(dwp),'perf':str(perf),'perf_sha256':sha(perf),'perf_version':subprocess.check_output([str(perf),'--version'],text=True,env=env).strip(),'settings_before':settings(),'scope':'CPU-sampling diagnostic; not unprofiled capacity or acceptance','events':'cycles:u','frequency':1000,'call_graph':'dwarf,16384','mmap_pages':128,'cases':[]}
(out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n')
try:
 for name,clients,batch,operations in [('bulk',4096,4,20000000),('singleton',1,1,1048576)]:
  command=[str(perf),'record','-e','cycles:u','-F','1000','--call-graph','dwarf,16384','-m','128','-o',str(out/(name+'.data')),'--','/usr/bin/env','-u','LD_LIBRARY_PATH',str(binary),'--engine','voteboat','--clients',str(clients),'--batch',str(batch),'--operations',str(operations),'--warmup','1024','--maintenance','default-retention','--envelopes','upstream-default','--attribution','false','--campaign-timeout-secs','3600']
  (out/(name+'.command.json')).write_text(json.dumps(command,indent=2)+'\n')
  print('START',name,flush=True);start=time.time()
  with (out/(name+'.stdout')).open('w') as stdout,(out/(name+'.stderr')).open('w') as stderr: result=subprocess.run(command,stdout=stdout,stderr=stderr,env=env)
  (out/(name+'.exit')).write_text(str(result.returncode)+'\n')
  record['cases'].append({'name':name,'start_unix':start,'end_unix':time.time(),'exit':result.returncode,'command':command})
  (out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n')
  print('END',name,result.returncode,flush=True)
  if result.returncode:raise SystemExit(result.returncode)
 assert sha(binary)==record['binary_sha256']
 assert sha(dwp)==record['dwp_sha256']
finally:
 record['settings_after']=settings()
 (out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n')
 print('SETTINGS',record['settings_after'],flush=True)
