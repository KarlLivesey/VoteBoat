from pathlib import Path
import hashlib,json,subprocess,time,os
job=Path(__file__).resolve().parent
out=job/'recording';assert not out.exists(), 'Refuse overwrite/restart of previousrecording';out.mkdir()
meta=json.loads((job/'SOURCE.json').read_text());art=json.loads((job/'ARTIFACTS.json').read_text());exe=next(r for r in art if r['target']=='native_benchmark');binary=Path(exe['copy'])
root=job.parents[2]
perf=root/'target/p9/hotspot-tools/perf-linux/extracted/usr/bin/perf'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
settings=lambda:{name:int(Path('/proc/sys/kernel/'+name).read_text()) for name in ('perf_event_paranoid','kptr_restrict','perf_event_mlock_kb')}
assert settings()=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
assert (job/'STATUS').read_text().strip()=='0' and (job/'STAGE').read_text().strip()=='DONE'
assert sha(binary)==exe['sha256']
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines()
assert not any(x.strip() in ('cargo','rustc','native_benchmar','native_benchmar','samply','perf','voteboat-matche') for x in active)
env=os.environ.copy();env['LD_LIBRARY_PATH']=str(root/'target/p9/hotspot-tools/perf-linux/extracted/usr/lib');env.pop('DEBUGINFOD_URLS',None)
record={'source_commit':meta['commit'],'source_inventory_sha256':sha(job/'SOURCE.json'),'binary':str(binary),'binary_sha256':sha(binary),'symbols':str(job/'SYMBOLS.json'),'symbols_sha256':sha(job/'SYMBOLS.json'),'perf':str(perf),'perf_sha256':sha(perf),'perf_version':subprocess.check_output([str(perf),'--version'],text=True,env=env).strip(),'settings_before':settings(),'scope':'WholeprocessnativeCounter/TCP/TLS/durableWAL CPUdiagnostics includingstartup/campaign/warmup/measurement/receiptverification/recovery/retry/workersjoin. Not unprofiledcapacity oracceptance.','event':'cycles:u','frequency':1000,'call_graph':'dwarf,16384','mmap_pages':128,'cases':[]}
(out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n')
try:
 for name,count,window in [('bulk',15000,32),('singleton',4096,1)]:
  wal=job/('wal-'+name);assert not wal.exists()
  command=[str(perf),'record','--no-buildid-cache','-e','cycles:u','-F','1000','--call-graph','dwarf,16384','-m','128','-o',str(out/(name+'.data')),'--','/usr/bin/env','-u','LD_LIBRARY_PATH',str(binary),str(wal),'tcp',str(count),str(window),'--ready-items','16','--journal-timings']
  (out/(name+'.command.json')).write_text(json.dumps(command,indent=2)+'\n');(job/'RECORD_STAGE').write_text(name+'\n')
  print('START',name,flush=True);start=time.time()
  with (out/(name+'.stdout')).open('w') as stdout,(out/(name+'.stderr')).open('w') as stderr:result=subprocess.run(command,stdout=stdout,stderr=stderr,env=env)
  (out/(name+'.exit')).write_text(str(result.returncode)+'\n');record['cases'].append({'name':name,'operations':count,'window':window,'warmup':64,'start_unix':start,'end_unix':time.time(),'exit':result.returncode,'command':command,'wal_directory':str(wal)})
  (out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n');print('END',name,result.returncode,flush=True)
  if result.returncode:raise SystemExit(result.returncode)
 assert sha(binary)==record['binary_sha256']
 (job/'RECORD_STAGE').write_text('DONE\n')
finally:
 record['settings_after']=settings();(out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n');print('SETTINGS',record['settings_after'],flush=True)
