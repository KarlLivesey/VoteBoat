from pathlib import Path
import hashlib,json,subprocess,time,os
root=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat')
out=root/'target/p9/profile426/linux'
binary=root/'target/p9/matched426-linux/artifacts/matched/voteboat-matched-framework'
samply=root/'target/p9/hotspot-tools/samply-linux/samply-x86_64-unknown-linux-gnu/samply'
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert sha(binary)=='e36086ec99160d70010dc2f4b46f04161fd0de410fc2460ffb9b708bea1d9b9c'
assert Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip()=='1'
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines()
assert not any(x.strip() in ('cargo','rustc','voteboat-matched','samply') for x in active),active
record={'binary':str(binary),'binary_sha256':sha(binary),'samply':str(samply),'samply_sha256':sha(samply),'samply_version':subprocess.check_output([str(samply),'--version'],text=True).strip(),'policy_before_record':1,'scope':'CPU-sampling diagnostic; not unprofiled capacity or acceptance','artifact_config':'validation/performance/matched426-linux/CONFIG.json','cases':[]}
(out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n')
try:
 for name,clients,batch,operations in [('bulk',4096,4,20000000),('singleton',1,1,1048576)]:
  command=[str(samply),'record','--save-only','--unstable-presymbolicate','--rate','1000','--output',str(out/(name+'.json.gz')),'--',str(binary),'--engine','voteboat','--clients',str(clients),'--batch',str(batch),'--operations',str(operations),'--warmup','1024','--maintenance','default-retention','--envelopes','upstream-default','--attribution','false','--campaign-timeout-secs','3600']
  (out/(name+'.command.json')).write_text(json.dumps(command,indent=2)+'\n')
  print('START',name,flush=True)
  start=time.time()
  with (out/(name+'.stdout')).open('w') as stdout,(out/(name+'.stderr')).open('w') as stderr:
   result=subprocess.run(command,stdout=stdout,stderr=stderr)
  (out/(name+'.exit')).write_text(str(result.returncode)+'\n')
  record['cases'].append({'name':name,'start_unix':start,'end_unix':time.time(),'exit':result.returncode,'command':command})
  (out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n')
  print('END',name,result.returncode,flush=True)
  if result.returncode: raise SystemExit(result.returncode)
 assert sha(binary)==record['binary_sha256']
finally:
 Path('/tmp/voteboat-samply-426-done').touch()
 deadline=time.monotonic()+5
 while Path('/proc/sys/kernel/perf_event_paranoid').read_text().strip()!='2' and time.monotonic()<deadline: time.sleep(.1)
 record['policy_after_record']=int(Path('/proc/sys/kernel/perf_event_paranoid').read_text())
 (out/'RECORDING.json').write_text(json.dumps(record,indent=2)+'\n')
 print('POLICY',record['policy_after_record'],flush=True)
