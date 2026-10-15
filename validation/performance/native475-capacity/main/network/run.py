from pathlib import Path
import json,subprocess,hashlib,time,datetime,os
repo=Path.cwd();job=Path(__file__).resolve().parent;assert not(job/'RUN.json').exists();binary=repo/'target/p9/slice475/checks/debug/examples/native_benchmark';sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();before=json.loads((job.parent/'root/BEFORE.json').read_text())['files'];assert all(sha(n)==v['sha256'] for n,v in before.items());(job/'wal').mkdir()
invalid=[('missing',['tcp','512','64','1','--offered','128','--offered-capacity']),('unsupported',['tcp','512','64','1','--offered','128','--offered-capacity','256']),('duplicate',['tcp','512','64','1','--offered','128','--offered-capacity','4096','--offered-capacity','4096']),('startup',['tcp','512','64','--offered-capacity','4096']),('closed-shared',['tcp','512','64','1','--offered-capacity','4096']),('zero',['tcp','512','0','1','--offered','128','--offered-capacity','4096']),('too-wide',['tcp','512','4097','1','--offered','128','--offered-capacity','4096']),('legacy-wide',['tcp','512','65','1','--offered','128']),('lanes',['--lanes','1','--offered-capacity','4096'])]
record={'pid':os.getpid(),'binary':str(binary),'binary_sha256':sha(binary),'start':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Unoptimized debug functional controls, not speed/fresh-performance artifact proof','invalid':[],'cases':[]};(job/'RUN.json').write_text(json.dumps(record,indent=2)+'\n')
def run(name,cmd):
 print('START '+name,flush=True);clock=time.monotonic()
 with(job/(name+'.stdout')).open('wb')as out,(job/(name+'.stderr')).open('wb')as err:r=subprocess.run(cmd,stdout=out,stderr=err)
 receipt={'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-clock};(job/(name+'.exit.json')).write_text(json.dumps(receipt,indent=2)+'\n');print('END '+name+' '+str(r.returncode),flush=True);return r.returncode
code=1
try:
 for name,args in invalid:
  root=job/'wal'/('invalid-'+name);assert not root.exists();cmd=[str(binary),str(root),*args] if name!='lanes' else [str(binary),*args];exit=run('invalid-'+name,cmd);assert exit!=0 and not root.exists();record['invalid'].append({'name':name,'exit':exit,'root_absent':True})
 for protocol in ['tcp','quic']:
  for window in [64,256,4096]:
   name=f'{protocol}-w{window}';root=job/'wal'/name;assert not root.exists();cmd=[str(binary),str(root),protocol,'512',str(window),'1','--offered','128','--maintenance','2','--ready-items','16']
   if window>64:cmd+=['--offered-capacity','4096']
   (job/'STAGE').write_text(name+'\n');exit=run(name,cmd);assert exit==0
   check=run(name+'-check',['node','validation/check-native-benchmark.mjs','--storage',str(root)]);assert check==0
   s=dict(x.split('=',1)for x in(root/'summary.txt').read_text().split());lines=[x for x in(job/(name+'.stderr')).read_text().splitlines()if x.startswith('capacity_readback ')];assert s['retry_verified']==s['workers_joined']=='true';assert int(s['recovered_value'])==64+int(s['applied'])
   if window>64:
    assert len(lines)==6 and sum('mode=Create 'in x for x in lines)==3 and sum('mode=Recover 'in x for x in lines)==3
    assert all('offered_capacity_effective=4096 'in x and 'client_group_requests=4096 'in x and 'shard_group_items=4112 'in x for x in lines)
    assert s['offered_capacity_effective']=='4096' and 'offered_capacity_requested=4096'in(root/'run.txt').read_text()
   else:assert not lines and 'offered_capacity_requested' not in s
   record['cases'].append({'name':name,'process_exit':exit,'validator_exit':check,'actual_readbacks':lines,'summary':s})
   (job/'RESULTS.json').write_text(json.dumps(record,indent=2)+'\n')
 code=0
finally:
 same=all(sha(n)==v['sha256'] and oct(Path(n).stat().st_mode&0o7777)==v['mode']for n,v in before.items());(job/'TERMINAL.json').write_text(json.dumps({'exit':code,'source_exact':same,'negative':len(record['invalid']),'cases':len(record['cases']),'end':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n');print('SOURCE_EXACT='+str(same),flush=True)
raise SystemExit(code if same else 1)
