from pathlib import Path
import hashlib,json,re,subprocess,time,datetime
job=Path(__file__).resolve().parent.parent;controls=job/'controls';h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();write=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
assert (job/'STATUS').read_text().strip()=='0' and (job/'STAGE').read_text().strip()=='DONE'
assert json.loads((job/'BUILD_TERMINAL.json').read_text())['actual_exit']==0
assert not (controls/'RUN.json').exists() and not (controls/'wal').exists()
source=json.loads((job/'SOURCE.json').read_text());artifact=next(x for x in json.loads((job/'ARTIFACTS.json').read_text())if x['target']=='native_benchmark')
assert all(h(artifact[k])==artifact['sha256'] for k in ['original','copy'])
tools=job/'control-tools';assert not tools.exists();tools.mkdir();queue=['validation/check-native-benchmark.mjs'];copied={}
while queue:
 relative=queue.pop()
 if relative in copied:continue
 data=(job/'source'/relative).read_bytes();target=tools/Path(relative).name;target.write_bytes(data);copied[relative]=h(target)
 assert source['sha256'][relative]==h(target)
 queue.extend(str(Path(relative).parent/ref)for ref in re.findall(r"(?:from\s*|import\s*)['\"](\./[^'\"]+)['\"]",data.decode()))
write(tools/'SOURCE.json',{'commit':source['commit'],'sha256':copied});(controls/'wal').mkdir()
files={str(job/'source'/n):{'sha256':d,'mode':source['physical_modes'][n]}for n,d in source['sha256'].items()}
files.update({str(p):{'sha256':h(p),'mode':oct(p.stat().st_mode&0o777)}for p in [Path(__file__),*tools.iterdir()]})
write(controls/'BEFORE.json',files);write(controls/'ARTIFACT.json',artifact)
commands=[];results=[];code=1
write(controls/'RUN.json',{'scope':'Fresh release functional controls only; offered window4096 is selected backlog ceiling, not achieved capacity/batch width.','start':datetime.datetime.now(datetime.timezone.utc).isoformat(),'commands':[['PROCESS',protocol,512,4096,1,'--offered',128,'--maintenance',2,'--ready-items',16,'--offered-capacity',4096]for protocol in ['tcp','quic']]})
def run(name,cmd):
 clock=time.monotonic();print('START '+name,flush=True)
 with(controls/(name+'.stdout')).open('wb')as out,(controls/(name+'.stderr')).open('wb')as err:r=subprocess.run(cmd,stdout=out,stderr=err)
 receipt={'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-clock};write(controls/(name+'.exit.json'),receipt);commands.append(receipt);print('END '+name+' '+str(r.returncode),flush=True);assert r.returncode==0
try:
 for protocol in ['tcp','quic']:
  root=controls/'wal'/protocol;assert not root.exists()
  run(protocol,[artifact['copy'],str(root),protocol,'512','4096','1','--offered','128','--maintenance','2','--ready-items','16','--offered-capacity','4096'])
  run(protocol+'-check',['node',str(tools/'check-native-benchmark.mjs'),'--storage',str(root)])
  summary=dict(x.split('=',1)for x in(root/'summary.txt').read_text().split());readbacks=[x for x in(controls/(protocol+'.stderr')).read_text().splitlines()if x.startswith('capacity_readback ')]
  assert len(readbacks)==6 and sum('mode=Create 'in x for x in readbacks)==3 and sum('mode=Recover 'in x for x in readbacks)==3
  assert all('offered_capacity_effective=4096 'in x and 'client_group_requests=4096 'in x and 'shard_group_items=4112 'in x for x in readbacks)
  assert summary['retry_verified']==summary['workers_joined']=='true' and int(summary['recovered_value'])==64+int(summary['applied'])
  assert summary['offered_capacity_requested']==summary['offered_capacity_effective']=='4096'
  results.append({'protocol':protocol,'summary':summary,'readbacks':readbacks});write(controls/'RESULTS.json',results)
 code=0
finally:
 same=all(Path(p).exists() and h(p)==v['sha256'] and oct(Path(p).stat().st_mode&0o777)==v['mode']for p,v in files.items())
 write(controls/'AFTER.json',{p:{'sha256':h(p),'mode':oct(Path(p).stat().st_mode&0o777)}for p in files if Path(p).exists()})
 write(controls/'TERMINAL.json',{'actual_exit':code,'source_exact':same,'commands':commands,'cases':len(results),'end':datetime.datetime.now(datetime.timezone.utc).isoformat()});print('CONTROLS_EXIT='+str(code),flush=True)
raise SystemExit(code if same else 1)
