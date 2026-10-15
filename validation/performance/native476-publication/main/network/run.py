from pathlib import Path
import hashlib,json,re,subprocess,time
job=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((job.parent/'checked/BEFORE.json').read_text())['files']
def state():return {p:{'sha256':sha(Path(p)),'mode':oct(Path(p).stat().st_mode&0o777)} for p in source}
before=state();assert before==source
(job/'BEFORE.json').write_text(json.dumps({'files':before},indent=2)+'\n')
exe=Path('target/debug/examples/native_benchmark').resolve();binary=sha(exe)
(job/'ARTIFACT.json').write_text(json.dumps({'executable':str(exe),'sha256':binary,'scope':'Current checked debug functional artifact; no native release capacity claim.'},indent=2)+'\n')
tools=job/'tools';assert not tools.exists();tools.mkdir();queue=['validation/check-native-benchmark.mjs'];copied={}
while queue:
 name=queue.pop()
 if name in copied:continue
 p=Path(name);b=p.read_bytes();assert source[name]['sha256']==sha(p)
 dest=tools/p.name;dest.write_bytes(b);copied[name]=sha(dest)
 queue.extend(str(p.parent/ref)for ref in re.findall(r"(?:from\s*|import\s*)['\"](\./[^'\"]+)['\"]",b.decode()))
(tools/'SOURCE.json').write_text(json.dumps(copied,indent=2)+'\n')
(job/'wal').mkdir();records=[];results=[];negative=0;code=1

def run(name,cmd,expect_success=True):
 print('START',name,flush=True);t=time.monotonic()
 with(job/(name+'.stdout')).open('wb')as o,(job/(name+'.stderr')).open('wb')as e:r=subprocess.run(cmd,stdout=o,stderr=e)
 row={'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-t};(job/(name+'.exit.json')).write_text(json.dumps(row,indent=2)+'\n');records.append(row);print('END',name,r.returncode,flush=True)
 assert (r.returncode==0)==expect_success,(name,r.returncode)
 return r
try:
 negatives=[['tcp','8','1','1','--offered','128','--publication'],['tcp','8','1','1','--offered','128','--publication','bad'],['tcp','8','1','1','--offered','128','--publication','atomic','--publication','witness'],['tcp','8','1','--publication','witness'],['tcp','8','1','--journal-timings','--publication','atomic'],['tcp','8','1','1','--publication','witness'],['--lanes','tcp','8','1','--publication','witness']]
 for i,args in enumerate(negatives):
  root=job/'wal'/('invalid-'+str(i));run('invalid-'+str(i),[str(exe),str(root),*args],False);assert not root.exists();negative+=1
 for provider in ['atomic','witness']:
  for protocol in ['tcp','quic']:
   name=provider+'-'+protocol;root=job/'wal'/name;assert not root.exists()
   run(name,[str(exe),str(root),protocol,'512','4096','1','--offered','128','--maintenance','2','--ready-items','16','--offered-capacity','4096','--publication',provider])
   run(name+'-check',['node',str(tools/'check-native-benchmark.mjs'),'--storage',str(root)])
   summary=dict(x.split('=',1)for x in(root/'summary.txt').read_text().split())
   assert summary['offers']=='512'and int(summary['recovered_value'])==64+int(summary['applied'])
   assert summary['retry_verified']==summary['workers_joined']=='true'
   assert summary['publication_requested']==summary['publication_effective']==provider
   assert summary['publication_provider']==('FileLogIo'if provider=='atomic'else'WitnessLogIo')
   assert summary['storage_error_scope']=='overlapping_observations'
   stderr=(job/(name+'.stderr')).read_text();readbacks={kind:[line for line in stderr.splitlines()if line.startswith(kind+'_readback ')]for kind in ['capacity','publication']}
   for kind,lines in readbacks.items():assert len(lines)==6 and sum('mode=Create 'in x for x in lines)==3 and sum('mode=Recover 'in x for x in lines)==3,(name,kind)
   assert all('publication_effective='+provider+' 'in x for x in readbacks['publication'])
   for n in range(1,4):assert (root/('replica'+str(n))/'WITNESS-CURRENT').exists()==(provider=='witness')
   results.append({'name':name,'summary':summary,'readbacks':readbacks});(job/'RESULTS.json').write_text(json.dumps(results,indent=2)+'\n')
 code=0
finally:
 after=state();same=before==after and sha(exe)==binary
 (job/'AFTER.json').write_text(json.dumps({'files':after},indent=2)+'\n');(job/'TERMINAL.json').write_text(json.dumps({'exit':code,'source_exact':same,'cases':len(results),'negative':negative,'commands':records},indent=2)+'\n')
 assert same
raise SystemExit(code)
