from pathlib import Path
import datetime,hashlib,json,os,subprocess
root=Path.cwd();job=root/'target/p9/native484-ingress/publication-mac';expected=json.loads((root/'target/p9/native483-ingress/final/INPUTS.json').read_text())['files']
def write(n,v):(job/n).write_text(json.dumps(v,indent=2)+'\n')
def source():return {n:{'sha256':hashlib.sha256((root/n).read_bytes()).hexdigest(),'mode':oct((root/n).stat().st_mode&0o7777)}for n in expected}
def run(name,args):
 write(name+'.command.json',args)
 with(job/(name+'.stdout')).open('wb')as out,(job/(name+'.stderr')).open('wb')as err:r=subprocess.run(args,stdout=out,stderr=err)
 write(name+'.exit.json',{'exit':r.returncode,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()});print(name,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
 return (job/(name+'.stdout')).read_text()
before=source();assert before==expected;write('BEFORE.json',{'files':before});assert subprocess.check_output(['git','config','--local','core.hooksPath'],text=True).strip()=='.githooks'
run('prestage-whitespace',['git','diff','--check'])
paths=['docs/IMPLEMENTATION.md','docs/P9.md','validation/performance/native484-ingress']
run('stage',['git','add','--',*paths]);run('staged-whitespace',['git','diff','--cached','--check'])
run('commit',['git','commit','-m','P9: qualify fresh Mac ingress artifacts and both paired configurations'])
commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip();run('push',['git','push','origin','main'])
remote=run('remote',['git','ls-remote','--heads','origin','refs/heads/main']).split()[0];assert remote==commit
after=source();write('AFTER.json',{'files':after});assert before==after==expected
write('RECEIPT.json',{'exit':0,'commit':commit,'remote_exit':0,'remote_main':remote,'enabled_hook':'.githooks/pre-push','same_full_selected_source':True,'selected_inputs':len(expected),'scope':'Fresh published483 Mac artifact qualification and actual both-host484 paired configurations only. No engine/build input changed; actual enabled strict hook. Candidate source remains efd383dc36b4b9edf8f66021b90115824440b468. No paired timing or performance acceptance claim.'});print('DONE',commit,flush=True)
