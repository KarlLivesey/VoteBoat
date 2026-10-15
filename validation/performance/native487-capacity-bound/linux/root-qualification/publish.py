from pathlib import Path
import subprocess,json,hashlib
root=Path.cwd();job=root/'target/p9/native486-capacity-bound/publication';job.mkdir(exist_ok=True);inputs=json.loads((root/'target/p9/native486-capacity-bound/checked/INPUTS_AFTER.json').read_text());h=lambda b:hashlib.sha256(b).hexdigest()
def guard():
 for name,row in inputs.items():
  p=root/name;assert h(p.read_bytes())==row['sha256']and oct(p.stat().st_mode&0o7777)==row['mode'],name
 assert subprocess.check_output(['git','config','--local','core.hooksPath'],text=True).strip()=='.githooks'
 return{'exact_selected_inputs':len(inputs),'source_inputs_sha256':h(json.dumps(inputs,sort_keys=True).encode())}
def run(name,argv):
 print('START',name,flush=True)
 with(job/(name+'.stdout')).open('wb')as out,(job/(name+'.stderr')).open('wb')as err:r=subprocess.run(argv,stdout=out,stderr=err)
 (job/(name+'.exit')).write_text(str(r.returncode)+'\n');print('END',name,r.returncode,flush=True)
 if r.returncode:raise SystemExit(r.returncode)
 return(job/(name+'.stdout')).read_text().strip()
(job/'BEFORE.json').write_text(json.dumps(guard(),indent=2)+'\n');run('whitespace',['git','diff','--check']);run('stage',['git','add','--','src/application.rs','tests/application.rs','tests/application/capacity_bound.rs','docs/P9.md','docs/IMPLEMENTATION.md','validation/performance/native486-capacity-bound']);run('staged-whitespace',['git','diff','--cached','--check']);run('commit',['git','commit','-m','P9: prove spare Counter admission capacity without building a tree']);commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip();run('push',['git','push','origin','main']);remote=run('remote',['git','ls-remote','origin','refs/heads/main']).split()[0];assert remote==commit;(job/'AFTER.json').write_text(json.dumps(guard(),indent=2)+'\n');(job/'RECEIPT.json').write_text(json.dumps({'actual_exit':0,'commit':commit,'remote_main':remote,'enabled_hook':'.githooks/pre-push','selected_inputs':len(inputs),'scope':'Checked486 exact cardinality algorithm plus six tests and qualification. No measured gain or P9 acceptance claimed.'},indent=2)+'\n');print('DONE',commit,flush=True)
