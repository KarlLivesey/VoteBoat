import datetime,hashlib,json,pathlib,subprocess
repo=pathlib.Path.cwd();job=repo/'target/p9/native480-admission/checked3';previous=repo/'target/p9/native480-admission/checked2'
def write(n,x):(job/n).write_text(json.dumps(x,indent=2)+'\n')
old=json.loads((previous/'BEFORE.json').read_text())['files']
def inventory():return {n:{'sha256':hashlib.sha256((repo/n).read_bytes()).hexdigest(),'mode':oct((repo/n).stat().st_mode&0o7777)}for n in old}
before=inventory();changed=[n for n in old if old[n]!=before[n]]
assert changed==['validation/check-provider-conformance.test.mjs'],changed
scopes=['application-all','client-all','provider-all','library-all','application-core','client-core','provider-core','service','matched','metadata']
assert all(json.loads((previous/(n+'.exit.json')).read_text())['exit']==0 for n in scopes)
write('BEFORE.json',{'files':before});write('SOURCE_REUSE.json',{'previous_original_handle':12587,'previous_job':str(previous),'actual_successful_scopes':scopes,'changed_inputs':changed,'justification':'Only metadata negative-control expectation totals changed43/411 to44/415 and three new strict omission controls added. Every Rust/application/service/standalone source, fixture, lock, compiler/configuration and positive metadata input is exact; retain successful checks without rerunning broad unchanged scopes. Prior expected-count failure retained.'})
commands=[('metadata',['node','validation/check-provider-conformance.mjs']),('metadata-negative',['node','--test','validation/check-provider-conformance.test.mjs']),('strict',['.githooks/pre-push'])]
complete=False
try:
 for name,args in commands:
  write(name+'.command.json',args);print('START '+name,flush=True)
  with(job/(name+'.stdout')).open('wb')as out,(job/(name+'.stderr')).open('wb')as err:r=subprocess.run(args,stdout=out,stderr=err)
  write(name+'.exit.json',{'exit':r.returncode,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()});print('END '+name+' '+str(r.returncode),flush=True)
  if r.returncode:raise SystemExit(r.returncode)
 complete=True
finally:
 after=inventory();write('AFTER.json',{'files':after});write('TERMINAL.json',{'exit':0 if complete else 1,'same_selected_inputs':before==after,'selected_inputs':len(before),'all_selected_commands_completed':complete})
print('DONE',len(before),flush=True)
