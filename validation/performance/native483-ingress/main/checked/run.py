import datetime, hashlib, json, os, pathlib, subprocess, sys
repo=pathlib.Path.cwd(); job=repo/'target/p9/native483-ingress/checked'
paths=['Cargo.toml','Cargo.lock','clippy.toml','build.rs','.cargo','src','tests','examples','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock','benchmarks/matched/src','docs/component-contracts.json','docs/provider-conformance.json','validation/check-provider-conformance.mjs','validation/check-provider-conformance.test.mjs']
def write(n,x):(job/n).write_text(json.dumps(x,indent=2)+'\n')
def inventory():
 names=subprocess.check_output(['git','ls-files','-co','--exclude-standard','--',*paths],text=True).splitlines()
 return {n:{'sha256':hashlib.sha256((repo/n).read_bytes()).hexdigest(),'bytes':(repo/n).stat().st_size,'mode':oct((repo/n).stat().st_mode&0o7777)} for n in sorted(set(names)) if (repo/n).is_file()}
before=inventory();write('BEFORE.json',{'files':before})
commands=[
 ('application-all',['cargo','+stable','test','--locked','--offline','--all-features','--test','application','--','--test-threads=1']),
 ('client-all',['cargo','+stable','test','--locked','--offline','--all-features','--test','effect_owner','client_admission::','--','--test-threads=1']),
 ('node-all',['cargo','+stable','test','--locked','--offline','--all-features','--test','effect_owner','node_facade::','--','--test-threads=1']),
 ('provider-all',['cargo','+stable','test','--locked','--offline','--all-features','--test','provider_conformance','--','--test-threads=1']),
 ('library-all',['cargo','+stable','test','--locked','--offline','--all-features','--lib','--','--test-threads=1']),
 ('native-scoped',['cargo','+stable','test','--locked','--offline','--all-features','--test','startup','multi::ingress::','--','--test-threads=1']),
 ('native-ready',['cargo','+stable','test','--locked','--offline','--all-features','--test','startup','native_selected_quantums_bound_original_visits_and_recover_exact_individual_retries','--','--test-threads=1']),
 ('service',['cargo','+stable','test','--locked','--offline','--all-features','--test','counter_service','three_','--','--test-threads=1']),
 ('application-core',['cargo','+stable','test','--locked','--offline','--no-default-features','--test','application','--','--test-threads=1']),
 ('client-core',['cargo','+stable','test','--locked','--offline','--no-default-features','--test','effect_owner','client_admission::','--','--test-threads=1']),
 ('node-core',['cargo','+stable','test','--locked','--offline','--no-default-features','--test','effect_owner','node_facade::','--','--test-threads=1']),
 ('provider-core',['cargo','+stable','test','--locked','--offline','--no-default-features','--test','provider_conformance','--','--test-threads=1']),
 ('matched',['cargo','+stable','test','--manifest-path','benchmarks/matched/Cargo.toml','--locked','--offline','--','--test-threads=1']),
 ('metadata',['node','validation/check-provider-conformance.mjs']),
 ('metadata-negative',['/home/karllivesey/.nvm/versions/node/v24.21.0/bin/node','--test','--test-reporter=tap','--test-isolation=none','validation/check-provider-conformance.test.mjs']),
 ('strict',['.githooks/pre-push'])]
env=os.environ.copy();env['CARGO_BUILD_JOBS']='2';complete=False;failure=None
try:
 for name,args in commands:
  write(name+'.command.json',args);print('START '+name,flush=True)
  with(job/(name+'.stdout')).open('wb')as out,(job/(name+'.stderr')).open('wb')as err:r=subprocess.run(args,stdout=out,stderr=err,env=env)
  write(name+'.exit.json',{'exit':r.returncode,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()});print('END '+name+' '+str(r.returncode),flush=True)
  if r.returncode:failure=name;raise SystemExit(r.returncode)
 complete=True
finally:
 after=inventory();write('AFTER.json',{'files':after});write('TERMINAL.json',{'exit':0 if complete else 1,'same_selected_inputs':before==after,'selected_inputs':len(before),'all_selected_commands_completed':complete,'failed_scope':failure})
print('DONE',len(before),flush=True)
