import datetime,hashlib,json,pathlib,subprocess,sys
repo=pathlib.Path.cwd();job=repo/'target/p9/native480-admission/checked2';base=json.loads((repo/'target/p9/native476-publication/checked/BEFORE.json').read_text())
def write(n,x):(job/n).write_text(json.dumps(x,indent=2)+'\n')
def run(name,args):
 write(name+'.command.json',args);print('START '+name,flush=True)
 with(job/(name+'.stdout')).open('wb')as out,(job/(name+'.stderr')).open('wb')as err:r=subprocess.run(args,stdout=out,stderr=err)
 write(name+'.exit.json',{'exit':r.returncode,'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()});print('END '+name+' '+str(r.returncode),flush=True)
 if r.returncode:raise SystemExit(r.returncode)
run('format',['cargo','+stable','fmt','--all'])
selected=set(base['files']);selected.update(subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard','--','src','tests','examples'],text=True).splitlines())
def inventory():return {n:{'sha256':hashlib.sha256((repo/n).read_bytes()).hexdigest(),'mode':oct((repo/n).stat().st_mode&0o7777)}for n in sorted(selected)}
before=inventory();write('BEFORE.json',{'base_commit':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'scope':'Final480 production, tests, fixtures, locks/configuration, contract and validator inputs including new untracked modules. Narrative and evidence excluded.','files':before})
commands=[
 ('application-all',['cargo','+stable','test','--locked','--offline','--all-features','--test','application','--','--test-threads=1']),
 ('client-all',['cargo','+stable','test','--locked','--offline','--all-features','--test','effect_owner','client_admission::','--','--test-threads=1']),
 ('provider-all',['cargo','+stable','test','--locked','--offline','--all-features','--test','provider_conformance','--','--test-threads=1']),
 ('library-all',['cargo','+stable','test','--locked','--offline','--all-features','--lib','--','--test-threads=1']),
 ('application-core',['cargo','+stable','test','--locked','--offline','--no-default-features','--test','application','--','--test-threads=1']),
 ('client-core',['cargo','+stable','test','--locked','--offline','--no-default-features','--test','effect_owner','client_admission::','--','--test-threads=1']),
 ('provider-core',['cargo','+stable','test','--locked','--offline','--no-default-features','--test','provider_conformance','--','--test-threads=1']),
 ('service',['cargo','+stable','test','--locked','--offline','--all-features','--test','counter_service','three_','--','--test-threads=1']),
 ('matched',['cargo','+stable','test','--manifest-path','benchmarks/matched/Cargo.toml','--locked','--offline','--','--test-threads=1']),
 ('metadata',['node','validation/check-provider-conformance.mjs']),
 ('metadata-negative',['node','--test','validation/check-provider-conformance.test.mjs']),
]
complete=False
try:
 for name,args in commands:run(name,args)
 complete=True
finally:
 after=inventory();write('AFTER.json',{'files':after});write('TERMINAL.json',{'exit':0 if complete else 1,'same_selected_inputs':before==after,'selected_inputs':len(before),'all_selected_commands_completed':complete})
print('DONE',len(before),flush=True)
