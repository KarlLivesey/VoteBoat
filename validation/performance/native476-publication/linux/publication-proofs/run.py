from pathlib import Path
import subprocess,json,time,hashlib
j=Path(__file__).resolve().parent;records=[]
assert subprocess.check_output(['git','config','--local','core.hooksPath'],text=True).strip()=='.githooks'
paths=['docs/IMPLEMENTATION.md','docs/P9.md','docs/component-contracts.json','docs/provider-conformance.json','examples/benchmark','examples/native_benchmark.rs','src/native/log_store.rs','src/native/log_store/witness.rs','src/native/log_store/witness','tests/journal_barrier.rs','tests/witness_log_store.rs','tests/witness_log_store','validation/check-native-benchmark.mjs','validation/check-provider-conformance.test.mjs','validation/check-publication-timings.mjs','validation/check-publication-timings.test.mjs','validation/check-publication-profile.mjs','validation/check-publication-profile.test.mjs','validation/performance/native475-capacity','validation/performance/native476-publication']
commands=[('diff-check',['git','diff','--check']),('stage',['git','add',*paths]),('commit',['git','commit','--file',str(j/'COMMIT_MESSAGE.txt')]),('push',['git','push','origin','main']),('remote',['git','ls-remote','origin','refs/heads/main'])]
code=1
try:
 for name,cmd in commands:
  print('START',name,flush=True);t=time.monotonic()
  with(j/(name+'.stdout')).open('wb')as o,(j/(name+'.stderr')).open('wb')as e:r=subprocess.run(cmd,stdout=o,stderr=e)
  row={'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-t};records.append(row);(j/(name+'.exit.json')).write_text(json.dumps(row,indent=2)+'\n');print('END',name,r.returncode,flush=True)
  if r.returncode:raise SystemExit(r.returncode)
 head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip();remote=(j/'remote.stdout').read_text().split()[0];assert head==remote
 qualified=json.loads((j.parent/'ROOT_FINAL_INPUTS.json').read_text())['files']
 for name,r in qualified.items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==r['sha256']and oct(Path(name).stat().st_mode&0o777)==r['mode']
 subprocess.run(['git','ls-files','--error-unmatch',*qualified],stdout=subprocess.DEVNULL,check=True)
 (j/'RECEIPT.json').write_text(json.dumps({'commit':head,'exit':0,'remote_exit':0,'remote_main':remote,'enabled_hook':'.githooks/pre-push','enabled_hook_sha256':hashlib.sha256(Path('.githooks/pre-push').read_bytes()).hexdigest(),'qualified_inputs':len(qualified),'source_exact':True},indent=2)+'\n');code=0
finally:(j/'TERMINAL.json').write_text(json.dumps({'exit':code,'commands':records},indent=2)+'\n')
