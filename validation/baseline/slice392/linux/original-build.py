from pathlib import Path
import subprocess,os,json,hashlib,time,sys
p=Path(__file__).resolve().parent
subprocess.run([sys.executable,str(p/'guard.py'),'before-build'],check=True)
env=dict(os.environ,CARGO_TARGET_DIR=str(p/'build'),CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS='true',CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS='true',CARGO_BUILD_JOBS='2')
start=time.monotonic()
with (p/'BUILD.jsonl').open('w') as out,(p/'BUILD.log').open('w') as err:
 rc=subprocess.run(['cargo','+stable','test','--locked','--offline','--release','--all-features','--test','logical_nested_native','--no-run','--message-format=json','--verbose'],cwd=p/'repo',env=env,stdout=out,stderr=err).returncode
row={'stage':'release-no-run','exit':rc,'seconds':round(time.monotonic()-start,2),'target_dir':str(p/'build'),'debug_assertions':True,'overflow_checks':True}
checks=json.loads((p/'CHECKS.json').read_text());checks.append(row);(p/'CHECKS.json').write_text(json.dumps(checks,indent=2)+'\n')
print(json.dumps(row),flush=True)
if rc:sys.exit(rc)
rows=[json.loads(l) for l in (p/'BUILD.jsonl').read_text().splitlines()]
artifacts=[r for r in rows if r.get('reason')=='compiler-artifact' and r['target']['name']=='logical_nested_native' and r.get('executable')]
assert len(artifacts)==1
artifact=artifacts[0];profile=artifact['profile'];assert profile['opt_level']=='3' and profile['debug_assertions'] and profile['overflow_checks']
libs=[r for r in rows if r.get('reason')=='compiler-artifact' and r['target']['name']=='voteboat' and not r['profile']['test']];assert len(libs)==1
lib=libs[0];assert lib['profile']['opt_level']=='3' and lib['profile']['debug_assertions'] and lib['profile']['overflow_checks']
sha=lambda f:hashlib.sha256(Path(f).read_bytes()).hexdigest()
result={'test_artifact':artifact,'binary_sha256':sha(artifact['executable']),'library_artifact':lib,'library_sha256':{f:sha(f) for f in lib['filenames']},'source_inputs':1306}
(p/'BINARY.json').write_text(json.dumps(result,indent=2)+'\n')
subprocess.run([sys.executable,str(p/'guard.py'),'after-build'],check=True);print(json.dumps({'binary':artifact['executable'],'sha256':result['binary_sha256'],'profile':profile,'library_fresh':lib['fresh']}),flush=True)
