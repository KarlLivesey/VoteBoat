from pathlib import Path
import json,hashlib,os,subprocess,time,sys
p=Path(__file__).resolve().parent;h=lambda f:hashlib.sha256(Path(f).read_bytes()).hexdigest();subprocess.run([sys.executable,str(p/'guard.py'),'before-build'],check=True)
env=dict(os.environ,CARGO_TARGET_DIR=str(p/'build-default'),CARGO_BUILD_JOBS='2',CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS='true',CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS='true');cmd=['cargo','+stable','test','--locked','--offline','--release','--test','logical_nested_native','--test','logical_activation_nested','--no-run','--message-format=json','--verbose'];start=time.monotonic()
with (p/'BUILD.jsonl').open('w') as out,(p/'BUILD.log').open('w') as err: rc=subprocess.run(cmd,cwd=p/'repo',env=env,stdout=out,stderr=err).returncode
row={'command':cmd,'cwd':str(p/'repo'),'exit':rc,'seconds':round(time.monotonic()-start,2),'featureSelection':'DEFAULT (no feature flags)','targetDir':str(p/'build-default'),'debugAssertions':True,'overflowChecks':True};(p/'BUILD_RUN.json').write_text(json.dumps(row,indent=2)+'\n');print(json.dumps(row),flush=True)
if rc:sys.exit(rc)
rows=[json.loads(l) for l in (p/'BUILD.jsonl').read_text().splitlines()];names=['logical_nested_native','logical_activation_nested'];records=[r for r in rows if r.get('reason')=='compiler-artifact' and ((r['target']['name'] in names and r.get('executable')) or (r['target']['name']=='voteboat' and r['target']['kind']==['lib']))];assert len(records)==3
for r in records:
 assert sorted(r['features'])==['default','native','tls'] and r['profile']['opt_level']=='3' and r['profile']['debug_assertions'] and r['profile']['overflow_checks']
artifacts={};lists={}
for r in records:
 for n in r['filenames']:artifacts[n]=h(n)
 if r.get('executable'):
  out=subprocess.check_output([r['executable'],'--list'],text=True);(p/(r['target']['name']+'-LIST.txt')).write_text(out);lists[r['target']['name']]=[l[:-6] for l in out.splitlines() if l.endswith(': test')];assert lists[r['target']['name']]
a={'records':records,'artifacts':artifacts,'enumeratedTests':lists};(p/'BINARY.json').write_text(json.dumps(a,indent=2)+'\n');subprocess.run([sys.executable,str(p/'guard.py'),'after-build'],check=True);print(json.dumps({'enumeration':{n:len(v) for n,v in lists.items()},'libraryFeatures':['default','native','tls']}),flush=True)
