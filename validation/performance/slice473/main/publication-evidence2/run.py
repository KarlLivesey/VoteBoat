from pathlib import Path
import os,subprocess,json,datetime,time
job=Path(__file__).resolve().parent
assert not (job/'RUN.json').exists()
env=os.environ.copy();env['CARGO_TARGET_DIR']=str(Path('target/p9/slice472/checks').resolve());env['CARGO_BUILD_JOBS']='2';env['PROTOC']='/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64'
commands=[('stage',['git','add','docs/IMPLEMENTATION.md','docs/P9.md','validation/performance/comparison472/linux','validation/performance/comparison472/LINUX_ARCHIVE_REVIEW.json','validation/performance/comparison472/LINUX_ROOT_ARCHIVE_REVIEW.json','validation/performance/slice473']),('commit',['git','commit','-m','P9: reject pipeline experiment from complete paired results']),('push',['git','push','origin','main']),('remote',['git','ls-remote','origin','refs/heads/main'])]
(job/'RUN.json').write_text(json.dumps({'pid':os.getpid(),'commands':commands,'env':{k:env.get(k) for k in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','PROTOC']},'start':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
code=1
try:
 for name,cmd in commands:
  print('START '+name,flush=True);start=time.monotonic()
  with (job/(name+'.stdout')).open('wb') as out,(job/(name+'.stderr')).open('wb') as err:r=subprocess.run(cmd,env=env,stdout=out,stderr=err)
  (job/(name+'.exit.json')).write_text(json.dumps({'command':cmd,'exit':r.returncode,'seconds':time.monotonic()-start},indent=2)+'\n');print('END '+name+' '+str(r.returncode),flush=True)
  if r.returncode:code=r.returncode;break
 else:code=0
finally:
 (job/'TERMINAL.json').write_text(json.dumps({'exit':code,'end':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
raise SystemExit(code)
