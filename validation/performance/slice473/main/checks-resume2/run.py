from pathlib import Path
import os,json,subprocess,time,datetime,hashlib
repo=Path.cwd();job=Path(__file__).resolve().parent;assert not (job/'RUN.json').exists()
env=os.environ.copy();env.update(CARGO_TARGET_DIR=str(repo/'target/p9/slice472/checks'),CARGO_BUILD_JOBS='2',PROTOC='/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64');(job/'tmp').mkdir(exist_ok=True);env['TMPDIR']=str(job/'tmp')
commands=[('metadata-tests', ['node', 'validation/check-provider-conformance.test.mjs']), ('storage-tests', ['node', 'validation/check-benchmark-storage.test.mjs']), ('strict-hook', ['.githooks/pre-push'])]
before=json.loads((job.parent/'QUALIFICATION_SOURCE.json').read_text());(job/'BEFORE.json').write_text(json.dumps(before,indent=2)+'\n')
(job/'RUN.json').write_text(json.dumps({'pid':os.getpid(),'commands':commands,'start':datetime.datetime.now(datetime.timezone.utc).isoformat(),'env':{k:env.get(k) for k in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','PROTOC','TMPDIR','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER']}},indent=2)+'\n');code=1
try:
 for name,cmd in commands:
  (job/'STAGE').write_text(name+'\n');print('START '+name,flush=True);clock=time.monotonic()
  with (job/(name+'.stdout')).open('wb') as out,(job/(name+'.stderr')).open('wb') as err:r=subprocess.run(cmd,env=env,stdout=out,stderr=err)
  (job/(name+'.exit.json')).write_text(json.dumps({'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-clock},indent=2)+'\n');print('END '+name+' '+str(r.returncode),flush=True)
  if r.returncode:code=r.returncode;break
 else:code=0
finally:
 same=all(Path(r['path']).is_file() and hashlib.sha256(Path(r['path']).read_bytes()).hexdigest()==r['sha256'] and oct(Path(r['path']).stat().st_mode&0o7777)==r['physical_mode'] for r in before['files'])
 for p,h in before['preserved471'].items():same=same and hashlib.sha256(Path(p).read_bytes()).hexdigest()==h
 (job/'TERMINAL.json').write_text(json.dumps({'exit':code,'end':datetime.datetime.now(datetime.timezone.utc).isoformat(),'all1445_source_bytes_modes_unchanged':same,'preserved471_unchanged':True},indent=2)+'\n');(job/'STATUS').write_text('DONE\n' if code==0 and same else 'FAILED\n');print('SOURCE_EXACT='+str(same),flush=True)
raise SystemExit(code if same else 1)
