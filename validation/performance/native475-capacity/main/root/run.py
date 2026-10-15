from pathlib import Path
import subprocess,hashlib,json,os,time,datetime
repo=Path.cwd();job=Path(__file__).resolve().parent;assert not(job/'RUN.json').exists();sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
names=subprocess.check_output(['git','ls-files','--cached','--others','--exclude-standard'],text=True).splitlines();prefix=('src/','tests/','examples/','benchmarks/matched/src/','.cargo/','.githooks/');fixed={'Cargo.toml','Cargo.lock','clippy.toml','build.rs','LICENSE','README.md','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock','benchmarks/matched/README.md','benchmarks/matched/SCHEMA.md','benchmarks/matched/COMPARISON.md','docs/component-contracts.json','docs/provider-conformance.json'};selected=sorted({n for n in names if n.startswith(prefix) or n in fixed or n.startswith('validation/') and n.count('/')==1 and n.endswith('.mjs')});before={n:{'sha256':sha(n),'mode':oct(Path(n).stat().st_mode&0o7777)} for n in selected};(job/'BEFORE.json').write_text(json.dumps({'head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),'files':before},indent=2)+'\n')
env=os.environ.copy();env.update(CARGO_BUILD_JOBS='2',CARGO_TARGET_DIR=str(repo/'target/p9/slice475/checks'),PROTOC='/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64');(job/'tmp').mkdir();env['TMPDIR']=str(job/'tmp')
for k in ['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER']:env.pop(k,None)
commands=[('example-all',['cargo','+stable','test','--locked','--offline','--all-features','--example','native_benchmark','--','--test-threads=1']),('example-build',['cargo','+stable','build','--locked','--offline','--all-features','--example','native_benchmark']),('offered-js',['node','validation/check-offered.test.mjs'])]
(job/'RUN.json').write_text(json.dumps({'pid':os.getpid(),'commands':commands,'env':{k:env.get(k) for k in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','PROTOC','TMPDIR']},'start':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n');code=1
try:
 for name,cmd in commands:
  (job/'STAGE').write_text(name+'\n');print('START '+name,flush=True);clock=time.monotonic()
  with(job/(name+'.stdout')).open('wb')as out,(job/(name+'.stderr')).open('wb')as err:r=subprocess.run(cmd,env=env,stdout=out,stderr=err)
  (job/(name+'.exit.json')).write_text(json.dumps({'command':cmd,'exit':r.returncode,'elapsed_s':time.monotonic()-clock},indent=2)+'\n');print('END '+name+' '+str(r.returncode),flush=True)
  if r.returncode:code=r.returncode;break
 else:code=0
finally:
 after={n:{'sha256':sha(n),'mode':oct(Path(n).stat().st_mode&0o7777)}for n in selected if Path(n).exists()};same=after==before;(job/'AFTER.json').write_text(json.dumps({'files':after,'source_exact':same},indent=2)+'\n');(job/'TERMINAL.json').write_text(json.dumps({'exit':code,'source_exact':same,'end':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n');print('SOURCE_EXACT='+str(same),flush=True)
raise SystemExit(code if same else 1)
