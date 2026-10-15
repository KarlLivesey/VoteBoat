import hashlib, io, json, os, pathlib, stat, subprocess, tarfile, tempfile, time
ROOT=pathlib.Path('/home/karllivesey/Documents/ChatGPT/VoteBoat')
JOB=ROOT/'target/p9/slice470/baseline14'
COMMIT='2d53b4c43b71a63d63c96b7a3d0062da8d7643ff'
def write(name,value): (JOB/name).write_text(json.dumps(value,indent=2)+'\n')
def stage(value):
 (JOB/'STAGE').write_text(value+'\n'); print(value,flush=True)
def inventory(source):
 return {str(p.relative_to(source)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mode':oct(stat.S_IMODE(p.stat().st_mode))} for p in sorted(source.rglob('*')) if p.is_file()}
def command(name,args,cwd,env):
 write(name+'.command.json',{'argv':args,'cwd':str(cwd)})
 with (JOB/(name+'.stdout')).open('wb') as out,(JOB/(name+'.stderr')).open('wb') as err:
  process=subprocess.Popen(args,cwd=cwd,env=env,stdout=out,stderr=err)
  write(name+'.pid.json',{'pid':process.pid,'started':time.time()})
  result=process.wait()
 (JOB/(name+'.exit')).write_text(str(result)+'\n')
 return result
stage('exporting_exact_published_baseline')
assert subprocess.check_output(['git','rev-parse',COMMIT],cwd=ROOT,text=True).strip()==COMMIT
entries={}
for record in subprocess.check_output(['git','ls-tree','-r','-z',COMMIT],cwd=ROOT).split(b'\0'):
 if not record: continue
 meta,raw=record.split(b'\t'); mode,kind,blob=meta.decode().split(); path=raw.decode()
 if path.startswith(('src/','tests/','examples/','.cargo/')) or path in ['Cargo.toml','Cargo.lock','build.rs','clippy.toml','.clippy.toml','rust-toolchain.toml','LICENSE','AGENTS.md']:
  assert kind=='blob' and mode in ['100644','100755'],path
  entries[path]={'git_mode':mode,'blob':blob}
source=pathlib.Path(tempfile.mkdtemp(prefix='voteboat-470-baseline-'))
archive=subprocess.check_output(['git','archive','--format=tar',COMMIT,'--',*entries],cwd=ROOT)
with tarfile.open(fileobj=io.BytesIO(archive)) as tar: tar.extractall(source,filter='data')
for relative,entry in entries.items():
 p=source/relative; data=p.read_bytes()
 assert hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()==entry['blob'],relative
 physical=0o755 if entry['git_mode']=='100755' else 0o644
 current=ROOT/relative
 if relative.startswith('tests/fixtures/') and current.is_file() and hashlib.sha256(current.read_bytes()).digest()==hashlib.sha256(data).digest(): physical=stat.S_IMODE(current.stat().st_mode)
 p.chmod(physical)
before=inventory(source); write('SOURCE_BEFORE.json',{'commit':COMMIT,'source':str(source),'files':before,'git':entries})
(JOB/'SOURCE_PATH').write_text(str(source)+'\n')
tmp=JOB/'test-tmp'; tmp.mkdir(exist_ok=False)
target=JOB/'target'; assert not target.exists()
env=os.environ.copy()
for key in list(env):
 if key in ['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','RUSTC','RUSTDOCFLAGS','CARGO_ENCODED_RUSTDOCFLAGS','CARGO_BUILD_RUSTFLAGS','CARGO_BUILD_RUSTC_WRAPPER','CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER'] or key.startswith('CARGO_PROFILE_'): env.pop(key,None)
env.update(CARGO_TARGET_DIR=str(target),CARGO_BUILD_JOBS='2',RUSTUP_TOOLCHAIN='1.98.1',TMPDIR=str(tmp))
args=['cargo','+stable','test','--locked','--offline','--all-features','--test','effect_owner','native::automatic_hundred_group_checkpoints_reclaim_and_reopen','--','--exact','--nocapture','--test-threads=1']
write('RUN.json',{'commit':COMMIT,'source':str(source),'argv':args,'environment':{k:env.get(k) for k in ['CARGO_TARGET_DIR','CARGO_BUILD_JOBS','RUSTUP_TOOLCHAIN','TMPDIR','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','PROTOC']},'deadline':'Unchanged committed15s fixture; no source edits','permission':'require_escalated for authorized TCP/QUIC localhost test'})
for name,cmd in [('compiler',['rustc','+stable','-Vv']),('cargo',['cargo','+stable','-V']),('filesystems',['df','-T',str(source),str(tmp),str(JOB)])]: assert command(name,cmd,source,env)==0
assert 'release: 1.98.1' in (JOB/'compiler.stdout').read_text()
stage('baseline_test_compiling_or_running')
result=command('checkpoint',args,source,env)
stage('source_after_guard')
after=inventory(source); write('SOURCE_AFTER.json',{'commit':COMMIT,'source':str(source),'files':after,'matches_before':before==after}); assert before==after
(JOB/'STATUS').write_text(str(result)+'\n'); write('TERMINAL.json',{'exit':result,'source_unchanged':True,'finished':time.time()})
stage('DONE'); raise SystemExit(result)
