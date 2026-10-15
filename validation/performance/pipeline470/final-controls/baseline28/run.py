import pathlib,json,hashlib,stat,subprocess,os,re,time
ROOT=pathlib.Path('/home/karllivesey/Documents/ChatGPT/VoteBoat');JOB=ROOT/'target/p9/slice470/baseline28';OLD=ROOT/'target/p9/slice470/baseline14'
record=json.loads((OLD/'SOURCE_AFTER.json').read_text());source=pathlib.Path(record['source'])
def write(n,v):(JOB/n).write_text(json.dumps(v,indent=2)+'\n')
def inventory():return {str(p.relative_to(source)):{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'mode':oct(stat.S_IMODE(p.stat().st_mode))} for p in sorted(source.rglob('*')) if p.is_file()}
before=inventory();assert before==record['files'];write('SOURCE_BEFORE.json',{'commit':record['commit'],'source':str(source),'files':before,'matches_baseline14':True})
match=re.search(r'Running tests/effect_owner\.rs \(([^)]+)\)',(OLD/'checkpoint.stderr').read_text());assert match
exe=pathlib.Path(match[1]);elf={'path':str(exe),'sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'mode':oct(stat.S_IMODE(exe.stat().st_mode))}
tmp=JOB/'test-tmp';tmp.mkdir(exist_ok=False)
env=os.environ.copy()
for key in list(env):
 if key in ['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','RUSTC','RUSTDOCFLAGS','CARGO_ENCODED_RUSTDOCFLAGS','CARGO_BUILD_RUSTFLAGS','CARGO_BUILD_RUSTC_WRAPPER','CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER'] or key.startswith('CARGO_PROFILE_'):env.pop(key,None)
env.update(CARGO_TARGET_DIR=str(OLD/'target'),CARGO_BUILD_JOBS='2',RUSTUP_TOOLCHAIN='1.98.1',TMPDIR=str(tmp))
argv=[str(exe),'native::owning_native_facade_checkpoints_restarts_and_retries_hundred_groups','--exact','--nocapture','--test-threads=1']
write('RUN.json',{'argv':argv,'cwd':str(source),'environment':{k:env.get(k) for k in ['TMPDIR','CARGO_TARGET_DIR','CARGO_BUILD_JOBS','RUSTUP_TOOLCHAIN','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','PROTOC']},'build_provenance':str(OLD/'checkpoint.stderr'),'compiler_provenance':str(OLD/'compiler.stdout'),'commit':record['commit'],'ELF':elf,'scope':'Only fourth failed history, unchanged deadline; direct existing test executable, no Cargo/recompile/first baseline repeat.'})
with (JOB/'filesystems.stdout').open('wb') as out,(JOB/'filesystems.stderr').open('wb') as err:rc=subprocess.call(['df','-T',str(source),str(tmp)],stdout=out,stderr=err)
(JOB/'filesystems.exit').write_text(str(rc)+'\n');assert rc==0
(JOB/'STAGE').write_text('exact_fourth_test_running\n');print('exact_fourth_test_running',flush=True)
with (JOB/'test.stdout').open('wb') as out,(JOB/'test.stderr').open('wb') as err:
 p=subprocess.Popen(argv,cwd=source,env=env,stdout=out,stderr=err);write('PID.json',{'pid':p.pid,'started':time.time()});rc=p.wait()
(JOB/'test.exit').write_text(str(rc)+'\n');after=inventory();assert after==before;assert hashlib.sha256(exe.read_bytes()).hexdigest()==elf['sha256'];write('SOURCE_AFTER.json',{'files':after,'matches_before':True,'ELF_unchanged':True})
(JOB/'STATUS').write_text(str(rc)+'\n');write('TERMINAL.json',{'exit':rc,'source_unchanged':True,'ELF_unchanged':True,'finished':time.time()});(JOB/'STAGE').write_text('DONE\n');print('DONE',flush=True);raise SystemExit(rc)
