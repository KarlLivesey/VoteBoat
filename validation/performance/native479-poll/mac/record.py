"""One owned headless Mac479 capture on the existing476 artifact. No browser/build/pilot."""
from pathlib import Path
import datetime,hashlib,json,os,resource,signal,subprocess,sys,time

job=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
config=load(job/'CONFIG.json')
assert str(job)==config['job']
sys.path.insert(0,str(Path(config['authority_config']).parent))
from common import guards,host_snapshot,check_settings,check_readbacks
authority=load(config['authority_config'])
def guard():
 assert str(Path(sys.executable).resolve())==config['python']
 assert sha(config['authority_config'])==config['authority_config_sha256']
 guards(authority)
 for p,h in config['files'].items():assert sha(p)==h,p
 assert sha(job/'record.py')==config['record_sha256']
 assert sha(job/'SCHEMA.md')==config['schema_sha256']
 for name,h in config['validators'].items():assert sha(job/'control-tools'/name)==h,name
 assert config['command'][0]==config['samply']
 assert '--save-only'in config['command']and'--no-open'in config['command']
 assert config['command'][config['command'].index('--rate')+1]=='1000'
 assert config['command'][config['command'].index('--')+1:]==config['case']['command']
 assert config['case']['command'][0]==config['artifact']
 assert config['case']['ready_items']==64 and config['case']['groups']==2 and config['case']['weights']=='31,1'
 assert config['case']['offers']==8192 and config['case']['rate']==1024 and config['case']['window']==4096
 snapshot=host_snapshot('mac',job)
 assert snapshot['device_identity']==authority['device_identity']
 assert snapshot['cpu_policy']==authority['cpu_policy']
 return snapshot

assert len(sys.argv)==2 and sys.argv[1]in ['--prepare','--authorized-quiet']
if sys.argv[1]=='--prepare':
 assert not(job/'READY.json').exists()and not(job/'RECORD_STATUS').exists()
 assert not Path(config['case']['wal_root']).exists()
 snapshot=guard();save(job/'setup-host.json',snapshot)
 save(job/'READY.json',{'exit':0,'scope':'PREPARED ONLY. No native process/profiler/pilot launched; requires separate root quiet GO.',
  'config_sha256':sha(job/'CONFIG.json'),'schema_sha256':config['schema_sha256'],'record_sha256':config['record_sha256'],
  'artifact_sha256':config['files'][config['artifact']],'source_commit':authority['source_commit'],
  'command':config['command'],'symbol_scope':config['symbol_scope'],'sampling_scope':config['sampling_scope']})
 print('MAC479_PREPARED_NO_RECORDING',job,flush=True);raise SystemExit(0)

assert load(job/'READY.json')['config_sha256']==sha(job/'CONFIG.json')
assert not(job/'RECORD_STATUS').exists(),'Never restart or overwrite a recording'
assert not Path(config['case']['wal_root']).exists()
child=None;received=[];cleanup=[];status=1;failure=None;started=time.monotonic()
def terminate_owned():
 if child is None:return
 # Successful samply has joined its recorded process. Avoid touching a re-used
 # numeric process-group id after post-record guards have taken time.
 if child.poll()is not None and child.returncode==0:return
 def alive():
  try:os.killpg(child.pid,0);return True
  except ProcessLookupError:return False
 if not alive():child.wait();return
 try:
  os.killpg(child.pid,signal.SIGTERM);cleanup.append({'action':'SIGTERM_owned_process_group','pgid':child.pid})
 except ProcessLookupError:child.wait();return
 deadline=time.monotonic()+10
 while alive()and time.monotonic()<deadline:
  child.poll();time.sleep(.05)
 if alive():
  try:
   os.killpg(child.pid,signal.SIGKILL);cleanup.append({'action':'SIGKILL_owned_process_group','pgid':child.pid})
  except ProcessLookupError:pass
 child.wait()
def interrupted(signum,frame):
 if received:return
 received.append(signum);terminate_owned();raise KeyboardInterrupt()
for sig in [signal.SIGINT,signal.SIGTERM,signal.SIGHUP]:signal.signal(sig,interrupted)
(job/'RECORD_STATUS').write_text('running\n');(job/'RECORD_STAGE').write_text('guard_before\n')
try:
 save(job/'before-host.json',guard())
 hard=resource.getrlimit(resource.RLIMIT_NOFILE)[1];resource.setrlimit(resource.RLIMIT_NOFILE,(4096 if hard==resource.RLIM_INFINITY else min(4096,hard),hard))
 tmp=job/'record-tmp';tmp.mkdir();env=os.environ.copy();env['PATH']=config['path'];env['TMPDIR']=str(tmp)
 relevant=['PATH','TMPDIR','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','DYLD_INSERT_LIBRARIES','DYLD_LIBRARY_PATH','MallocStackLogging','MallocScribble']
 save(job/'LAUNCH.json',{'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'command':config['command'],
  'cwd':str(job),'environment':{key:env.get(key)for key in relevant},'nofile':resource.getrlimit(resource.RLIMIT_NOFILE),
  'ownership':'Profiler starts in a new session/process group owned by this controller; interrupt sends TERM to that group, waits10s, then KILL+wait if needed. No shared-process kill or browser/server.'})
 (job/'RECORD_STAGE').write_text('recording\n')
 with(job/'record.stdout').open('xb')as out,(job/'record.stderr').open('xb')as err:
  child=subprocess.Popen(config['command'],cwd=job,env=env,stdout=out,stderr=err,start_new_session=True)
  save(job/'OWNED_PROCESS.json',{'controller_pid':os.getpid(),'profiler_pid':child.pid,'pgid':child.pid,'command':config['command']})
  child_exit=child.wait()
 save(job/'record.exit.json',{'exit':child_exit,'elapsed_s':time.monotonic()-started,'command':config['command']})
 assert child_exit==0,child_exit
 assert (job/'profile.json.gz').is_file()and(job/'profile.json.gz').stat().st_size>0
 (job/'RECORD_STAGE').write_text('guard_after\n');save(job/'after-host.json',guard())
 status=0
except BaseException as error:
 failure=repr(error);terminate_owned()
finally:
 terminate_owned()
 terminal={'exit':status,'received_signals':received,'failure':failure,'cleanup':cleanup,'profiler_exit':child.returncode if child is not None else None,
  'elapsed_s':time.monotonic()-started,'end_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
  'profile':{'sha256':sha(job/'profile.json.gz'),'bytes':(job/'profile.json.gz').stat().st_size}if(job/'profile.json.gz').is_file()else None,
  'scope':'Recording only; any profiler JSON finalization is inherent to samply record. No post-record conversion, validation, symbol server, browser or SVG. Wait for both-host terminal/root GO before analysis.'}
 save(job/'TERMINAL.json',terminal);(job/'RECORD_STATUS').write_text(str(status)+'\n');(job/'RECORD_STAGE').write_text('DONE\n'if status==0 else'FAILED\n')
 print('MAC479_RECORD_EXIT',status,flush=True)
raise SystemExit(status)
