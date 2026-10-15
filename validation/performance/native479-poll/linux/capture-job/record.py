"""One original owned headless native479 perf capture; never overwrite/restart."""
import os,signal,subprocess,sys,time,traceback
from pathlib import Path
sys.dont_write_bytecode=True
from common import guards,host_snapshot,load,save,sha
job=Path(__file__).resolve().parent
assert sys.argv[1:]==['--authorized-quiet'],'Separate root quiet-window GO required'
ready=load(job/'READY.json');assert sha(job/'CONFIG.json')==ready['config_sha256']
config=load(job/'CONFIG.json');assert config['job']==str(job)
assert not(job/'capture').exists()and not(job/'STARTED.json').exists(),'No overwrite/restart'
policy=lambda:{n:int(Path('/proc/sys/kernel',n).read_text())for n in ['perf_event_paranoid','kptr_restrict','perf_event_mlock_kb']}
def exact():
 guards(config)
 assert sha(job/'SCHEMA.md')==config['schema_sha256']
 assert policy()==config['kernel_policy']
 for item in config['perf_tools']:assert sha(item['path'])==item['sha256'],item['path']
 assert {k:os.environ.get(k)for k in config['inherited_environment']}==config['inherited_environment']
exact()
before=host_snapshot('linux',job);save(job/'before-host.json',before)
assert before['device_identity']==config['device_identity']and before['cpu_policy']==config['cpu_policy']
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines()
assert not any(Path(x.strip()).name in ['cargo','rustc','perf','samply','native_benchmark','native_benchmar','voteboat-matched-framework','voteboat-matche']for x in active),'Competing build/profile/benchmark'
out=job/'capture';out.mkdir();(out/'metadata').mkdir()
save(job/'STARTED.json',{'pid':os.getpid(),'start_unix':time.time(),'scope':config['scope']})
for name,value in [('native.command.json',config['cases'][0]['command']),('resource.command.json',config['cases'][0]['resource_command']),('perf.command.json',config['perf_command'])]:save(out/name,value)
record={'scope':config['scope'],'start_unix':time.time(),'kernel_policy_before':policy(),'perf_exit':None,'native_exit':None,'received_signals':[],'errors':[],'conversion_started':False}
save(out/'RECORDING.json',record);(job/'STATUS').write_text('RECORDING\n')
child=None
# The original perf process and its resource owner/native child inherit one new
# process group. The existing resource owner forwards signals and waits native.
def signal_owned(number):
 if child is not None:
  try:os.killpg(child.pid,number)
  except ProcessLookupError:pass
def interrupted(number,frame):
 record['received_signals'].append(number);signal_owned(number)
for number in [signal.SIGINT,signal.SIGTERM]:signal.signal(number,interrupted)
failed=False
try:
 print('START native-counter-tcp-q64',flush=True)
 env=os.environ.copy();env['LD_LIBRARY_PATH']=config['perf_library_path']
 with(out/'native.stdout').open('wb')as stdout,(out/'perf-native.stderr').open('wb')as stderr:
  child=subprocess.Popen(config['perf_command'],stdout=stdout,stderr=stderr,env=env,start_new_session=True)
  save(out/'OWNED_PROFILER.json',{'pid':child.pid,'process_group':child.pid,'owned_command':config['perf_command']})
  if record['received_signals']:signal_owned(record['received_signals'][-1])
  record['perf_exit']=child.wait()
 (out/'perf.exit').write_text(str(record['perf_exit'])+'\n')
 failed=record['perf_exit']!=0 or bool(record['received_signals'])
except BaseException:
 record['errors'].append(traceback.format_exc());failed=True
finally:
 if child is not None and child.poll()is None:
  signal_owned(signal.SIGTERM)
  try:record['perf_exit']=child.wait(timeout=10)
  except subprocess.TimeoutExpired:signal_owned(signal.SIGKILL);record['perf_exit']=child.wait()
 # Even an early perf failure must not leave its launched helper/native alive.
 if child is not None:
  def members():
   rows=subprocess.check_output(['ps','-eo','pgid=,stat='],text=True).splitlines()
   return [r for r in rows if int(r.split()[0])==child.pid and not r.split()[1].startswith('Z')]
  if members():
   signal_owned(signal.SIGTERM);end=time.monotonic()+5
   while members()and time.monotonic()<end:time.sleep(.05)
   if members():signal_owned(signal.SIGKILL);time.sleep(.1)
   if members():record['errors'].append('Original owned process group did not terminate');failed=True
 try:
  resource=load(out/'metadata/RESOURCES.json');record['native_exit']=resource['exit'];record['resource_helper_exit']=resource['helper_exit']
  assert resource['exit']==resource['helper_exit']==0 and resource['helper_error']is None
 except BaseException:record['errors'].append(traceback.format_exc());failed=True
 try:
  exact();record['post_guard_exit']=0
  after=host_snapshot('linux',job);save(job/'after-host.json',after)
  assert before['device_identity']==after['device_identity']and before['cpu_policy']==after['cpu_policy']
 except BaseException:record['errors'].append(traceback.format_exc());record['post_guard_exit']=1;failed=True
 record.update({'end_unix':time.time(),'kernel_policy_after':policy(),'data_bytes':(out/'perf.data').stat().st_size if(out/'perf.data').exists()else 0})
 failed|=record['data_bytes']==0
 save(out/'RECORDING.json',record)
 terminal={'exit':int(failed),'actual_profiler_exit':record['perf_exit'],'actual_native_exit':record['native_exit'],'all_owned_processes_terminated':not child or not members(),'recording_ended':True,'no_restart_or_replacement':True,'conversion_started':False}
 save(job/'TERMINAL.json',terminal);(job/'STATUS').write_text('RECORDING_FAILED\n'if failed else'RECORDING_COMPLETE\n')
 print('RECORDING_EXIT',int(failed),flush=True)
raise SystemExit(int(failed))
