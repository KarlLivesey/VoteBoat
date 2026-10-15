from pathlib import Path
import json,subprocess,time,datetime,os,hashlib
job=Path(__file__).resolve().parent;assert not(job/'RUN.json').exists() and not(job/'campaign').exists()
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();ready=json.loads((job/'READY.json').read_text());assert sha(job/'campaign.py')==ready['campaign_driver_sha256'];assert sha(job/'CAMPAIGN_CONFIG.json')==ready['campaign_config_sha256']
busy=[line for line in subprocess.check_output(['ps','-eo','pid=,comm='],text=True).splitlines() if line.split()[-1] in ['cargo','rustc','native_benchmar','voteboat-matche','perf','samply']];assert not busy,busy
commit=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip();remote=subprocess.check_output(['git','rev-parse','origin/main'],text=True).strip();assert commit==remote and commit.startswith('9559f37e');assert not subprocess.check_output(['git','status','--porcelain'])
write=lambda name,v:(job/name).write_text(json.dumps(v,indent=2)+'\n');now=lambda:datetime.datetime.now(datetime.timezone.utc).isoformat()
write('QUIET_GRANT.json',{'time':now(),'published_restoration':commit,'enabled_hook_actual_exit':0,'no_competing_build_check_profile_benchmark_processes':not busy,'governor_changes':False,'other_work_on_linux_during_timing':'No builds/checks/copies/archive/competingbenchmarks; only small original-handle statuspolls.','frozen_ready_sha256':sha(job/'READY.json'),'config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),'controller_sha256':sha(job/'campaign.py')})
cmd=['python3',str(job/'campaign.py'),'--authorized-quiet'];write('RUN.json',{'pid':os.getpid(),'start':now(),'command':cmd,'quiet_grant_sha256':sha(job/'QUIET_GRANT.json')});clock=time.monotonic();print('START FS474 twelve serial cases',flush=True)
with (job/'campaign.stdout').open('wb') as out,(job/'campaign.stderr').open('wb') as err:result=subprocess.run(cmd,stdout=out,stderr=err)
settings={}
for p in Path('/sys/devices/system/cpu/cpufreq').glob('policy*'):
 settings[str(p)]={n:(p/n).read_text().strip() for n in ['scaling_governor','energy_performance_preference','scaling_driver','affected_cpus'] if (p/n).exists()}
write('HOST_AFTER.json',{'settings':settings,'time':now()});write('TERMINAL.json',{'actual_exit':result.returncode,'elapsed_s':time.monotonic()-clock,'end':now(),'campaign_status':(job/'CAMPAIGN_STATUS').read_text().strip() if(job/'CAMPAIGN_STATUS').exists() else None,'stage':(job/'CAMPAIGN_STAGE').read_text().strip() if(job/'CAMPAIGN_STAGE').exists() else None});print('FS474_ACTUAL_EXIT='+str(result.returncode),flush=True);raise SystemExit(result.returncode)
