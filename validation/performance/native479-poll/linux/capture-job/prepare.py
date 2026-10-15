"""Prepare one existing-artifact native Counter perf diagnostic; no capture/pilot."""
import ast,os,subprocess,sys
from pathlib import Path
sys.dont_write_bytecode=True
from common import guards,host_snapshot,load,proof,save,sha
job=Path(__file__).resolve().parent
assert not(job/'CONFIG.json').exists()and not(job/'READY.json').exists(),'No overwrite'
prior=job.parents[1]/'native478-ready/linux/CAMPAIGN_CONFIG.json'
previous=load(prior);assert previous['source_commit']=='7e9068296ce00dde5bf3e03bf4d852ed2833d185'
perf=job.parents[1]/'hotspot-tools/perf-linux/extracted/usr/bin/perf';library=perf.parent.parent/'lib'
assert sha(perf)=='f0fe2b92d280391fa7efa55616f098ba683b9682cf145275d5eda136125f2a64'
python=previous['python'];node=previous['node'];assert str(Path(sys.executable).resolve())==python
case=dict(next(c for c in previous['cases']if c['name']=='t1-bulk-tcp-q64'))
case.update({'name':'native-counter-tcp-q64','wal_root':str(job/'capture/wal'),'metadata':str(job/'capture/metadata')})
case['command']=case['command'].copy();case['command'][1]=case['wal_root']
case['resource_command']=[python,str(job/'resources.py'),case['metadata'],*case['command']]
perf_command=[str(perf),'record','-e','cycles:u','-F','1000','--call-graph','dwarf,16384','-m','128','-o',str(job/'capture/perf.data'),'--','/usr/bin/env','-u','LD_LIBRARY_PATH',*case['resource_command']]
assert [case[k]for k in ['ready_items','offers','rate','window','groups','publication']]==[64,8192,1024,4096,1,'witness']
assert case['command'][case['command'].index('--maintenance')+1]=='2'
settings={n:int(Path('/proc/sys/kernel',n).read_text())for n in ['perf_event_paranoid','kptr_restrict','perf_event_mlock_kb']}
assert settings=={'perf_event_paranoid':2,'kptr_restrict':2,'perf_event_mlock_kb':516}
for name in ['prepare.py','record.py','validate.py','common.py','resources.py']:ast.parse((job/name).read_text())
assert sha(job/'resources.py')==sha(job.parents[1]/'native478-ready/linux/resources.py')
assert {p.name:sha(p)for p in(job/'control-tools').iterdir()}==previous['validators']
env=os.environ.copy();env['LD_LIBRARY_PATH']=str(library)
version=subprocess.run([str(perf),'--version'],capture_output=True,env=env)
(job/'perf-version.stdout').write_bytes(version.stdout);(job/'perf-version.stderr').write_bytes(version.stderr);(job/'perf-version.exit').write_text(str(version.returncode)+'\n')
assert version.returncode==0
snapshot=host_snapshot('linux',job);save(job/'setup-host.json',snapshot)
assert snapshot['device_identity']==previous['device_identity']and snapshot['cpu_policy']==previous['cpu_policy']
config={**previous,'scope':'479 Linux cycles:u CPU sampling diagnostic, whole perf launch including Python resource owner, startup/warmup/native measurement/drain/maintenance/verification/recovery/cleanup. Native476 debug0 limits inline/source resolution; raw addresses/unresolved/truncated samples retained. Not unprofiled throughput or an acceptance result. No browser/SVG/sudo/sysctl/install/rebuild.','job':str(job),'wal_parent':str(job),'cases':[case],'scripts':{n:sha(job/n)for n in ['prepare.py','record.py','validate.py','common.py','resources.py']},'schema_sha256':sha(job/'SCHEMA.md'),'kernel_policy':settings,'perf_command':perf_command,'perf_library_path':str(library),'perf_tools':[proof(perf),proof((library/'libpfm.so.4').resolve())],'perf_version':version.stdout.decode().strip(),'inherited_environment':{k:os.environ.get(k)for k in ['PATH','TMPDIR','RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','LD_LIBRARY_PATH']},'validator_command':[node,str(job/'control-tools/check-native-benchmark.mjs'),'--storage',case['wal_root']],'existing_native_drain_guard_seconds':120,'device_identity':snapshot['device_identity'],'cpu_policy':snapshot['cpu_policy'],'recording_policy':'Never overwrite/restart. Root quiet GO before record.py; retain original handle through actual terminal. No conversion or validation until both captures actually terminate and root GO. Owned original perf process group receives forwarded signals; existing resource owner waits native. On controller failure terminate then wait; preserve partial perf data.','provenance':{'config478':proof(prior),'qualification':'Published476 artifact/root/buildscript native freshness and actual478 q64 functional controls, bound in candidate proofs. No new workload pilot.'}}
save(job/'CONFIG.json',config);guards(config)
save(job/'PREPARATION.json',{'exit':0,'static_checks':'Five Python ASTs, exact frozen eight-module validator and existing resource-owner hashes; native matrix/perf argv and source/artifact/compiler/runtime SHA/modes guards pass. No capture or benchmark pilot.','kernel_policy':settings,'source_files':len(config['candidate']['source']['files']),'source_directories':len(config['candidate']['source']['directories']),'perf_version':config['perf_version'],'perf_tools':config['perf_tools'],'symbols':'Existing476 native artifact debug0 only; no mismatched framework debug2 symbols or rebuilt artifact used. Native ELF/root freshness/buildID recorded in candidate476 proofs.','recording_started':False})
save(job/'READY.json',{'config_sha256':sha(job/'CONFIG.json'),'scripts':config['scripts'],'schema_sha256':config['schema_sha256'],'native_artifact_sha256':next(a['sha256']for a in config['candidate']['artifacts']if a['target']=='native_benchmark'),'perf_tools':config['perf_tools'],'record_command':[python,str(job/'record.py'),'--authorized-quiet'],'postterminal_validation_command':[python,str(job/'validate.py'),'--authorized-postterminal'],'scope':'PREPARED ONLY. No recording, benchmark pilot, conversion or symbol server started. Root publication/all-host setup completion and separate quiet GO required.'})
print((job/'READY.json').read_text())
