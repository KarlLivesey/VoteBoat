"""Actual postterminal Mac482 validator and lossless text/profile export; no recording."""
from pathlib import Path
import hashlib,json,subprocess,sys,tarfile,time

job=Path('/private/tmp/voteboat-native482-sxtrtW')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
config=load(job/'CONFIG.json');terminal=load(job/'TERMINAL.json')
assert terminal['exit']==terminal['profiler_exit']==0 and terminal['received_signals']==[] and terminal['failure']is None
assert (job/'RECORD_STATUS').read_text().strip()=='0'and(job/'RECORD_STAGE').read_text().strip()=='DONE'
assert sha(job/'profile.json.gz')==terminal['profile']['sha256']
assert sha(job/'CONFIG.json')==load(job/'READY.json')['config_sha256']
assert sha(job/'record.py')==config['record_sha256']and sha(job/'SCHEMA.md')==config['schema_sha256']
for path,h in config['files'].items():assert sha(path)==h,path
for name,h in config['validators'].items():assert sha(job/'control-tools'/name)==h,name
sys.path.insert(0,str(Path(config['authority_config']).parent))
from common import guards,check_readbacks,check_settings
authority=load(config['authority_config']);guards(authority)
before=load(job/'before-host.json');after=load(job/'after-host.json')
assert before['device_identity']==after['device_identity']==authority['device_identity']
assert before['cpu_policy']==after['cpu_policy']==authority['cpu_policy']
command=[config['node'],str(job/'control-tools/check-native-benchmark.mjs'),'--storage',config['case']['wal_root']]
assert not(job/'validation.exit.json').exists(),'Never overwrite an original validation'
with(job/'validation.stdout').open('xb')as out,(job/'validation.stderr').open('xb')as err:r=subprocess.run(command,stdout=out,stderr=err)
validation={'command':command,'exit':r.returncode};(job/'validation.exit.json').write_text(json.dumps(validation,indent=2)+'\n');assert r.returncode==0
summary=check_settings(config['case'],Path(config['case']['wal_root']));readbacks=check_readbacks((job/'record.stderr').read_text().splitlines(),'witness')
assert int(summary['recovered_value'])==64+int(summary['applied'])and summary['retry_verified']==summary['workers_joined']=='true'
assert all(summary[k]=='0'for k in ['no_leader','admission_refused','not_proposed','unknown','pending'])
assert int(summary['checkpoints'])>0 and int(summary['reclaims'])>0
(job/'VALIDATION.json').write_text(json.dumps({'exit':0,'validation':validation,'summary':summary,'readbacks':readbacks,'source_artifact_device_policy_guards_exact':True,'scope':'Post-both-terminal frozen validation. Profiled rates/tails are diagnostics, not unprofiled acceptance.'},indent=2)+'\n')
selected={p.relative_to(job).as_posix():p for p in job.rglob('*')if p.is_file()and p.parts[len(job.parts)]not in ['workload','record-tmp']and'__pycache__'not in p.parts and not p.name.startswith('COLLECTED.')}
for p in Path(config['case']['wal_root']).iterdir():
 if p.is_file()and p.suffix in ['.csv','.txt']:selected['results/'+p.name]=p
assert 'results/offers.csv'in selected and 'profile.json.gz'in selected
inventory={name:{'sha256':sha(p),'bytes':p.stat().st_size,'mode':oct(p.stat().st_mode&0o777)}for name,p in sorted(selected.items())}
receipt={'original_handle':18694,'actual_profiler_exit':0,'validation_exit':0,'source_commit':authority['source_commit'],'collected_unix':time.time(),'files':inventory,'scope':'Original Mac482 JSON/text/CSV only, after both captures actually terminated/root GO. No source,binary,buildtree,generated WAL transfer or recapture.'}
(job/'COLLECTED.json').write_text(json.dumps(receipt,indent=2)+'\n')
with tarfile.open(job/'COLLECTED.tar.gz','w:gz')as tar:
 for name,p in sorted(selected.items()):tar.add(p,arcname=name,recursive=False)
 tar.add(job/'COLLECTED.json',arcname='COLLECTED.json',recursive=False)
assert all(sha(selected[name])==row['sha256']for name,row in inventory.items())
print(json.dumps({'exit':0,'files':len(inventory),'archive_sha256':sha(job/'COLLECTED.tar.gz'),'archive_bytes':(job/'COLLECTED.tar.gz').stat().st_size,'manifest_sha256':sha(job/'COLLECTED.json'),'summary':summary},indent=2))
