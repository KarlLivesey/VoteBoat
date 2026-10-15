"""Collect original478 after both hosts terminated; no workload or source edits."""
from pathlib import Path
import hashlib,json,stat,sys,tarfile,time

job=Path(sys.argv[1]).resolve()
sys.path.insert(0,str(job))
from common import guards
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
terminal=load(job/'TERMINAL.json');config=load(job/'CAMPAIGN_CONFIG.json');campaign=load(job/'campaign/CAMPAIGN.json')
assert terminal['exit']==0 and terminal['all_timing_ended'] and terminal['cases_terminated']==48
assert terminal['no_retries_or_replacements'] and terminal['received_signal'] is None
assert (job/'CAMPAIGN_STATUS').read_text().strip()=='0' and (job/'CAMPAIGN_STAGE').read_text().strip()=='ALL_TIMING_END'
assert len(campaign['cases'])==len(config['cases'])==48 and not campaign['post_timing_errors']
guards(config)
before=load(job/'before-host.json');after=load(job/'after-host.json')
assert before['device_identity']==after['device_identity']==config['device_identity']
assert before['cpu_policy']==after['cpu_policy']==config['cpu_policy']
selected={}
for p in job.rglob('*'):
 if not p.is_file():continue
 rel=p.relative_to(job).as_posix()
 if p.name.startswith('COLLECTED.') or p.name=='collect478.py' or '__pycache__' in p.parts:continue
 selected[rel]=p
for case in campaign['cases']:
 assert case['exit']==case['validator_exit']==case['readbacks_exit']==0,case['name']
 assert (job/'campaign'/(case['name']+'.exit')).read_text().strip()=='0'
 assert (job/'campaign'/(case['name']+'-check.exit')).read_text().strip()=='0'
 resource=load(Path(case['metadata'])/'RESOURCES.json')
 assert resource['exit']==resource['helper_exit']==0 and resource['max_rss_unit']=='bytes'
 root=Path(case['wal_root'])
 for p in root.iterdir():
  if p.is_file() and p.suffix in ('.csv','.txt'):selected['results/'+case['name']+'/'+p.name]=p
 assert 'results/'+case['name']+'/offers.csv' in selected
assert all(p.suffix not in ('.rlib','.rmeta','.o') for p in selected.values())
inventory={name:{'sha256':sha(p),'bytes':p.stat().st_size,'mode':oct(stat.S_IMODE(p.stat().st_mode))} for name,p in sorted(selected.items())}
receipt={'scope':'Original Mac478 campaign14036, collected after root confirmed both hosts actually terminated. No rerun/source/binary/buildtree/WAL transfer.',
 'original_handle':14036,'actual_exit':0,'actual_cases':48,'actual_validators':48,'actual_readback_checks':48,
 'no_retries_or_replacements':True,'collected_unix':time.time(),'source_commit':config['source_commit'],'files':inventory}
(job/'COLLECTED.json').write_text(json.dumps(receipt,indent=2)+'\n')
with tarfile.open(job/'COLLECTED.tar.gz','w:gz') as tar:
 for name,p in sorted(selected.items()):tar.add(p,arcname=name,recursive=False)
 tar.add(job/'COLLECTED.json',arcname='COLLECTED.json',recursive=False)
assert all(sha(selected[name])==row['sha256'] for name,row in inventory.items())
print(json.dumps({'files':len(inventory),'original_bytes':sum(r['bytes'] for r in inventory.values()),
 'archive_sha256':sha(job/'COLLECTED.tar.gz'),'archive_bytes':(job/'COLLECTED.tar.gz').stat().st_size,
 'manifest_sha256':sha(job/'COLLECTED.json')}))
