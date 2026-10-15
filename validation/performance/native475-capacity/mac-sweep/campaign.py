from pathlib import Path
import hashlib,json,subprocess,time,sys
job=Path(__file__).resolve().parent
assert sys.argv[1:]==['--authorized-quiet'], 'Rootquietgrantrequired'
assert (job/'READY.json').is_file(), 'Setupnotready'
ready=json.loads((job/'READY.json').read_text())
assert hashlib.sha256((job/'CAMPAIGN_CONFIG.json').read_bytes()).hexdigest()==ready['campaign_config_sha256']
assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==ready['campaign_driver_sha256']
config=json.loads((job/'CAMPAIGN_CONFIG.json').read_text());sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def guards():
 assert all(sha(Path(case['metadata'])/'resources.py')==case['resource_collector_sha256'] for case in config['cases'])
 for variant in ['candidate']:
  c=config[variant];assert sha(c['source_inventory'])==c['source_inventory_sha256'];s=json.loads(Path(c['source_inventory']).read_text());root=Path(c['source_root']);assert s['commit']==c['source_commit'];assert all(sha(root/name)==digest and oct((root/name).stat().st_mode&0o777)==s['modes'][name] for name,digest in s['sha256'].items())
  for name,mode in s['directory_modes'].items():assert oct((root/name).stat().st_mode&0o777)==mode,name
  for a in [c['artifact'],c['root_artifact']]:assert sha(a['copy'])==sha(a['original'])==a['sha256'];assert a['compiler_record']['fresh'] is False
  if 'profile_proof' in c:assert sha(c['profile_proof']['path'])==c['profile_proof']['sha256']
  for proof in c['compiler'].values():assert sha(proof['path'])==proof['sha256']
 for proof in config['setup_dependencies']:assert sha(proof['path'])==proof['sha256']
 assert Path(config['wal_parent']).is_dir()
 tools=config['control_tools'];root=Path(tools['directory']);assert all(sha(root/name)==r['sha256'] and oct((root/name).stat().st_mode&0o777)==r['mode'] for name,r in tools['files'].items())
guards()
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines();assert not any(Path(x.strip()).name in ('cargo','rustc','native_benchmar','native_benchmark','samply','perf','voteboat-matche','voteboat-matched-framework') for x in active)
def host_snapshot(label):
 results=[]
 for cmd in [['/bin/date','-u'],['/usr/bin/pmset','-g','custom'],['/usr/bin/pmset','-g','therm'],['/bin/ps','-axo','pid=,ppid=,state=,%cpu=,%mem=,etime=,comm='],['/bin/df','-P',str(job)],['/sbin/mount'],['/usr/sbin/diskutil','info','-plist','/dev/disk3s1']]:
  p=subprocess.run(cmd,capture_output=True,text=True);results.append({'command':cmd,'exit':p.returncode,'stdout':p.stdout,'stderr':p.stderr})
 (job/(label+'-host.json')).write_text(json.dumps({'scope':'Read-only once outside workload processes; no power/background/settings changes. Three voters share APFS volume and physical store.','commands':results},indent=2)+'\n')
host_snapshot('before')
out=job/'campaign';assert not out.exists(), 'Nooverwrite/restart';out.mkdir();record={'scope':config['scope'],'cases':[]};failed=False
try:
 for case in config['cases']:
  name=case['name'];assert not Path(case['wal_root']).exists();(job/'CAMPAIGN_STAGE').write_text(name+'\n');cmd=case['resource_command'];(out/(name+'.command.json')).write_text(json.dumps(cmd,indent=2)+'\n');print('START',name,flush=True);start=time.time()
  with (out/(name+'.stdout')).open('wb') as stdout,(out/(name+'.stderr')).open('wb') as stderr:run=subprocess.run(cmd,stdout=stdout,stderr=stderr)
  (out/(name+'.exit')).write_text(str(run.returncode)+'\n');record['cases'].append({**case,'start_unix':start,'end_unix':time.time(),'exit':run.returncode});(out/'CAMPAIGN.json').write_text(json.dumps(record,indent=2)+'\n');failed|=run.returncode!=0;print('END',name,run.returncode,flush=True)
 guards()
 host_snapshot('after')
 # AllworkloadprocessesEND beforevalidator/CSVcollection, no failedcase replacement.
 for case in record['cases']:
  if case['exit']:continue
  cmd=['node',str(Path(config['control_tools']['directory'])/'check-native-benchmark.mjs'),'--storage',case['wal_root']];name=case['name']+'-check';(out/(name+'.command.json')).write_text(json.dumps(cmd,indent=2)+'\n');r=subprocess.run(cmd,capture_output=True);(out/(name+'.stdout')).write_bytes(r.stdout);(out/(name+'.stderr')).write_bytes(r.stderr);(out/(name+'.exit')).write_text(str(r.returncode)+'\n');case['validator_exit']=r.returncode;failed|=r.returncode!=0
 (out/'CAMPAIGN.json').write_text(json.dumps(record,indent=2)+'\n');(job/'CAMPAIGN_STAGE').write_text('ALL_TIMING_END\n');(job/'CAMPAIGN_STATUS').write_text(str(int(failed))+'\n')
except BaseException:
 (job/'CAMPAIGN_STATUS').write_text('1\n');raise
raise SystemExit(int(failed))
