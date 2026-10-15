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
 for variant in ['baseline','candidate']:
  c=config[variant];assert sha(c['source_inventory'])==c['source_inventory_sha256'];s=json.loads(Path(c['source_inventory']).read_text());root=Path(s['source_root']);assert s['commit']==c['source_commit'];assert all(sha(root/name)==digest and oct((root/name).stat().st_mode&0o777)==s['physical_modes'][name] for name,digest in s['sha256'].items())
  for a in [c['artifact'],c['root_artifact']]:assert sha(a['copy'])==sha(a['original'])==a['sha256'];assert a['compiler_record']['fresh'] is False
  if 'profile_proof' in c:assert sha(c['profile_proof']['path'])==c['profile_proof']['sha256']
  for proof in c['compiler'].values():assert sha(proof['path'])==proof['sha256']
 tools=config['control_tools'];root=Path(tools['directory']);assert sha(root/'SOURCE.json')==tools['inventory_sha256'];assert all(sha(root/name)==r['sha256'] and oct((root/name).stat().st_mode&0o777)==r['mode'] for name,r in tools['files'].items())
 for proof in config['setup_dependencies']:assert sha(proof['path'])==proof['sha256']
 assert Path(config['wal_parent']).is_dir()
guards()
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines();assert not any(x.strip() in ('cargo','rustc','native_benchmar','samply','perf','voteboat-matche') for x in active)
out=job/'campaign';assert not out.exists(), 'Nooverwrite/restart';out.mkdir();record={'scope':config['scope'],'cases':[]};failed=False
try:
 for case in config['cases']:
  name=case['name'];assert not Path(case['wal_root']).exists();(job/'CAMPAIGN_STAGE').write_text(name+'\n');cmd=case['resource_command'];(out/(name+'.command.json')).write_text(json.dumps(cmd,indent=2)+'\n');print('START',name,flush=True);start=time.time()
  with (out/(name+'.stdout')).open('wb') as stdout,(out/(name+'.stderr')).open('wb') as stderr:run=subprocess.run(cmd,stdout=stdout,stderr=stderr)
  (out/(name+'.exit')).write_text(str(run.returncode)+'\n');record['cases'].append({**case,'start_unix':start,'end_unix':time.time(),'exit':run.returncode});(out/'CAMPAIGN.json').write_text(json.dumps(record,indent=2)+'\n');failed|=run.returncode!=0;print('END',name,run.returncode,flush=True)
 guards()
 (job/'CAMPAIGN_STAGE').write_text('ALL_WORKLOADS_END\n')
 # AllworkloadprocessesEND beforevalidator/CSVcollection, no failedcase replacement.
 for case in record['cases']:
  if case['exit']:continue
  case['validator_exits']=[]
  for index,cmd in enumerate(case['validator_commands']):
   name=case['name']+'-check-'+str(index);(out/(name+'.command.json')).write_text(json.dumps(cmd,indent=2)+'\n');r=subprocess.run(cmd,capture_output=True);(out/(name+'.stdout')).write_bytes(r.stdout);(out/(name+'.stderr')).write_bytes(r.stderr);(out/(name+'.exit')).write_text(str(r.returncode)+'\n');case['validator_exits'].append(r.returncode);failed|=r.returncode!=0
  case['validator_exit']=max(case['validator_exits'])
 (out/'CAMPAIGN.json').write_text(json.dumps(record,indent=2)+'\n');(job/'CAMPAIGN_STAGE').write_text('ALL_TIMING_END\n');(job/'CAMPAIGN_STATUS').write_text(str(int(failed))+'\n')
except BaseException:
 (job/'CAMPAIGN_STATUS').write_text('1\n');raise
raise SystemExit(int(failed))
