from pathlib import Path
import hashlib,json,subprocess,time,sys,csv
job=Path(__file__).resolve().parent
assert sys.argv[1:]==['--authorized-quiet'], 'Root quietauthorization required; setupalone doesnotlaunch'
assert (job/'READY.json').is_file(), 'Artifact/control-tools setup notready'
out=job/'campaign';assert not out.exists(), 'Refuse overwrite/restart';out.mkdir()
config=json.loads((job/'CAMPAIGN_CONFIG.json').read_text());source=json.loads((job/'SOURCE.json').read_text());sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert source['commit']==config['required_source_commit'] and sha(job/'SOURCE.json')==config['source_inventory_sha256']
exe=config['artifact'];binary=Path(exe['copy']);assert sha(binary)==exe['sha256'] and sha(Path(exe['original']))==exe['sha256']
tools=config['control_tools'];toolroot=Path(tools['directory']);assert sha(toolroot/'SOURCE.json')==tools['inventory_sha256']
assert all(sha(toolroot/name)==r['sha256'] and oct((toolroot/name).stat().st_mode & 0o777)==r['mode'] for name,r in tools['files'].items())
assert (job/'STATUS').read_text().strip()=='0' and(job/'STAGE').read_text().strip()=='DONE'
active=subprocess.check_output(['ps','-eo','comm='],text=True).splitlines();assert not any(x.strip() in ('cargo','rustc','native_benchmar','samply','perf','voteboat-matche') for x in active),active
assert all(sha(job/'source'/p)==v and oct((job/'source'/p).stat().st_mode & 0o777)==source['physical_modes'][p] for p,v in source['sha256'].items())
record={'source_commit':source['commit'],'binary_sha256':exe['sha256'],'profile':config['profile'],'scope':'18once serialnativeCounter/window64 configuredpolicy diagnostics; notsustainedacceptance/defaultgain','cases':[]};(out/'CAMPAIGN.json').write_text(json.dumps(record,indent=2)+'\n')
try:
 for case in config['cases']:
  name=case['name'];wal=Path(case['wal_root']);assert not wal.exists()
  command=case['command'];(out/(name+'.command.json')).write_text(json.dumps(command,indent=2)+'\n');(job/'CAMPAIGN_STAGE').write_text(name+'\n');print('START',name,flush=True);started=time.time()
  with (out/(name+'.stdout')).open('w') as stdout,(out/(name+'.stderr')).open('w') as stderr:result=subprocess.run(command,stdout=stdout,stderr=stderr)
  (out/(name+'.exit')).write_text(str(result.returncode)+'\n');record['cases'].append({**case,'start_unix':started,'end_unix':time.time(),'exit':result.returncode});(out/'CAMPAIGN.json').write_text(json.dumps(record,indent=2)+'\n');print('END',name,result.returncode,flush=True)
 assert sha(binary)==exe['sha256'] and sha(Path(exe['original']))==exe['sha256']
 assert all(sha(job/'source'/p)==v and oct((job/'source'/p).stat().st_mode & 0o777)==source['physical_modes'][p] for p,v in source['sha256'].items())
 # All workload processes have terminated before any validator or CSV analysis.
 for case in record['cases']:
  case['checks']=[]
  for label,args in [('receipts',['node',str(toolroot/'check-native-benchmark.mjs'),case['wal_root']]),('publication',['node',str(toolroot/'check-publication-timings.mjs'),str(Path(case['wal_root'])/'publication.csv')])]:
   base=out/(case['name']+'-'+label);Path(str(base)+'.command.json').write_text(json.dumps(args,indent=2)+'\n')
   result=subprocess.run(args,capture_output=True);Path(str(base)+'.stdout').write_bytes(result.stdout);Path(str(base)+'.stderr').write_bytes(result.stderr);Path(str(base)+'.exit').write_text(str(result.returncode)+'\n');case['checks'].append({'name':label,'exit':result.returncode})
  batches=Path(case['wal_root'])/'proposal-batches.csv'
  if batches.is_file():
   rows=list(csv.DictReader(batches.open()));measured=[r for r in rows if r['phase']=='measurement' and int(r['width'])>0]
   case['observed_widths']=[{'replica':r['replica'],'width':int(r['width']),'visits':int(r['visits']),'original_proposals':int(r['original_proposals'])} for r in measured]
   case['proposal_width_bound_ok']=all(int(r['width'])<=case['ready_items'] for r in measured)
   case['proposal_originals']=sum(int(r['original_proposals']) for r in measured)
   case['proposal_complete_ok']=case['proposal_originals']==case['operations']
  else:case['proposal_width_bound_ok']=False;case['proposal_complete_ok']=False
 (out/'CAMPAIGN.json').write_text(json.dumps(record,indent=2)+'\n')
 assert all(sha(toolroot/name)==r['sha256'] for name,r in tools['files'].items())
 failed=any(x['exit'] or any(c['exit'] for c in x['checks']) or not x['proposal_width_bound_ok'] or not x['proposal_complete_ok'] for x in record['cases'])
 (job/'CAMPAIGN_STAGE').write_text('DONE\n');(job/'CAMPAIGN_STATUS').write_text(str(int(failed))+'\n')
except BaseException:
 (job/'CAMPAIGN_STATUS').write_text('1\n');raise
