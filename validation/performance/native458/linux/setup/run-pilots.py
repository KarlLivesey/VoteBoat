from pathlib import Path
import json,hashlib,subprocess,sys
j=Path(__file__).resolve().parent
assert sys.argv[1:]==['--authorized-functional-pilot'], 'Rootpilotquietcoordinationrequired'
c=json.loads((j/'PILOT_CONFIG.json').read_text());h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();a=c['artifact'];assert h(a['copy'])==h(a['original'])==a['sha256'];source=json.loads((j/'SOURCE.json').read_text());assert source['commit']==c['candidate_source_commit'];assert all(h(j/'source'/name)==digest and oct((j/'source'/name).stat().st_mode&0o777)==source['physical_modes'][name] for name,digest in source['sha256'].items())
tools=c['control_tools'];assert h(Path(tools['directory'])/'SOURCE.json')==tools['inventory_sha256'];assert all(h(Path(tools['directory'])/name)==v['sha256'] for name,v in tools['files'].items())
try:
 for case in c['cases']:
  name=case['name'];assert not Path(case['wal_root']).exists(), 'Nooverwrite/retry'
  (j/'controls/STAGE').write_text(name+'\n');print('START',name,flush=True)
  for label,cmd in [('native',case['resource_command']),('validator',case['check_command'])]:
   base=j/'controls'/(name+'-'+label);Path(str(base)+'.command.json').write_text(json.dumps(cmd,indent=2)+'\n')
   with Path(str(base)+'.stdout').open('wb') as out,Path(str(base)+'.stderr').open('wb') as err:run=subprocess.run(cmd,stdout=out,stderr=err)
   Path(str(base)+'.exit').write_text(str(run.returncode)+'\n');assert run.returncode==0,(name,label,run.returncode)
  print('END',name,0,flush=True)
 assert h(a['copy'])==a['sha256'];(j/'controls/STAGE').write_text('DONE\n');(j/'controls/STATUS').write_text('0\n')
except BaseException:
 (j/'controls/STATUS').write_text('1\n');raise
