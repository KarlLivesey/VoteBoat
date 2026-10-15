import collections,hashlib,json,sys,shutil
from pathlib import Path
b=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(b/'common'))
from plan import cases
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
rows={}
for host,job in [('linux',b/'linux/revision2/campaign'),('mac',b/'mac/prepared/campaign')]:
 c=json.loads((job/'CAMPAIGN_CONFIG.json').read_text());r=json.loads((job/'READY.json').read_text());setup=json.loads((job/'setup-host.json').read_text())
 assert sha(job/'CAMPAIGN_CONFIG.json')==r['campaign_config_sha256']
 assert c['device_identity']==setup['device_identity']and c['cpu_policy']==setup['cpu_policy']
 assert c['source_commits']=={'baseline':'f7b0ef12709bb7e7aad4f0279851aa035b508e9e','candidate':'efd383dc36b4b9edf8f66021b90115824440b468'}
 bins={arm:next(a['copy']for a in x['artifacts']if a['target']=='native_benchmark')for arm,x in c['candidates'].items()}
 assert c['cases']==cases(bins,Path(c['wal_parent']),Path(c['job'])/'campaign-metadata',c['python'],Path(c['job'])/'resources.py')
 assert len(c['cases'])==48 and sum(x['offers']for x in c['cases'])==391680 and sum(x['horizon_seconds']for x in c['cases'])==1344
 assert len(c['validators'])==8 and c['existing_native_drain_guard_seconds']==120 and c['all_workloads_end_before_validation']is True
 assert not (job/'STARTED.json').exists()and not(job/'campaign').exists()
 assert (job/'ORDER.txt').read_text().splitlines()==[x['name']for x in c['cases']]
 for name,digest in c['scripts'].items():assert sha(job/name)==digest==sha(b/'common'/name)
 for name,digest in c['validators'].items():assert sha(job/'control-tools'/name)==digest
 for left,right in zip(c['cases'][::2],c['cases'][1::2]):
  assert left['pair']==right['pair']and{left['arm'],right['arm']}=={'baseline','candidate'}
  for key in ['trial','cell','protocol','offers','rate','window','groups','weights','ready_items']:assert left[key]==right[key]
  assert left['publication']==right['publication']=='witness'
  for x in [left,right]:
   assert x['resource_command']==[c['python'],c['job']+'/resources.py',x['metadata'],*x['command']]
   assert x['command'][x['command'].index('--maintenance')+1]=='2'
   assert x['command'][x['command'].index('--offered-capacity')+1]=='4096'
 assert collections.Counter(x['arm']for x in c['cases'][::2])=={'baseline':12,'candidate':12}
 rows[host]={'config':str(job/'CAMPAIGN_CONFIG.json'),'remote_job':c['job'],'config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),'ready_sha256':sha(job/'READY.json'),'setup_sha256':sha(job/'setup-host.json'),'scripts':c['scripts'],'cases':48,'pairs':24,'offers':391680,'scheduled_horizons_s':1344,'validators':8,'launch':[c['python'],c['job']+'/campaign.py','--authorized-quiet'],'source_commits':c['source_commits'],'artifacts':r['artifacts_sha256'],'device_identity':c['device_identity'],'cpu_policy':c['cpu_policy']}
assert rows['linux']['scripts']==rows['mac']['scripts']
old=Path('validation/performance/native484-ingress/preparation/comparison/common')
assert sha(old/'common.py')=='54e0e59b7268a17bc16e3879ec56cf986441ef1961ed0e27fd294434ee34eb41'
for name in rows['linux']['scripts']:
 if name!='common.py':assert sha(old/name)==sha(b/'common'/name)
result={'actual_exit':0,'hosts':rows,'schema_sha256':sha(b/'SCHEMA.md'),'timing_started':False,'scope':'Prepared configs reviewed only; actual source/artifact/tool guards ran on each host during setup. Requires independent review, publication, and separate root quiet GO.'}
(b/'final/CONFIG_CHECK.json').write_text(json.dumps(result,indent=2)+'\n')
print('CONFIG_CHECK_PASS both hosts48 each, exact fixed matrix, adjacent rotation, actual fresh proof/tool/source bindings, no timing')
