"""Post-terminal481 raw recomputation only. Never launches or contacts a host.

Invoke only after root both-host END/GO. --linux/--mac select collected roots;
defaults match the prepared layout. Exact rates use completed originals, with
scheduled-horizon rates separate. Failed cases/pairs remain explicit.
"""
import argparse,collections,csv,hashlib,json,statistics,traceback
from pathlib import Path

BASE=Path(__file__).resolve().parent
CELLS=[('singleton',16,256,8,1,1,None),('moderate',16,8192,128,256,1,None),('loaded',64,16000,2000,4096,1,None),('skew',64,8192,1024,4096,2,'31,1')]
COMMITS={'baseline':'7e9068296ce00dde5bf3e03bf4d852ed2833d185','candidate':'f7b0ef12709bb7e7aad4f0279851aa035b508e9e'}
inputs={}
def read(path):
 data=path.read_bytes();inputs[str(path)]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)};return data.decode()
def load(path):return json.loads(read(path))
def rows(path):return list(csv.DictReader(read(path).splitlines()))
def settings(path):return dict(token.split('=',1)for token in read(path).split())
def quantile(values,p):return sorted(values)[(len(values)*p+99)//100-1]/1e6 if values else None
def histogram(value):return {int(k):int(v)for k,v in (item.split(':')for item in value.split('|')if item)}
def distribution(values):return {**{f'p{p}_ms':quantile(values,p)for p in [50,95,99]},'max_ms':max(values)/1e6 if values else None}
def cohort(offers,horizon,elapsed):
 applied=[r for r in offers if r['status']=='applied'];admitted=[r for r in offers if r['sequence']>0]
 status=collections.Counter(r['status']for r in offers)
 return {'offers':len(offers),'applied':len(applied),'statuses':dict(status),'refused':sum(v for k,v in status.items()if k!='applied'),
  'completed_ops_s':len(applied)*1e9/elapsed,'during_ops_s':sum(r['completed_ns']<horizon for r in applied)*1e9/horizon,
  'backlog':sum(r['dispatched_ns']<horizon<=r['completed_ns']for r in admitted),
  **distribution([r['completed_ns']-r['intended_ns']for r in applied]),
  'service_latency':distribution([r['completed_ns']-r['dispatched_ns']for r in applied]),
  'dispatch_latency':distribution([r['dispatched_ns']-r['intended_ns']for r in offers])}
def expected_matrix():
 result=[]
 for trial in range(3):
  for index in list(range(4))[trial:]+list(range(4))[:trial]:
   cell,quantum,offers,rate,window,groups,weights=CELLS[index]
   for protocol in (['tcp','quic']if(trial+index)%2==0 else['quic','tcp']):
    first='baseline'if(trial+index+['tcp','quic'].index(protocol))%2==0 else'candidate'
    for arm in [first,'candidate'if first=='baseline'else'baseline']:
     result.append((trial+1,cell,protocol,arm,quantum,offers,rate,window,groups,weights))
 return result
def analyze_case(host,root,c,artifact):
 assert c['exit']==c['validator_exit']==c['readbacks_exit']==0
 wal=Path(c['wal_root'])if host=='linux'else root/'results'/c['name']
 argv=[artifact['copy'],c['wal_root'],c['protocol'],str(c['offers']),str(c['window']),str(c['groups']),'--offered',str(c['rate']),'--maintenance','2','--ready-items',str(c['ready_items']),'--offered-capacity','4096','--publication','witness']
 if c['weights']:argv+=['--group-weights',c['weights']]
 assert c['command']==argv==load(root/'campaign'/(c['name']+'.native-command.json'))
 assert c['resource_command'][3:]==argv
 summary=settings(wal/'summary.txt');run=settings(wal/'run.txt')
 for key,value in [('offers',c['offers']),('offered_rate',c['rate']),('window',c['window']),('groups',c['groups'])]:assert summary[key]==run[key]==str(value)
 assert summary['ready_visit_items_requested']==summary['ready_visit_items_effective']==str(c['ready_items'])
 assert summary['core_proposal_entries']=='64'and summary['ready_visit_bytes']=='1048576'and summary['ready_visit_ms']=='2'
 assert summary['publication_effective']==run['publication_effective']=='witness'
 assert summary['leader_placement']==','.join(f'{g}:1'for g in range(1,c['groups']+1))
 if c['weights']:assert summary['workload_group_weights']==run['workload_group_weights']=='31,1'
 offers=rows(wal/'offers.csv');assert len(offers)==c['offers']
 horizon=int(summary['horizon_ns']);elapsed=int(summary['elapsed_ns']);assert horizon==c['offers']*1_000_000_000//c['rate']and elapsed>=horizon
 for i,r in enumerate(offers):
  for key in r:
   if key not in ['status','reason']:r[key]=int(r[key])
  assert r['operation']==65+i and r['intended_ns']==i*1_000_000_000//c['rate']
  assert r['group']==(2 if c['weights']and(r['operation']-1)%32==31 else 1)
  assert 0<=r['intended_ns']<=r['dispatched_ns']<=r['completed_ns']<=elapsed
 total=cohort(offers,horizon,elapsed)
 assert total['applied']==int(summary['applied'])==int(summary['admitted'])
 assert int(summary['recovered_value'])==64+total['applied']and summary['retry_verified']==summary['workers_joined']=='true'
 for status in ['applied','window_refused','no_leader','admission_refused','not_proposed']:assert total['statuses'].get(status,0)==int(summary[status])
 assert set(total['statuses'])<={'applied','window_refused'}and all(summary[k]=='0'for k in ['unknown','pending','no_leader','admission_refused','not_proposed'])
 assert abs(total['p99_ms']-float(summary['p99_us'])/1000)<.0000011
 assert total['backlog']==int(summary['backlog_at_horizon'])
 groups={g:cohort([r for r in offers if r['group']==g],horizon,elapsed)for g in range(1,c['groups']+1)}
 if c['weights']:assert groups[1]['offers']==7936 and groups[2]['offers']==256
 events=[];local=collections.Counter();max_local=collections.Counter();live=maximum=0
 for i,r in enumerate(offers):
  events.append((r['dispatched_ns'],0,i,r))
  if r['sequence']>0:events.append((r['completed_ns'],-1,i,r))
 for _,kind,_,r in sorted(events,key=lambda event:event[:3]):
  group=r['group']
  if kind==-1:live-=1;local[group]-=1
  elif r['status']=='window_refused':assert live>=c['window']or local[group]>=c['group_pending_ceiling']
  elif r['sequence']>0:live+=1;local[group]+=1
  assert 0<=live<=c['window']and 0<=local[group]<=c['group_pending_ceiling']
  maximum=max(maximum,live);max_local[group]=max(max_local[group],local[group])
 assert live==0 and maximum==int(summary['max_inflight'])
 for group in groups:groups[group]['max_pending']=max_local[group]
 storage=rows(wal/'storage.csv');physical={};primitive={}
 fields=['append_calls','append_units','append_commands','append_ns','barrier_calls','barrier_tickets','barrier_ns','errors','encoded_bytes','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']
 for replica in [1,2,3]:
  before=next(r for r in storage if r['stage']=='before_measurement'and int(r['replica'])==replica);after=next(r for r in storage if r['stage']=='after_measurement'and int(r['replica'])==replica)
  delta={k:int(after[k])-int(before[k])for k in fields};assert min(delta.values())>=0 and delta['errors']==0
  for field,name in [('batch_histogram','append_units_per_call_histogram'),('group_units','group_units')]:
   old=histogram(before[field]);new=histogram(after[field]);delta[name]={k:n-old.get(k,0)for k,n in new.items()}
  assert sum(delta['append_units_per_call_histogram'].values())==delta['append_calls']
  assert sum(k*v for k,v in delta['append_units_per_call_histogram'].items())==delta['append_units']
  physical[replica]=delta
 publication=rows(wal/'publication.csv')
 for replica in [1,2,3]:
  primitive[replica]={}
  for step in {r['step']for r in publication}:
   before=next(r for r in publication if r['stage']=='before_measurement'and int(r['replica'])==replica and r['step']==step);after=next(r for r in publication if r['stage']=='after_measurement'and int(r['replica'])==replica and r['step']==step)
   delta={k:int(after[k])-int(before[k])for k in ['calls','errors','elapsed_ns']};assert min(delta.values())>=0 and delta['errors']==0
   delta['cumulative_max_ns_before']=int(before['max_ns']);delta['cumulative_max_ns_after']=int(after['max_ns']);primitive[replica][step]=delta
 maintenance=rows(wal/'maintenance.csv');read(wal/'bases.csv');cold={}
 for kind in {r['kind']for r in maintenance}:
  selected=[r for r in maintenance if r['kind']==kind];times=[int(r['completed_ns'])-int(r['submitted_ns'])for r in selected if r['status']=='completed']
  cold[kind]={'statuses':dict(collections.Counter(r['status']for r in selected)),'completed':len(times),'inclusive_sum_ms':sum(times)/1e6,**distribution(times)}
 resource=load(root/'campaign-metadata'/c['name']/'RESOURCES.json');assert resource['exit']==resource['helper_exit']==0
 leader=physical[1]
 return {**{key:c[key]for key in ['name','trial','cell','protocol','arm','ready_items']},'host':host,'valid':True,**total,'groups':groups,
  'max_pending':maximum,'drain_ms':(elapsed-horizon)/1e6,'late_decisions':int(summary['late_decisions']),
  'host_poll_ms':float(summary['host_poll_ms']),'max_host_poll_ms':float(summary['max_host_poll_ms']),'host_poll_wall_fraction':float(summary['host_poll_ms'])/(elapsed/1e6),
  'physical':physical,'publication':primitive,'maintenance':cold,'leader_barriers_per_original':leader['barrier_calls']/total['applied'],
  'leader_commands_per_append_unit':leader['append_commands']/leader['append_units'],'leader_mean_barrier_ms':leader['barrier_ns']/leader['barrier_calls']/1e6,
  'leader_barrier_wall_fraction':leader['barrier_ns']/elapsed,'wholeprocess_cpu_core_equivalent':(resource['user_seconds']+resource['system_seconds'])/resource['elapsed_seconds'],
  'max_rss_bytes':resource['max_rss_bytes'],'checkpoints':int(summary['checkpoints']),'reclaims':int(summary['reclaims']),'recovery_retries':int(summary['recovery_retries'])}

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--linux',type=Path,default=BASE/'linux');parser.add_argument('--mac',type=Path,default=BASE/'mac-collection/raw');parser.add_argument('--authorized-postterminal',action='store_true',required=True);args=parser.parse_args()
 records=[];failures=[];identities={}
 # No input is opened before explicit postterminal invocation. Root grants this
 # only after BOTH original jobs terminate; check both durable terminals first.
 roots={'linux':args.linux,'mac':args.mac}
 terminals={host:load(root/'TERMINAL.json')for host,root in roots.items()}
 assert all(t['all_timing_ended']for t in terminals.values()),'Never analyze partial timing'
 for host,root in roots.items():
  config=load(root/'CAMPAIGN_CONFIG.json');ready=load(root/'READY.json');assert inputs[str(root/'CAMPAIGN_CONFIG.json')]['sha256']==ready['campaign_config_sha256']
  assert config['source_commits']==COMMITS
  assert hashlib.sha256(read(root/'SCHEMA.md').encode()).hexdigest()==config['schema_sha256']
  for name,digest in config['scripts'].items():assert hashlib.sha256(read(root/name).encode()).hexdigest()==digest
  for name,digest in config['validators'].items():assert hashlib.sha256(read(root/'control-tools'/name).encode()).hexdigest()==digest
  artifacts={arm:next(a for a in selected['artifacts']if a['target']=='native_benchmark')for arm,selected in config['candidates'].items()}
  identities[host]={'source_commits':config['source_commits'],'artifacts':{arm:{'sha256':a['sha256'],'profile':a['compiler_record']['profile']}for arm,a in artifacts.items()},'config_sha256':ready['campaign_config_sha256']}
  assert {arm:a['sha256']for arm,a in artifacts.items()}==ready['artifacts_sha256']
  assert artifacts['baseline']['compiler_record']['profile']==artifacts['candidate']['compiler_record']['profile']
  assert sorted(t['sha256']for t in config['candidates']['baseline']['compiler_tools'])==sorted(t['sha256']for t in config['candidates']['candidate']['compiler_tools'])
  campaign=load(root/'campaign/CAMPAIGN.json');frozen=config['cases'];expected=expected_matrix()
  assert len(frozen)==48 and sum(c['horizon_seconds']for c in frozen)==1344 and sum(c['offers']for c in frozen)==391680
  for c,want in zip(frozen,expected,strict=True):assert tuple(c[k]for k in ['trial','cell','protocol','arm','ready_items','offers','rate','window','groups','weights'])==want
  actual={c['name']:c for c in campaign['cases']}
  if terminals[host]['exit']or campaign['post_timing_errors']:failures.append({'host':host,'terminal':terminals[host],'post_timing_errors':campaign['post_timing_errors']})
  for declaration in frozen:
   c=actual.get(declaration['name'])
   try:
    assert c is not None,'Declared case absent from original terminal ledger'
    assert all(c[k]==v for k,v in declaration.items());assert c['resource_command'][0]==config['python']
    records.append(analyze_case(host,root,c,artifacts[c['arm']]))
   except Exception:
    failure={'host':host,'case':declaration['name'],'original':c,'error':traceback.format_exc()};failures.append(failure)
    records.append({**{k:declaration[k]for k in ['name','trial','cell','protocol','arm','ready_items']},'host':host,'valid':False,'error':failure['error']})
 cells=[]
 keys=['completed_ops_s','during_ops_s','p99_ms','max_ms','refused','backlog','max_pending','max_host_poll_ms','host_poll_ms','leader_barriers_per_original','leader_mean_barrier_ms','leader_barrier_wall_fraction','wholeprocess_cpu_core_equivalent','max_rss_bytes']
 for host in roots:
  for protocol in ['tcp','quic']:
   for cell,*_ in CELLS:
    selected=[r for r in records if(r['host'],r['protocol'],r['cell'])==(host,protocol,cell)];arms={}
    for arm in ['baseline','candidate']:
     valid=sorted([r for r in selected if r['arm']==arm and r['valid']],key=lambda r:r['trial'])
     arms[arm]={k:{'median':statistics.median(r[k]for r in valid)if valid else None,'trials':[{ 'trial':r['trial'],'value':r[k]}for r in valid]}for k in keys}
    pairs=[]
    for trial in [1,2,3]:
     a=next(r for r in selected if r['trial']==trial and r['arm']=='baseline');b=next(r for r in selected if r['trial']==trial and r['arm']=='candidate')
     pair={'trial':trial,'baseline':a['name'],'candidate':b['name'],'both_valid':a['valid']and b['valid']}
     if pair['both_valid']:
      pair.update({k+'_ratio':b[k]/a[k]if a[k]else None for k in ['completed_ops_s','during_ops_s','p99_ms','max_ms','leader_barriers_per_original','max_host_poll_ms']})
      pair['refusal_delta']=b['refused']-a['refused'];pair['cold']=({'baseline':a['groups'][2],'candidate':b['groups'][2],'p99_ratio':b['groups'][2]['p99_ms']/a['groups'][2]['p99_ms']}if cell=='skew'else None)
     pairs.append(pair)
    cells.append({'host':host,'protocol':protocol,'cell':cell,'arms':arms,'pairs':pairs})
 output={'exit':int(bool(failures)),'scope':'Independent481 post-both-terminal analysis. Source/artifact current bytes are guarded by original before/after controller checks; this reader independently binds frozen identities/argv/script+validator closure and raw arithmetic. Inclusive elapsed scopes overlap and are not CPU. Physical histogram counts append units, not literal proposal width. Publication scope excludes cold namespace work. Resources cover whole launch/recovery/cleanup. Finite offered diagnostics, not fullP9/sustained/closed-loop acceptance.','identities':identities,'terminals':terminals,'inputs':inputs,'records':records,'cells':cells,'failures':failures}
 (BASE/'independent-readout.json').write_text(json.dumps(output,indent=2)+'\n')
 for c in cells:
  print(c['host'],c['protocol'],c['cell'],json.dumps({arm:{k:c['arms'][arm][k]['median']for k in ['completed_ops_s','during_ops_s','p99_ms','refused','leader_barriers_per_original']}for arm in ['baseline','candidate']}))
 print('ANALYSIS_EXIT',output['exit'],'records',len(records),'failures',len(failures));return output['exit']
if __name__=='__main__':raise SystemExit(main())
