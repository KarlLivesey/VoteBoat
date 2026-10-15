"""Independent post-terminal 478 ledger arithmetic. No workloads or subprocesses."""
import collections,csv,hashlib,json,statistics
from pathlib import Path
BASE=Path(__file__).resolve().parent
inputs={};records=[]
def read(p):
    inputs[str(p)]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
    return p.read_text()
def load(p):return json.loads(read(p))
def pct(v,p):return sorted(v)[(len(v)*p+99)//100-1]/1e6 if v else None
def hist(v):return {int(k):int(n)for k,n in (s.split(':')for s in v.split('|')if s)}
def stats(rows,horizon,elapsed):
    applied=[r for r in rows if r['status']=='applied'];admitted=[r for r in rows if r['sequence']>0]
    lat=[r['completed_ns']-r['intended_ns']for r in applied]
    return {'offers':len(rows),'applied':len(applied),'statuses':dict(collections.Counter(r['status']for r in rows)),
      'completed_ops_s':len(applied)*1e9/elapsed,'during_ops_s':sum(r['completed_ns']<horizon for r in applied)*1e9/horizon,
      'p50_ms':pct(lat,50),'p95_ms':pct(lat,95),'p99_ms':pct(lat,99),'max_ms':max(lat)/1e6 if lat else None,
      'service_p99_ms':pct([r['completed_ns']-r['dispatched_ns']for r in applied],99),
      'dispatch_p99_ms':pct([r['dispatched_ns']-r['intended_ns']for r in rows],99),
      'backlog':sum(r['dispatched_ns']<horizon<=r['completed_ns']for r in admitted)}
cellspec=[('singleton',256,8,1,1,None),('moderate',8192,128,256,1,None),('bulk',8192,1024,4096,1,None),('skew',8192,1024,4096,2,'31,1')]
for host,root in [('linux',BASE/'linux'),('mac',BASE/'mac-collection/raw')]:
    terminal=load(root/'TERMINAL.json');assert terminal['exit']==0 and terminal['all_timing_ended'] and terminal['cases_terminated']==48
    config=load(root/'CAMPAIGN_CONFIG.json');ready=load(root/'READY.json')
    assert inputs[str(root/'CAMPAIGN_CONFIG.json')]['sha256']==ready['campaign_config_sha256']
    assert config['source_commit']=='7e9068296ce00dde5bf3e03bf4d852ed2833d185'
    artifact=next(a for a in config['candidate']['artifacts']if a['target']=='native_benchmark')
    assert artifact['sha256']=={'linux':'0bf40f6b4547c446f9db2a86a4bffe6d9ab223640d470154ec266a0d7cc2d064','mac':'2fd48262a655debafaf7f04496bf34c867dc17b92f1e5890969819333cbcae84'}[host]
    for name,digest in config['scripts'].items():assert hashlib.sha256(read(root/name).encode()).hexdigest()==digest
    for name,digest in config['validators'].items():assert hashlib.sha256(read(root/'control-tools'/name).encode()).hexdigest()==digest
    assert inputs[str(root/'SCHEMA.md')]['sha256']==config['schema_sha256'] if str(root/'SCHEMA.md') in inputs else hashlib.sha256(read(root/'SCHEMA.md').encode()).hexdigest()==config['schema_sha256']
    campaign=load(root/'campaign/CAMPAIGN.json');assert len(campaign['cases'])==48 and campaign['post_timing_errors']==[]
    expected=[]
    for trial in range(3):
      for ci in list(range(4))[trial:]+list(range(4))[:trial]:
       cell,offers,rate,window,groups,weights=cellspec[ci]
       for protocol in (['tcp','quic']if (trial+ci)%2==0 else ['quic','tcp']):
        first=16 if (trial+ci+['tcp','quic'].index(protocol))%2==0 else 64
        for quantum in [first,80-first]:expected.append((trial+1,cell,protocol,quantum,offers,rate,window,groups,weights))
    for c,frozen,want in zip(campaign['cases'],config['cases'],expected,strict=True):
      assert all(c[k]==v for k,v in frozen.items());assert c['exit']==c['validator_exit']==c['readbacks_exit']==0
      assert tuple(c[k]for k in ['trial','cell','protocol','ready_items','offers','rate','window','groups','weights'])==want
      argv=[artifact['copy'],c['wal_root'],c['protocol'],str(c['offers']),str(c['window']),str(c['groups']),'--offered',str(c['rate']),'--maintenance','2','--ready-items',str(c['ready_items']),'--offered-capacity','4096','--publication','witness']
      if c['weights']:argv+=['--group-weights',c['weights']]
      assert c['command']==argv==load(root/'campaign'/(c['name']+'.native-command.json'))
      assert c['resource_command'][0]==config['python'] and c['resource_command'][3:]==argv
      wal=Path(c['wal_root'])if host=='linux'else root/'results'/c['name']
      s=dict(t.split('=',1)for t in read(wal/'summary.txt').split());run=dict(t.split('=',1)for t in read(wal/'run.txt').split())
      assert s['ready_visit_items_requested']==s['ready_visit_items_effective']==str(c['ready_items'])
      assert s['core_proposal_entries']=='64' and s['ready_visit_bytes']=='1048576' and s['ready_visit_ms']=='2'
      assert s['publication_effective']==run['publication_effective']=='witness'
      assert s['leader_placement']==','.join(f'{g}:1'for g in range(1,c['groups']+1))
      rows=list(csv.DictReader(read(wal/'offers.csv').splitlines()))
      for i,r in enumerate(rows):
       for k in r:
        if k not in ['status','reason']:r[k]=int(r[k])
       assert r['operation']==65+i and r['intended_ns']==i*1_000_000_000//c['rate']
       assert r['group']==(2 if c['weights'] and (r['operation']-1)%32==31 else 1)
       assert r['intended_ns']<=r['dispatched_ns']<=r['completed_ns']<=int(s['elapsed_ns'])
      horizon=int(s['horizon_ns']);elapsed=int(s['elapsed_ns']);assert horizon==c['offers']*1_000_000_000//c['rate']
      total=stats(rows,horizon,elapsed);assert total['offers']==c['offers'] and set(total['statuses'])<={'applied','window_refused'}
      for k,n in total['statuses'].items():assert int(s[k])==n
      assert total['applied']==int(s['applied'])==int(s['admitted']) and int(s['recovered_value'])==total['applied']+64
      assert s['retry_verified']==s['workers_joined']=='true' and all(s[k]=='0'for k in ['unknown','pending','no_leader','admission_refused','not_proposed'])
      assert abs(total['p99_ms']-float(s['p99_us'])/1000)<.0000011 and total['backlog']==int(s['backlog_at_horizon'])
      pergroup={g:stats([r for r in rows if r['group']==g],horizon,elapsed)for g in range(1,c['groups']+1)}
      events=[]
      for i,r in enumerate(rows):
       events.append((r['dispatched_ns'],0,i,r))
       if r['sequence']>0:events.append((r['completed_ns'],-1,i,r))
      live=0;local=collections.Counter();maximum=0
      for _,kind,_,r in sorted(events,key=lambda e:e[:3]):
       if kind==-1:live-=1;local[r['group']]-=1
       elif r['status']=='window_refused':assert live>=c['window']or local[r['group']]>=c['group_pending_ceiling']
       elif r['sequence']>0:live+=1;local[r['group']]+=1
       assert 0<=live<=c['window']and 0<=local[r['group']]<=c['group_pending_ceiling'];maximum=max(maximum,live)
      assert live==0 and maximum==int(s['max_inflight'])
      storage=list(csv.DictReader(read(wal/'storage.csv').splitlines()));deltas={}
      fields=['append_calls','append_units','append_commands','append_ns','barrier_calls','barrier_tickets','barrier_ns','errors','encoded_bytes','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']
      for replica in [1,2,3]:
       before=next(r for r in storage if r['stage']=='before_measurement'and int(r['replica'])==replica);after=next(r for r in storage if r['stage']=='after_measurement'and int(r['replica'])==replica)
       d={k:int(after[k])-int(before[k])for k in fields};assert all(v>=0 for v in d.values())and d['errors']==0
       h0=hist(before['batch_histogram']);h1=hist(after['batch_histogram']);d['append_units_per_call_histogram']={k:v-h0.get(k,0)for k,v in h1.items()}
       assert sum(d['append_units_per_call_histogram'].values())==d['append_calls']
       assert sum(k*v for k,v in d['append_units_per_call_histogram'].items())==d['append_units']
       g0=hist(before['group_units']);g1=hist(after['group_units']);d['group_units']={k:v-g0.get(k,0)for k,v in g1.items()}
       deltas[replica]=d
      leader=deltas[1];resource=load(root/'campaign-metadata'/c['name']/'RESOURCES.json');assert resource['exit']==resource['helper_exit']==0
      record={**{k:c[k]for k in ['name','trial','cell','protocol','ready_items']},'host':host,**total,'groups':pergroup,'max_pending':maximum,'drain_ms':(elapsed-horizon)/1e6,'late_decisions':int(s['late_decisions']),
       'host_poll_ms':float(s['host_poll_ms']),'max_host_poll_ms':float(s['max_host_poll_ms']),'host_poll_wall_fraction':float(s['host_poll_ms'])/(elapsed/1e6),
       'physical':deltas,'leader_barriers_per_original':leader['barrier_calls']/total['applied'],'leader_commands_per_append_unit':leader['append_commands']/leader['append_units'],
       'leader_mean_barrier_ms':leader['barrier_ns']/leader['barrier_calls']/1e6,'leader_barrier_wall_fraction':leader['barrier_ns']/elapsed,
       'wholeprocess_cpu_core_equivalent':(resource['user_seconds']+resource['system_seconds'])/resource['elapsed_seconds'],'max_rss_bytes':resource['max_rss_bytes'],
       'checkpoints':int(s['checkpoints']),'reclaims':int(s['reclaims']),'recovery_retries':int(s['recovery_retries'])}
      records.append(record)
cells=[]
keys=['completed_ops_s','during_ops_s','p99_ms','max_ms','max_pending','backlog','drain_ms','host_poll_ms','max_host_poll_ms','host_poll_wall_fraction','leader_barriers_per_original','leader_mean_barrier_ms','leader_commands_per_append_unit','leader_barrier_wall_fraction','wholeprocess_cpu_core_equivalent','max_rss_bytes']
for host in ['linux','mac']:
 for protocol in ['tcp','quic']:
  for cell,*_ in cellspec:
   selected=[r for r in records if(r['host'],r['protocol'],r['cell'])==(host,protocol,cell)]
   arms={str(q):{k:{'median':statistics.median(r[k]for r in selected if r['ready_items']==q),'trials':[r[k]for r in selected if r['ready_items']==q]}for k in keys}for q in [16,64]}
   pairs=[]
   for t in [1,2,3]:
    a=next(r for r in selected if r['trial']==t and r['ready_items']==16);b=next(r for r in selected if r['trial']==t and r['ready_items']==64)
    pairs.append({'trial':t,'completed_ratio':b['completed_ops_s']/a['completed_ops_s'],'during_ratio':b['during_ops_s']/a['during_ops_s'],'p99_ratio':b['p99_ms']/a['p99_ms'],'q16':a['name'],'q64':b['name']})
   cells.append({'host':host,'protocol':protocol,'cell':cell,'arms':arms,'pairs':pairs})
output={'scope':'Independent478 raw recomputation. Physical histogram counts append units, not command proposal width; metadata transitions remain included. Inclusive elapsed scopes may overlap and are not CPU. CPU/RSS is whole child process including recovery/cleanup, with a different boundary. Finite offered diagnostics, not fullP9/sustained/closed-loop acceptance.','inputs':inputs,'records':records,'cells':cells}
(BASE/'independent-readout.json').write_text(json.dumps(output,indent=2)+'\n')
for c in cells:
 print(c['host'],c['protocol'],c['cell'],json.dumps({q:{k:round(c['arms'][q][k]['median'],3)for k in ['completed_ops_s','during_ops_s','p99_ms','host_poll_ms','max_host_poll_ms','leader_barriers_per_original','leader_barrier_wall_fraction','wholeprocess_cpu_core_equivalent']}for q in ['16','64']}))
