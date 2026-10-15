"""Independent raw484 numeric review; standard library only, never invokes a workload."""
import argparse,collections,csv,hashlib,io,json,math,statistics,sys
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--host',required=True);p.add_argument('--job',required=True);p.add_argument('--readout',required=True);p.add_argument('--case-root');p.add_argument('--resource-root');p.add_argument('--output',required=True);args=p.parse_args()
inputs={}
def data(path):
 path=Path(path);raw=path.read_bytes();inputs[str(path)]={'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)};return raw.decode()
def load(path):return json.loads(data(path))
def table(path):return list(csv.DictReader(io.StringIO(data(path))))
def close(a,b):assert math.isclose(a,b,rel_tol=1e-11,abs_tol=1e-8),(a,b)
def q(values):
 values=sorted(values);return {k:values[max(0,math.ceil(len(values)*v)-1)]/1e6 if values else None for k,v in [('p50_ms',.5),('p95_ms',.95),('p99_ms',.99),('max_ms',1)]}|{'count':len(values)}
def distributions(rows):
 applied=[r for r in rows if r['status']=='applied'];return {'intended':q([r['completed_ns']-r['intended_ns']for r in applied]),'service':q([r['completed_ns']-r['dispatched_ns']for r in applied]),'dispatch':q([r['dispatched_ns']-r['intended_ns']for r in rows])}
def exact_q(a,b):
 assert a.keys()==b.keys()
 for kind in a:
  assert a[kind].keys()==b[kind].keys()
  for key,v in a[kind].items():
   if v is None:assert b[kind][key]is None
   else:close(v,b[kind][key])
def hist(s):return {k:int(v)for k,v in (part.split(':')for part in s.split(';')if part)}
base=Path('target/p9/native484-ingress');go=load(base/'ROOT_QUIET_GO.json');end=load(base/'ROOT_QUIET_END.json');job=Path(args.job);config=load(job/'CAMPAIGN_CONFIG.json');ready=load(job/'READY.json');campaign=load(job/'campaign/CAMPAIGN.json');ref=load(args.readout)
assert go[args.host]['job']==config['job']and go[args.host]['config_sha256']==inputs[str(job/'CAMPAIGN_CONFIG.json')]['sha256']==ready['campaign_config_sha256']==ref['config_sha256']
assert go[args.host]['ready_sha256']==inputs[str(job/'READY.json')]['sha256']
assert end[args.host]['actual_terminal_exit']==0 and end[args.host]['completed_case_end_records']==48
assert config['source_commits']==go['baseline_source_commit'] and False if False else True
assert config['source_commits']=={'baseline':go['baseline_source_commit'],'candidate':go['candidate_source_commit']}==ref['source_commits']
assert len(campaign['cases'])==len(config['cases'])==ref['valid_cases']==48 and not ref['failures']and not campaign['post_timing_errors']
reference={r['case']:r for r in ref['records']};records=[]
for planned,case in zip(config['cases'],campaign['cases']):
 name=case['name'];assert all(case[k]==v for k,v in planned.items());assert case['exit']==case['readbacks_exit']==case['validator_exit']==0
 frozen=reference[name];prefix=job/'campaign';wal=Path(args.case_root)/name if args.case_root else Path(case['wal_root']);resources=Path(args.resource_root)/name if args.resource_root else Path(case['metadata'])
 s=dict(part.split('=',1)for part in data(wal/'summary.txt').split());assert data(prefix/(name+'.stdout'))==data(wal/'summary.txt')
 assert load(prefix/(name+'.native-command.json'))==case['command'];assert load(prefix/(name+'.command.json'))==case['resource_command']
 for suffix in ['.exit','-check.exit']:assert data(prefix/(name+suffix)).strip()=='0'
 rows=table(wal/'offers.csv')
 for r in rows:
  for k in ['operation','group','intended_ns','dispatched_ns','completed_ns','replica','sequence','applied_index','value']:r[k]=int(r[k])if r[k]else 0
 assert len(rows)==int(s['offers'])==case['offers'];assert len({r['operation']for r in rows})==len(rows)
 for i,r in enumerate(rows):
  assert r['operation']==65+i and r['intended_ns']==i*1000000000//case['rate']
  assert r['intended_ns']<=r['dispatched_ns']<=r['completed_ns']<=int(s['elapsed_ns'])
 counts=collections.Counter(r['status']for r in rows);assert set(counts)<={'applied','window_refused'}
 assert all(counts[k]==v for k,v in frozen['statuses'].items());assert all(counts[k]==int(s[k])for k in frozen['statuses'])
 done=[r for r in rows if r['status']=='applied'];admitted=[r for r in rows if r['sequence']]
 assert len(admitted)==len(done)==int(s['admitted']);assert len({(r['replica'],r['sequence'])for r in admitted})==len(admitted)
 elapsed=int(s['elapsed_ns']);horizon=int(s['horizon_ns']);drain=int(s['drain_ns']);assert elapsed-horizon==drain and horizon==case['offers']*1000000000//case['rate']
 assert (elapsed,horizon,drain)==(frozen['elapsed_ns'],frozen['horizon_ns'],frozen['drain_ns'])
 during=sum(r['completed_ns']<horizon for r in done);assert during==int(s['applied_during']);assert len(done)-during==int(s['applied_drain']);assert sum(r['dispatched_ns']>=horizon for r in rows)==int(s['late_decisions'])==frozen['late_decisions']
 backlog=sum(r['dispatched_ns']<horizon<=r['completed_ns']for r in admitted);assert backlog==int(s['backlog_at_horizon'])==frozen['backlog_at_horizon']
 rate=len(done)*1e9/elapsed;during_rate=during*1e9/horizon;close(rate,frozen['rates']['completed_ops_s']);close(during_rate,frozen['rates']['during_ops_s'])
 assert abs(rate-float(s['applied_total_ops_s']))<=.00051;assert abs(during_rate-float(s['applied_during_ops_s']))<=.00051
 quantiles=distributions(rows);exact_q(quantiles,frozen['quantiles'])
 for key,field in [('intended','p99_us'),('service','service_p99_us'),('dispatch','dispatch_p99_us')]:assert abs(quantiles[key]['p99_ms']-float(s[field])/1000)<=.00000051
 live=peak=0;glive=collections.Counter();gpeak=collections.Counter();events=[]
 for i,r in enumerate(rows):
  events.append((r['dispatched_ns'],1,i,r))
  if r['sequence']:events.append((r['completed_ns'],0,i,r))
 for timestamp,kind,i,r in sorted(events,key=lambda e:e[:3]):
  g=str(r['group'])
  if kind==1 and r['status']=='window_refused':assert live>=case['window']or glive[g]>=case['group_pending_ceiling'];continue
  if not r['sequence']:continue
  delta=1 if kind else -1;live+=delta;glive[g]+=delta;peak=max(peak,live);gpeak[g]=max(gpeak[g],glive[g]);assert 0<=live<=case['window']and 0<=glive[g]<=case['group_pending_ceiling']
 assert live==0 and not any(glive.values())and peak==int(s['max_inflight'])==frozen['max_inflight']
 groups={}
 for group in range(1,case['groups']+1):
  g=str(group);offered=[r for r in rows if r['group']==group];gd=[r for r in offered if r['status']=='applied'];gr=frozen['groups'][g]
  expected=case['offers']if case['groups']==1 else(7936 if group==1 else 256);assert len(offered)==expected==gr['offers']
  status=collections.Counter(r['status']for r in offered);assert all(status[k]==v for k,v in gr['statuses'].items());assert len(gd)==gr['completed'];assert gpeak[g]==gr['max_inflight']
  gb=sum(r['dispatched_ns']<horizon<=r['completed_ns']for r in offered if r['sequence']);assert gb==gr['backlog_at_horizon']
  exact_q(distributions(offered),gr['quantiles']);close(len(gd)*1e9/elapsed,gr['completed_ops_s'])
  groups[g]={'applied':len(gd),'refused':status['window_refused'],'intended_p99_ms':distributions(offered)['intended']['p99_ms'],'backlog':gb,'peak':gpeak[g]}
 assert int(s['recovered_value'])==64+len(done)and s['retry_verified']==s['workers_joined']=='true';assert frozen['recovery']=={k:s[k]for k in frozen['recovery']}
 resource=load(resources/'RESOURCES.json');assert resource['exit']==resource['helper_exit']==0 and not resource['helper_error']and not resource['received_signals']
 assert resource['max_rss_bytes']==resource['max_rss_raw']*(1024 if args.host=='linux'else 1);cpu=(resource['user_seconds']+resource['system_seconds'])*1e6/len(done)
 close(cpu,frozen['wholechild_resources']['cpu_us_per_completed_original']);assert all(resource[k]==v for k,v in frozen['wholechild_resources'].items()if k!='cpu_us_per_completed_original')
 storage=table(wal/'storage.csv');physical={}
 for rep in ['1','2','3']:
  rs={r['stage']:r for r in storage if r['replica']==rep};before=rs['before_measurement'];after=rs['after_measurement'];d=frozen['replica_storage'][rep]['measurement_delta']
  for k,v in d.items():
   if isinstance(v,int):assert int(after[k])-int(before[k])==v
   else:
    a=hist(before[k]);z=hist(after[k]);assert {n:z.get(n,0)-a.get(n,0)for n in set(z)|set(a)if z.get(n,0)-a.get(n,0)}=={n:v0 for n,v0 in v.items()if v0}
  assert d['errors']==0
  physical[rep]={'barriers':d['barrier_calls'],'barrier_ns':d['barrier_ns'],'sync_ns':d['sync_ns'],'publication_ns':d['publish_ns'],'commands':d['append_commands'],'append_units':d['append_units']}
  for field,denom in [('mean_barrier_ms','barrier_calls'),('mean_wal_sync_ms','sync_calls'),('mean_publication_ms','publish_calls')]:
   numerator={'mean_barrier_ms':'barrier_ns','mean_wal_sync_ms':'sync_ns','mean_publication_ms':'publish_ns'}[field];close(d[numerator]/d[denom]/1e6,frozen['replica_storage'][rep][field])
 for rep in ['1','2','3']:
  pub=table(wal/'publication.csv')
  for step in ['open','write','file_sync','rename','directory_sync']:
   rs={r['stage']:r for r in pub if r['replica']==rep and r['step']==step};d=frozen['publication_primitives'][rep][step]['measurement_delta']
   for k,v in d.items():assert int(rs['after_measurement'][k])-int(rs['before_measurement'][k])==v
   assert d['errors']==d['manifest_errors']==0
   if step in ['open','rename','directory_sync']:assert d['calls']==d['elapsed_ns']==0
 maint=table(wal/'maintenance.csv');mc=collections.Counter((r['kind'],r['status'])for r in maint);assert {str(k):v for k,v in mc.items()}==frozen['maintenance']['counts']
 bases=table(wal/'bases.csv');assert bases==frozen['maintenance']['base_readbacks']
 for rep in ['1','2','3']:
  for g in range(1,case['groups']+1):assert next(r['base_index']for r in bases if r['stage']=='before_close'and r['replica']==rep and r['group']==str(g))==next(r['base_index']for r in bases if r['stage']=='after_recover'and r['replica']==rep and r['group']==str(g))
 records.append({'name':name,'pair':case['pair'],'cell':case['cell'],'protocol':case['protocol'],'arm':case['arm'],'applied':len(done),'refused':counts['window_refused'],'completed_ops_s':rate,'during_ops_s':during_rate,'p99_ms':quantiles['intended']['p99_ms'],'cpu_us_per_completed':cpu,'wholechild_cpu_s':resource['user_seconds']+resource['system_seconds'],'max_rss_bytes':resource['max_rss_bytes'],'backlog':backlog,'groups':groups,'physical':physical})
pairs=[]
for left,right in zip(records[::2],records[1::2]):
 a=left if left['arm']=='baseline'else right;b=right if right['arm']=='candidate'else left;fr=next(p for p in ref['pairs']if p['pair']==a['pair']);assert a['pair']==b['pair']
 losses={'rate':b['completed_ops_s']<a['completed_ops_s'],'during':b['during_ops_s']<a['during_ops_s'],'p99':b['p99_ms']>a['p99_ms'],'cpu':b['cpu_us_per_completed']>a['cpu_us_per_completed'],'rss':b['max_rss_bytes']>a['max_rss_bytes'],'cold_p99':a['cell']=='skew'and b['groups']['2']['intended_p99_ms']>a['groups']['2']['intended_p99_ms']}
 assert losses['rate']==fr['rate_loss']and losses['during']==fr['during_loss']and losses['p99']==fr['p99_loss']
 pairs.append({'pair':a['pair'],'cell':a['cell'],'protocol':a['protocol'],'baseline':a,'candidate':b,'losses':losses})
cells=[]
for cell in ['singleton','moderate','loaded','skew']:
 for proto in ['tcp','quic']:
  selected=[p for p in pairs if p['cell']==cell and p['protocol']==proto]
  x={'cell':cell,'protocol':proto,'loss_counts':{k:sum(p['losses'][k]for p in selected)for k in selected[0]['losses']},'arms':{}}
  for arm in ['baseline','candidate']:
   rr=[p[arm]for p in selected];x['arms'][arm]={k:{'median':statistics.median(r[k]for r in rr),'min':min(r[k]for r in rr),'max':max(r[k]for r in rr)}for k in ['completed_ops_s','during_ops_s','p99_ms','cpu_us_per_completed','wholechild_cpu_s','max_rss_bytes','backlog']};x['arms'][arm]['refused_total']=sum(r['refused']for r in rr)
   if cell=='skew':x['arms'][arm]['cold']={'p99_median_ms':statistics.median(r['groups']['2']['intended_p99_ms']for r in rr),'applied_total':sum(r['groups']['2']['applied']for r in rr),'refused_total':sum(r['groups']['2']['refused']for r in rr)}
  cells.append(x)
result={'actual_exit':0,'host':args.host,'cases':len(records),'config_sha256':ref['config_sha256'],'actual_terminal':end[args.host],'totals':{'applied':sum(r['applied']for r in records),'window_refused':sum(r['refused']for r in records)},'cells':cells,'pairs':pairs,'inputs':inputs,'limits':['Native elapsed is horizon+drain, before recovery/verification; CPU/RSS is whole child including warmup, recovery and cleanup. CPUus/completed is a wider numerator, no per-original causal claim.','Tail/memory/debt/fairness causality not established;3 rotating finite trials/one shared physical disk per host, differing within-host fixed compilers. No sustained maximum capacity/P9/default/third-filesystem acceptance.','Append units include metadata; physical elapsed is inclusive/nested, not CPU, sum, or literal proposal width.']}
assert result['totals']=={k:ref['totals'][k]for k in result['totals']};assert sum(result['totals'].values())==391680
Path(args.output).write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'actual_exit':0,'host':args.host,'cases':48,'inputs':len(inputs),'totals':result['totals'],'losses':{k:sum(p['losses'][k]for p in pairs)for k in pairs[0]['losses']}},indent=2))
