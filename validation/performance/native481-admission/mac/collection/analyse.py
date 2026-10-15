"""Independent481 raw-ledger arithmetic after original jobs terminated; no workloads."""
from pathlib import Path
import collections,csv,datetime,hashlib,json,statistics

job=Path(__file__).resolve().parent;raw=job/'raw'
load=lambda p:json.loads(Path(p).read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
config=load(raw/'CAMPAIGN_CONFIG.json');campaign=load(raw/'campaign/CAMPAIGN.json');terminal=load(raw/'TERMINAL.json')
assert terminal['exit']==0 and terminal['cases_terminated']==48 and terminal['all_timing_ended']
assert len(campaign['cases'])==48 and not campaign['post_timing_errors']
assert sha(raw/'CAMPAIGN_CONFIG.json')==load(raw/'READY.json')['campaign_config_sha256']
for name,digest in config['scripts'].items():assert sha(raw/name)==digest,name
for name,digest in config['validators'].items():assert sha(raw/'control-tools'/name)==digest,name
for name,record in load(raw/'COLLECTED.json')['files'].items():assert sha(raw/name)==record['sha256'],name
before=load(raw/'before-host.json');after=load(raw/'after-host.json')
assert before['device_identity']==after['device_identity']==config['device_identity']
assert before['cpu_policy']==after['cpu_policy']==config['cpu_policy']
def rows(path):
 with Path(path).open() as stream:return list(csv.DictReader(stream))
def percentile(values,p):
 values=sorted(values);assert values
 return values[(len(values)*p+99)//100-1]
def distribution(values):return {**{f'p{p}_ms':percentile(values,p)/1e6 for p in [50,95,99]},'max_ms':max(values)/1e6}
def stats(values):return {'median':statistics.median(values),'min':min(values),'max':max(values),'trials':values}
def histogram(text):return {int(k):int(v) for k,v in (pair.split(':') for pair in text.split('|') if pair)}
def outcome_metrics(ledger,horizon,elapsed,window):
 counts=collections.Counter(r['status'] for r in ledger);applied=[r for r in ledger if r['status']=='applied']
 during=sum(int(r['completed_ns'])<horizon for r in applied)
 admitted_during=sum(int(r['dispatched_ns'])<horizon for r in applied)
 backlog=sum(int(r['dispatched_ns'])<horizon<=int(r['completed_ns']) for r in applied)
 inflight=0;peak=0
 for _,change in sorted([(int(r['dispatched_ns']),1)for r in applied]+[(int(r['completed_ns']),-1)for r in applied]):
  inflight+=change;peak=max(peak,inflight);assert 0<=inflight<=window
 assert inflight==0
 return {'offers':len(ledger),'applied':len(applied),'applied_during':during,'admitted_during':admitted_during,'applied_drain':len(applied)-during,
  'completed_originals_s':len(applied)*1e9/elapsed,'during_originals_s':during*1e9/horizon,
  'drain_originals_s':(len(applied)-during)*1e9/(elapsed-horizon)if elapsed>horizon else 0,
  'offered_originals_s':len(ledger)*1e9/horizon,'max_inflight':peak,'backlog_at_horizon':backlog,
  'refusals':{k:counts[k]for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending']},
  'intended_latency':distribution([int(r['completed_ns'])-int(r['intended_ns'])for r in applied])if applied else None,
  'service_latency':distribution([int(r['completed_ns'])-int(r['dispatched_ns'])for r in applied])if applied else None,
  'dispatch_latency':distribution([int(r['dispatched_ns'])-int(r['intended_ns'])for r in ledger])}
records=[];totals=collections.Counter();maintenance_totals=collections.Counter()
fields=['append_calls','append_units','append_commands','append_ns','barrier_calls','barrier_tickets','barrier_ns','errors','encoded_bytes','io_append_calls','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']
for case in campaign['cases']:
 name=case['name'];root=raw/'results'/name
 assert case['exit']==case['validator_exit']==case['readbacks_exit']==0
 assert (raw/'campaign'/(name+'.exit')).read_text().strip()==(raw/'campaign'/(name+'-check.exit')).read_text().strip()=='0'
 s=dict(token.split('=',1) for token in (root/'summary.txt').read_text().split());n=lambda key:int(s[key])
 assert (raw/'campaign'/(name+'.stdout')).read_bytes()==(root/'summary.txt').read_bytes()
 assert s['publication_requested']==s['publication_effective']==case['publication']
 assert s['ready_visit_items_requested']==s['ready_visit_items_effective']==str(case['ready_items'])
 assert case['command'][case['command'].index('--ready-items')+1]==str(case['ready_items'])
 assert s['core_proposal_entries']=='64'and s['ready_visit_bytes']=='1048576'and s['ready_visit_ms']=='2'
 ledger=rows(root/'offers.csv');counts=collections.Counter(row['status'] for row in ledger);applied=[row for row in ledger if row['status']=='applied']
 assert len(ledger)==n('offers')==case['offers'] and len(applied)==n('applied')==n('admitted')
 assert sorted(int(row['operation']) for row in ledger)==list(range(65,65+len(ledger)))
 for key in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending']:
  assert counts[key]==n(key),(name,key)
 assert set(counts)<={'applied','window_refused'} and n('recovered_value')==64+len(applied)
 assert s['retry_verified']==s['workers_joined']=='true'
 assert int(s['groups'])==case['groups']
 if case['weights']:
  assert s['workload_group_weights']=='31,1'and s['workload_group_phase']=='absolute_operation_minus_one'
  assert all(int(r['group'])==(2 if (int(r['operation'])-1)%32==31 else 1)for r in ledger)
 else:assert all(r['group']=='1'for r in ledger)
 horizon=n('horizon_ns');elapsed=n('elapsed_ns')
 assert horizon==case['offers']*1_000_000_000//case['rate'] and elapsed>=horizon
 assert sum(int(row['completed_ns'])<horizon for row in applied)==n('applied_during')
 assert sum(int(row['dispatched_ns'])<horizon for row in applied)==n('admitted_during')
 assert len(applied)-n('applied_during')==n('applied_drain') and elapsed-horizon==n('drain_ns')
 assert sum(int(row['dispatched_ns'])<horizon<=int(row['completed_ns']) for row in applied)==n('backlog_at_horizon')
 events=sorted([(int(row['dispatched_ns']),1) for row in applied]+[(int(row['completed_ns']),-1) for row in applied])
 inflight=0;peak=0
 for _,change in events:
  inflight+=change;peak=max(peak,inflight);assert 0<=inflight<=case['window']
 assert inflight==0 and peak==n('max_inflight')
 groups={group:outcome_metrics([r for r in ledger if int(r['group'])==group],horizon,elapsed,case['group_pending_ceiling'])for group in range(1,case['groups']+1)}
 assert sum(g['applied']for g in groups.values())==len(applied)
 if case['cell']=='skew':assert groups[1]['offers']==case['offers']*31//32 and groups[2]['offers']==case['offers']//32
 intended=distribution([int(row['completed_ns'])-int(row['intended_ns']) for row in applied])
 service=distribution([int(row['completed_ns'])-int(row['dispatched_ns']) for row in applied])
 dispatch=distribution([int(row['dispatched_ns'])-int(row['intended_ns']) for row in ledger])
 for d,key in [(intended,'p99_us'),(service,'service_p99_us'),(dispatch,'dispatch_p99_us')]:assert abs(d['p99_ms']-float(s[key])/1000)<.0000011,(name,key)
 rate=len(applied)*1e9/elapsed;during=n('applied_during')*1e9/horizon
 assert abs(rate-float(s['applied_total_ops_s']))<=.00051
 assert abs(during-float(s['applied_during_ops_s']))<=.00051
 storage=rows(root/'storage.csv');stages={stage:{int(row['replica']):row for row in storage if row['stage']==stage} for stage in ['before_measurement','after_measurement','after_create_join','after_recover_join']}
 assert all(len(value)==3 for value in stages.values())
 deltas={replica:{key:int(stages['after_measurement'][replica][key])-int(stages['before_measurement'][replica][key]) for key in fields} for replica in [1,2,3]}
 for replica,d in deltas.items():
  assert min(d.values())>=0 and d['errors']==0 and d['encoded_bytes']>0
  before_h=histogram(stages['before_measurement'][replica]['batch_histogram']);after_h=histogram(stages['after_measurement'][replica]['batch_histogram'])
  h={key:value-before_h.get(key,0) for key,value in after_h.items()};assert min(h.values())>=0
  assert sum(h.values())==d['append_calls'] and sum(key*value for key,value in h.items())==d['append_units']
  d['append_unit_count_histogram']=h
  before_g=histogram(stages['before_measurement'][replica]['group_units']);after_g=histogram(stages['after_measurement'][replica]['group_units'])
  d['append_units_per_group']={key:value-before_g.get(key,0)for key,value in after_g.items()}
  assert min(d['append_units_per_group'].values())>=0 and sum(d['append_units_per_group'].values())==d['append_units']
 publication=rows(root/'publication.csv');primitive={}
 for replica in [1,2,3]:
  primitive[replica]={}
  for step in ['open','write','file_sync','rename','directory_sync']:
   first=next(r for r in publication if r['stage']=='before_measurement' and int(r['replica'])==replica and r['step']==step)
   last=next(r for r in publication if r['stage']=='after_measurement' and int(r['replica'])==replica and r['step']==step)
   delta={key:int(last[key])-int(first[key]) for key in ['calls','errors','elapsed_ns']}
   assert min(delta.values())>=0 and delta['errors']==0
   if case['publication']=='witness' and step in ['open','rename','directory_sync']:assert all(value==0 for value in delta.values())
   primitive[replica][step]=delta
 placements={int(g):int(r) for g,r in (part.split(':') for part in s['leader_placement'].split(','))};leader=placements[1];d=deltas[leader]
 leader_metrics={'commands_per_append_unit':d['append_commands']/d['append_units'],
  'append_units_per_barrier':d['append_units']/d['barrier_calls'],'barriers_per_applied_original':d['barrier_calls']/len(applied),
  'mean_barrier_ms':d['barrier_ns']/d['barrier_calls']/1e6,'mean_wal_sync_ms':d['sync_ns']/d['sync_calls']/1e6,
  'mean_manifest_ms':d['publish_ns']/d['publish_calls']/1e6,'barrier_wall_fraction':d['barrier_ns']/elapsed,
  'wal_sync_wall_fraction':d['sync_ns']/elapsed,'manifest_wall_fraction':d['publish_ns']/elapsed,
  **{f'mean_{step}_ms':primitive[leader][step]['elapsed_ns']/primitive[leader][step]['calls']/1e6 if primitive[leader][step]['calls'] else 0 for step in primitive[leader]}}
 maintenance=rows(root/'maintenance.csv');mc=collections.Counter((r['kind'],r['status']) for r in maintenance);maintenance_totals.update(mc)
 assert mc[('checkpoint','completed')]==n('checkpoints') and mc[('reclaim','completed')]==n('reclaims')
 cold={kind:{'completed':mc[(kind,'completed')],'latency':distribution([int(r['completed_ns'])-int(r['submitted_ns']) for r in maintenance if r['kind']==kind and r['status']=='completed'])} for kind in ['checkpoint','reclaim'] if mc[(kind,'completed')]}
 resource=load(raw/'campaign-metadata'/name/'RESOURCES.json');assert resource['exit']==resource['helper_exit']==0 and resource['max_rss_unit']=='bytes'
 resource['cpu_core_equivalent']=(resource['user_seconds']+resource['system_seconds'])/resource['elapsed_seconds']
 rec={'case':name,'trial':case['trial'],'cell':case['cell'],'protocol':case['protocol'],'publication':case['publication'],'arm':case['arm'],'ready_items':'q'+str(case['ready_items']),'groups':case['groups'],'weights':case['weights'],'group_pending_ceiling':case['group_pending_ceiling'],'leader_placement':placements,
  'offers':len(ledger),'rate':case['rate'],'window':case['window'],'completed_originals_s':rate,'during_originals_s':during,
  'elapsed_ms':elapsed/1e6,'horizon_ms':horizon/1e6,'drain_ms':n('drain_ns')/1e6,'applied':len(applied),
  'applied_during':n('applied_during'),'applied_drain':n('applied_drain'),'refusals':{k:n(k) for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending']},
  'max_inflight':peak,'backlog_at_horizon':n('backlog_at_horizon'),'late_decisions':n('late_decisions'),'group_outcomes':groups,
  'intended_latency':intended,'service_latency':service,'dispatch_latency':dispatch,'leader':leader,
  'leader_metrics':leader_metrics,'replica_deltas':deltas,'ordinary_publication_step_deltas':primitive,'wholeprocess_resources':resource,
  'maintenance':cold,'maintenance_status_counts':{str(k):v for k,v in mc.items()},'maintenance_summary':{k:s[k] for k in s if any(x in k for x in ['checkpoint','reclaim','maintenance','snapshot','missed'])},
  'full_summary':s}
 records.append(rec);totals.update({'offers':len(ledger),'applied':len(applied),**rec['refusals']})
cells=[]
for cell in ['singleton','moderate','loaded','skew']:
 for protocol in ['tcp','quic']:
  arms={arm:sorted([r for r in records if (r['cell'],r['protocol'],r['arm'])==(cell,protocol,arm)],key=lambda r:r['trial']) for arm in ['baseline','candidate']}
  assert all(len(value)==3 for value in arms.values())
  keys=['completed_originals_s','during_originals_s','applied','applied_during','applied_drain','max_inflight','backlog_at_horizon','drain_ms','late_decisions']
  armstats={arm:{**{key:stats([r[key] for r in rs]) for key in keys},'intended_latency':{key:stats([r['intended_latency'][key] for r in rs]) for key in rs[0]['intended_latency']},
   'service_latency':{key:stats([r['service_latency'][key] for r in rs]) for key in rs[0]['service_latency']},'dispatch_latency':{key:stats([r['dispatch_latency'][key] for r in rs]) for key in rs[0]['dispatch_latency']},
   'refusals':{key:stats([r['refusals'][key] for r in rs]) for key in rs[0]['refusals']},'leader_metrics':{key:stats([r['leader_metrics'][key] for r in rs]) for key in rs[0]['leader_metrics']},
   'wholeprocess_rss_bytes':stats([r['wholeprocess_resources']['max_rss_bytes'] for r in rs]),
   'wholeprocess_cpu_core_equivalent':stats([r['wholeprocess_resources']['cpu_core_equivalent']for r in rs]),
   'groups':{group:{**{key:stats([r['group_outcomes'][group][key]for r in rs])for key in ['offers','applied','applied_during','admitted_during','applied_drain','completed_originals_s','during_originals_s','drain_originals_s','offered_originals_s','max_inflight','backlog_at_horizon']},
    'refusals':{key:stats([r['group_outcomes'][group]['refusals'][key]for r in rs])for key in rs[0]['group_outcomes'][group]['refusals']},
    **{kind:{key:stats([r['group_outcomes'][group][kind][key]for r in rs])for key in rs[0]['group_outcomes'][group][kind]}for kind in ['intended_latency','service_latency','dispatch_latency']}}
    for group in rs[0]['group_outcomes']}} for arm,rs in arms.items()}
  paired=[]
  for a,w in zip(arms['baseline'],arms['candidate']):
   assert a['trial']==w['trial']
   paired.append({'trial':a['trial'],'baseline_case':a['case'],'candidate_case':w['case'],'completed_rate_ratio':w['completed_originals_s']/a['completed_originals_s'],
    'p99_ratio':w['intended_latency']['p99_ms']/a['intended_latency']['p99_ms'],'during_rate_ratio':w['during_originals_s']/a['during_originals_s'],
    'baseline_leader_placement':a['leader_placement'],'candidate_leader_placement':w['leader_placement'],
    'barriers_per_original_ratio':w['leader_metrics']['barriers_per_applied_original']/a['leader_metrics']['barriers_per_applied_original'],
    'groups':{group:{'completed_rate_ratio':w['group_outcomes'][group]['completed_originals_s']/a['group_outcomes'][group]['completed_originals_s'],
      'during_rate_ratio':w['group_outcomes'][group]['during_originals_s']/a['group_outcomes'][group]['during_originals_s'],
      'p99_ratio':w['group_outcomes'][group]['intended_latency']['p99_ms']/a['group_outcomes'][group]['intended_latency']['p99_ms'],
      'baseline_refusals':a['group_outcomes'][group]['refusals'],'candidate_refusals':w['group_outcomes'][group]['refusals'],
      'baseline_backlog':a['group_outcomes'][group]['backlog_at_horizon'],'candidate_backlog':w['group_outcomes'][group]['backlog_at_horizon']}
      for group in a['group_outcomes']},
    'rate_regression':w['completed_originals_s']<a['completed_originals_s'],'p99_regression':w['intended_latency']['p99_ms']>a['intended_latency']['p99_ms']})
  cells.append({'cell':cell,'protocol':protocol,'offers':arms['baseline'][0]['offers'],'rate':arms['baseline'][0]['rate'],'window':arms['baseline'][0]['window'],
   'arms':armstats,'paired':paired,'paired_rate_ratio':stats([p['completed_rate_ratio'] for p in paired]),'paired_p99_ratio':stats([p['p99_ratio'] for p in paired])})
output={'scope':'All48 original Mac481 raw ledgers independently recomputed after both hosts terminated; no rerun. Latencies are nearest-rank successful originals; service/dispatch separate. Rates use native elapsed boundary, not whole-child timing. Physical command/unit means include metadata units and are not maximum proposal width; encoded bytes are codec bytes, not device I/O. Nested elapsed scopes are not additive CPU. Ordinary counters exclude cold repair/selector/reclaim. RSS is whole process in Mac bytes, not a continuous memory/debt plateau. Offered caps are not maximum capacity. No fullP9/default/all3filesystem acceptance.',
 'original_handle':81015,'actual_exit':0,'cases':48,'validators':48,'readback_checks':48,'totals':dict(totals),
 'source_commit':config['source_commit'],'source_commits':config['source_commits'],'artifacts_sha256':{arm:next(a['sha256']for a in record['artifacts']if a['target']=='native_benchmark')for arm,record in config['candidates'].items()},
 'maintenance_status_counts':{str(k):v for k,v in maintenance_totals.items()},'source_device_cpu_guards_match':True,
 'analysis_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'cells':cells,'records':records}
(job/'ANALYSIS.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({'totals':dict(totals),'cells':len(cells),'maintenance':output['maintenance_status_counts'],'all48_raw_recomputations':'PASS'},indent=2))
