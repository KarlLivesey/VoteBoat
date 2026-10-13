import json,sys
from pathlib import Path
p=Path(sys.argv[1]);cases=[];originals=1048576;warmup=1024
for trial in [1,2,3]:
 for variant in ['baseline','off','on']:
  label=f'{variant}-c4096-b4-t{trial}';assert (p/f'{label}.exit').read_text().strip()=='0'
  v=json.loads((p/f'{label}.json').read_text());s=v['settings'];on=variant=='on'
  assert s['operations']==v['completed_originals']==originals and v['completed_requests']==originals//4
  assert s['clients']==4096 and s['batch']==4 and s['warmup']==warmup and s['attribution']==on
  assert s['maintenance']=='default_retention' and s['envelopes']=='upstream_default'
  assert v['applied_frontiers']==[originals+warmup+1]*3
  assert [v[k] for k in ['api_entry_batch_capacity','append_entry_batch_capacity','replication_entry_capacity']]==[4096,4096,1024]
  leader=v['diagnostics'][0];assert leader['proposed_originals']==originals+warmup
  assert sum(x['original_entries']*x['batches'] for x in leader['proposal_widths'])==originals+warmup
  if not on:assert v.get('attribution') is None
  else:
   a=v['attribution'];assert len(a)==3 and sorted(x['replica_slot'] for x in a)==[0,1,2]
   for replica in a:
    assert replica['scope']=='whole_actor_lifetime_including_campaign_warmup_follower_drain_and_shutdown'
    assert replica['clock']=='monotonic_elapsed_wall_intervals_not_cpu_or_critical_path'
    assert 'do_not_sum' in replica['nesting'];phases=replica['phases'];assert len(phases)==12 and len({x['phase'] for x in phases})==12
    for x in phases:
     assert all(isinstance(x[k],int) and x[k]>=0 for k in ['calls','failures','total_elapsed_ns','maximum_elapsed_ns','items'])
     assert x['failures']==0 and x['maximum_elapsed_ns']<=x['total_elapsed_ns']
     if x['calls']==0:assert x['total_elapsed_ns']==x['maximum_elapsed_ns']==x['items']==0
     if x['phase']=='send':assert x['items']==x['calls']
     if x['phase']=='actor_lifetime_inclusive':assert x['calls']==1 and x['total_elapsed_ns']==x['maximum_elapsed_ns']
     if replica['replica_slot']==0 and x['phase'] in ['host_admission','core_direct','application','results']:assert x['items']==originals+warmup
  cases.append({'label':label,'variant':variant,'trial':trial,'report':v})
assert len(cases)==9
(p/'RESULTS.json').write_text(json.dumps({'scope':'matching355417 to322418 on/off diagnostic; whole actor lifetime including warmup; elapsed phases nested/overlap, not CPU or exclusive critical path; no primary floor claim','cases':cases,'original_phase_scope_identity_guards':True},indent=2)+'\n')
print('nine original/frontier/policy/phase-scope controls pass')
