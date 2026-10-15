#!/usr/bin/env python3
"""Read completed immutable native470 outputs only; never launches subprocesses."""
from pathlib import Path
import csv,json,statistics,decimal,math
D=decimal.Decimal
decimal.getcontext().prec=40
JOB=Path(__file__).resolve().parent/'linux'
def ratio(a,b):return str(D(a)/D(b)) if b else None
def percentile(values):return sorted(values)[math.ceil(len(values)*.99)-1] if values else None
def run():
 config=json.loads((JOB/'CAMPAIGN_CONFIG.json').read_text());campaign=json.loads((JOB/'campaign/CAMPAIGN.json').read_text());assert len(campaign['cases'])==18 and (JOB/'CAMPAIGN_STAGE').read_text().strip()=='ALL_TIMING_END'
 outcomes=[]
 for case in campaign['cases']:
  root=Path(case['wal_root']);s=dict(p.split('=',1) for p in (root/'summary.txt').read_text().split());n=lambda k:int(s[k]);offers=list(csv.DictReader((root/'offers.csv').open()));applied=[r for r in offers if r['status']=='applied'];assert len(offers)==n('offers')==15000 and len(applied)==n('applied') and sum(int(r['completed_ns'])<n('horizon_ns') for r in applied)==n('applied_during')
  rates={'applied_total_exact':ratio(n('applied')*10**9,n('elapsed_ns')),'applied_during_exact':ratio(n('applied_during')*10**9,n('horizon_ns')),'completed_numerator':n('applied')*10**9,'total_denominator_ns':n('elapsed_ns'),'during_numerator':n('applied_during')*10**9,'during_denominator_ns':n('horizon_ns')}
  tails={'intended_to_complete_p99_ns':percentile([int(r['completed_ns'])-int(r['intended_ns']) for r in applied]),'service_p99_ns':percentile([int(r['completed_ns'])-int(r['dispatched_ns']) for r in applied]),'dispatch_p99_ns':percentile([int(r['dispatched_ns'])-int(r['intended_ns']) for r in offers])}
  storage=list(csv.DictReader((root/'storage.csv').open()));hists={'batch_histogram','group_units','barrier_unit_histogram','barrier_original_histogram','barrier_same_group_unit_histogram'};numeric=set(storage[0])-hists-{'stage','replica'};gauge={'barrier_pending_tickets','barrier_correlation_capacity_bytes'};replicas=[]
  for replica in ['1','2','3']:
   snapshots={r['stage']:r for r in storage if r['replica']==replica};assert len(snapshots)==4
   before=snapshots['before_measurement'];after=snapshots['after_measurement'];delta={k:int(after[k])-int(before[k]) for k in numeric-gauge};assert all(v>=0 for v in delta.values())
   per={'replica':int(replica),'measurement_deltas':delta,'inclusive_wall_over_total_elapsed':{k:ratio(delta[k],n('elapsed_ns')) for k in ['barrier_ns','sync_ns','publish_ns']},'gauges_by_stage':{stage:{k:int(r[k]) for k in gauge if k in r} for stage,r in snapshots.items()},'histograms_by_stage':{stage:{k:r[k] for k in hists if k in r} for stage,r in snapshots.items()},'barriers_per_applied_client_original':ratio(delta['barrier_calls'],n('applied')),'sync_calls_per_applied_client_original':ratio(delta['sync_calls'],n('applied')),'publication_calls_per_applied_client_original':ratio(delta['publish_calls'],n('applied')),'suffix_commands_per_append_unit':ratio(delta['append_commands'],delta['append_units']),'tickets_per_barrier':ratio(delta['barrier_tickets'],delta['barrier_calls']),'all_numeric_snapshots':{stage:{k:int(r[k]) for k in numeric} for stage,r in snapshots.items()}}
   if 'barrier_known_originals' in delta:per['known_suffix_originals_per_successful_barrier']=ratio(delta['barrier_known_originals'],delta['successful_barriers'])
   replicas.append(per)
  totals={k:sum(r['measurement_deltas'][k] for r in replicas) for k in numeric-gauge}
  gauges={f: [r['gauges_by_stage'] for r in replicas] for f in []}
  resources=json.loads((Path(case['metadata'])/'RESOURCES.json').read_text())
  outcomes.append({'case':case['name'],'variant':case['variant'],'protocol':case['protocol'],'trial':case['trial'],'workload_exit':case['exit'],'original_validator_exits':case.get('validator_exits',[case.get('validator_exit')]),'summary_raw':s,'rates':rates,'tails':tails,'resources':resources,'storage_replicas':replicas,'storage_totals_replica_inclusive':totals,'leader_replica':1,'limitations':'Storage suffix originals include replica/retry work, not client successes. Inclusive wall scopes overlap; gauges are snapshot values, not cumulative/physical RSS. All-offer horizon differs from final drain. Historical458 observer schema21 is context only.'})
 paired={};summary={}
 for protocol in ['tcp','quic']:
  rows=[r for r in outcomes if r['protocol']==protocol];summary[protocol]={}
  for variant in ['baseline','off','on']:
   group=[r for r in rows if r['variant']==variant];summary[protocol][variant]={'total_rate_median':str(statistics.median(D(r['rates']['applied_total_exact']) for r in group)),'during_rate_median':str(statistics.median(D(r['rates']['applied_during_exact']) for r in group)),'p99_intended_ns_median':statistics.median(r['tails']['intended_to_complete_p99_ns'] for r in group),'applied_counts':[int(r['summary_raw']['applied']) for r in group],'window_refusals':[int(r['summary_raw']['window_refused']) for r in group],'leader_barriers_per_applied_median':str(statistics.median(D(r['storage_replicas'][0]['barriers_per_applied_client_original']) for r in group))}
  paired[protocol]=[]
  for trial in [1,2,3]:
   off=next(r for r in rows if r['variant']=='off' and r['trial']==trial);on=next(r for r in rows if r['variant']=='on' and r['trial']==trial)
   paired[protocol].append({'trial':trial,'total_rate_on_over_off':str(D(on['rates']['applied_total_exact'])/D(off['rates']['applied_total_exact'])),'during_rate_on_over_off':str(D(on['rates']['applied_during_exact'])/D(off['rates']['applied_during_exact'])),'p99_intended_on_over_off':ratio(on['tails']['intended_to_complete_p99_ns'],off['tails']['intended_to_complete_p99_ns']),'leader_barriers_per_applied_on_over_off':str(D(on['storage_replicas'][0]['barriers_per_applied_client_original'])/D(off['storage_replicas'][0]['barriers_per_applied_client_original']))})
 for name,value in [('OUTCOMES.json',outcomes),('SUMMARY.json',summary),('PAIRED.json',paired)]: (JOB/name).write_text(json.dumps(value,indent=2)+'\n')
 print(json.dumps({'summary':summary,'paired':paired},indent=2))
if __name__=='__main__':run()
