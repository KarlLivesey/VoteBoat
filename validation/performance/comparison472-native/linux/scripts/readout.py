#!/usr/bin/env python3
"""Read completed immutable native472 outputs only; never launches subprocesses."""
from pathlib import Path
import csv,json,statistics,decimal,math
D=decimal.Decimal
decimal.getcontext().prec=40
JOB=Path(__file__).resolve().parent/'linux'
def ratio(a,b):return str(D(a)/D(b)) if b else None
def percentile(values):return sorted(values)[math.ceil(len(values)*.99)-1] if values else None
def run():
 config=json.loads((JOB/'CAMPAIGN_CONFIG.json').read_text());campaign=json.loads((JOB/'campaign/CAMPAIGN.json').read_text());assert len(campaign['cases'])==24 and (JOB/'CAMPAIGN_STAGE').read_text().strip()=='ALL_TIMING_END'
 outcomes=[]
 for case in campaign['cases']:
  root=Path(case['wal_root']);s=dict(p.split('=',1) for p in (root/'summary.txt').read_text().split());n=lambda k:int(s[k]);offers=list(csv.DictReader((root/'offers.csv').open()));applied=[r for r in offers if r['status']=='applied'];assert len(offers)==n('offers')==15000 and len(applied)==n('applied') and sum(int(r['completed_ns'])<n('horizon_ns') for r in applied)==n('applied_during')
  rates={'applied_total_exact':ratio(n('applied')*10**9,n('elapsed_ns')),'applied_during_exact':ratio(n('applied_during')*10**9,n('horizon_ns')),'completed_numerator':n('applied')*10**9,'total_denominator_ns':n('elapsed_ns'),'during_numerator':n('applied_during')*10**9,'during_denominator_ns':n('horizon_ns')}
  tails={'intended_to_complete_p99_ns':percentile([int(r['completed_ns'])-int(r['intended_ns']) for r in applied]),'service_p99_ns':percentile([int(r['completed_ns'])-int(r['dispatched_ns']) for r in applied]),'dispatch_p99_ns':percentile([int(r['dispatched_ns'])-int(r['intended_ns']) for r in offers])}
  storage=list(csv.DictReader((root/'storage.csv').open()));hists={'batch_histogram','group_units','barrier_unit_histogram','barrier_original_histogram','barrier_same_group_unit_histogram'};numeric=set(storage[0])-hists-{'stage','replica'};gauge={'barrier_pending_tickets','barrier_correlation_capacity_bytes'};maxima={k for k in numeric if k.endswith('_max_ns')};replicas=[]
  for replica in ['1','2','3']:
   snapshots={r['stage']:r for r in storage if r['replica']==replica};assert len(snapshots)==4
   before=snapshots['before_measurement'];after=snapshots['after_measurement'];delta={k:int(after[k])-int(before[k]) for k in numeric-gauge-maxima};assert all(v>=0 for v in delta.values())
   per={'replica':int(replica),'measurement_deltas':delta,'inclusive_wall_over_total_elapsed':{k:ratio(delta[k],n('elapsed_ns')) for k in ['barrier_ns','sync_ns','publish_ns']},'gauges_by_stage':{stage:{k:int(r[k]) for k in gauge if k in r} for stage,r in snapshots.items()},'lifetime_maxima_by_stage':{stage:{k:int(r[k]) for k in maxima} for stage,r in snapshots.items()},'histograms_by_stage':{stage:{k:r[k] for k in hists if k in r} for stage,r in snapshots.items()},'barriers_per_applied_client_original':ratio(delta['barrier_calls'],n('applied')),'sync_calls_per_applied_client_original':ratio(delta['sync_calls'],n('applied')),'publication_calls_per_applied_client_original':ratio(delta['publish_calls'],n('applied')),'suffix_commands_per_append_unit':ratio(delta['append_commands'],delta['append_units']),'tickets_per_barrier':ratio(delta['barrier_tickets'],delta['barrier_calls']),'all_numeric_snapshots':{stage:{k:int(r[k]) for k in numeric} for stage,r in snapshots.items()}}
   if 'barrier_known_originals' in delta:per['known_suffix_originals_per_successful_barrier']=ratio(delta['barrier_known_originals'],delta['successful_barriers'])
   replicas.append(per)
  totals={k:sum(r['measurement_deltas'][k] for r in replicas) for k in numeric-gauge-maxima}
  gauges={f: [r['gauges_by_stage'] for r in replicas] for f in []}
  resources=json.loads((Path(case['metadata'])/'RESOURCES.json').read_text())
  outcomes.append({'case':case['name'],'variant':case['variant'],'protocol':case['protocol'],'trial':case['trial'],'workload_exit':case['exit'],'original_validator_exits':case.get('validator_exits',[case.get('validator_exit')]),'summary_raw':s,'rates':rates,'tails':tails,'resources':resources,'storage_replicas':replicas,'storage_totals_replica_inclusive':totals,'leader_replica':1,'limitations':'Storage suffix originals include replica/retry work, not client successes. Inclusive wall scopes overlap; gauges are snapshot values, not cumulative/physical RSS. Lifetime maxima are separate raw stage snapshots, not interval tails or cumulative counters. All-offer horizon differs from final drain. Both470/472 observer Rust implementations match; grouping-dependent observer work remains included.'})
 paired={};summary={}
 for protocol in ['tcp','quic']:
  rows=[r for r in outcomes if r['protocol']==protocol];summary[protocol]={}
  for variant in ['baseline-off','candidate-off','baseline-on','candidate-on']:
   group=[r for r in rows if r['variant']==variant];summary[protocol][variant]={'total_rate_median':str(statistics.median(D(r['rates']['applied_total_exact']) for r in group)),'during_rate_median':str(statistics.median(D(r['rates']['applied_during_exact']) for r in group)),'p99_intended_ns_median':statistics.median(r['tails']['intended_to_complete_p99_ns'] for r in group),'applied_counts':[int(r['summary_raw']['applied']) for r in group],'window_refusals':[int(r['summary_raw']['window_refused']) for r in group],'leader_barriers_per_applied_median':str(statistics.median(D(r['storage_replicas'][0]['barriers_per_applied_client_original']) for r in group))}
  paired[protocol]={}
  for family in ['off','on']:
   paired[protocol][family]=[]
   for trial in [1,2,3]:
    before=next(r for r in rows if r['variant']=='baseline-'+family and r['trial']==trial);after=next(r for r in rows if r['variant']=='candidate-'+family and r['trial']==trial)
    paired[protocol][family].append({'trial':trial,'baseline_case':before['case'],'candidate_case':after['case'],'total_rate_candidate_over_baseline':str(D(after['rates']['applied_total_exact'])/D(before['rates']['applied_total_exact'])),'during_rate_candidate_over_baseline':str(D(after['rates']['applied_during_exact'])/D(before['rates']['applied_during_exact'])),'p99_intended_candidate_over_baseline':ratio(after['tails']['intended_to_complete_p99_ns'],before['tails']['intended_to_complete_p99_ns']),'leader_barriers_per_applied_candidate_over_baseline':str(D(after['storage_replicas'][0]['barriers_per_applied_client_original'])/D(before['storage_replicas'][0]['barriers_per_applied_client_original'])),'baseline_window_refused':int(before['summary_raw']['window_refused']),'candidate_window_refused':int(after['summary_raw']['window_refused'])})
 for name,value in [('OUTCOMES.json',outcomes),('SUMMARY.json',summary),('PAIRED.json',paired)]: (JOB/name).write_text(json.dumps(value,indent=2)+'\n')
 print(json.dumps({'summary':summary,'paired':paired},indent=2))
if __name__=='__main__':run()
