"""Saved481 timing correlations only; no workloads, profiles or remote tools."""
import collections,csv,hashlib,json,math,statistics
from pathlib import Path
BASE=Path(__file__).resolve().parent;ROOT=BASE.parent;inputs={}
def read(path):
 data=path.read_bytes();inputs[str(path)]={'sha256':hashlib.sha256(data).hexdigest(),'bytes':len(data)};return data.decode()
def table(path):return list(csv.DictReader(read(path).splitlines()))
def quantile(values,p):return sorted(values)[math.ceil(len(values)*p/100)-1]if values else None
def convert(row,keys):
 for key in keys:row[key]=int(row[key])
 return row
def ms(value):return value/1e6
def simple(row):
 return {'operation':row['operation'],'group':row['group'],'intended_ms':ms(row['intended_ns']),'dispatched_ms':ms(row['dispatched_ns']),'completed_ms':ms(row['completed_ns']),'intended_latency_ms':ms(row['latency']),'service_ms':ms(row['service']),'dispatch_ms':ms(row['dispatch'])}
def event(row):
 return {'wave':row['wave'],'kind':row['kind'],'group':row['group'],'replica':row['replica'],'submitted_ms':ms(row['submitted_ns']),'completed_ms':ms(row['completed_ns']),'elapsed_ms':ms(row['completed_ns']-row['submitted_ns'])}
def overlaps(row,events):return [e for e in events if row['dispatched_ns']<e['completed_ns']and e['submitted_ns']<row['completed_ns']]
def summary(cohort,events):
 lats=[r['latency']for r in cohort];service=[r['service']for r in cohort];dispatch=[r['dispatch']for r in cohort]
 return {'count':len(cohort),'intended_p99_ms':ms(quantile(lats,99))if lats else None,'max_ms':ms(max(lats))if lats else None,'service_p99_ms':ms(quantile(service,99))if lats else None,'dispatch_p99_ms':ms(quantile(dispatch,99))if lats else None,'service_overlaps_any_maintenance':sum(bool(overlaps(r,events))for r in cohort),'dispatch_during_maintenance':sum(any(e['submitted_ns']<=r['dispatched_ns']<e['completed_ns']for e in events)for r in cohort)}
cases=[]
for host,cell,protocol in [('mac','moderate','quic'),('mac','skew','tcp'),('linux','skew','quic')]:
 for trial in [1,2,3]:
  for arm in ['baseline','candidate']:
   name=f't{trial}-{cell}-{protocol}-{arm}';path=ROOT/'mac-collection/raw/results'/name if host=='mac'else ROOT/'linux-wal'/name
   s=dict(v.split('=',1)for v in read(path/'summary.txt').split());offers=table(path/'offers.csv')
   for r in offers:
    convert(r,['operation','group','intended_ns','dispatched_ns','completed_ns','sequence']);r['latency']=r['completed_ns']-r['intended_ns'];r['service']=r['completed_ns']-r['dispatched_ns'];r['dispatch']=r['dispatched_ns']-r['intended_ns']
   applied=[r for r in offers if r['status']=='applied'];selected=[r for r in applied if cell=='moderate'or r['group']==2]
   maintenance=table(path/'maintenance.csv')
   for r in maintenance:convert(r,['wave','group','replica','intended_ns','submitted_ns','completed_ns'])
   events=[r for r in maintenance if r['status']=='completed'and r['kind']in ['checkpoint','reclaim']]
   threshold=quantile([r['latency']for r in selected],99);slow=[r for r in selected if r['latency']>=threshold]
   slowids={r['operation']for r in slow};runs=[];current=[]
   for r in selected:
    if r['operation']in slowids:current.append(r)
    elif current:runs.append(current);current=[]
   if current:runs.append(current)
   cohorts=[]
   for run in runs:
    touched={id(e):e for r in run for e in overlaps(r,events)}
    cohorts.append({'count':len(run),'first':simple(run[0]),'last':simple(run[-1]),'max_ms':ms(max(r['latency']for r in run)),'maintenance_overlaps':[event(e)for e in sorted(touched.values(),key=lambda e:e['submitted_ns'])]})
   ordered=sorted(selected,key=lambda r:r['completed_ns']);gaps=[]
   for a,b in zip(ordered,ordered[1:]):
    gap=b['completed_ns']-a['completed_ns']
    if gap>=250_000_000:
     matching=[e for e in events if a['completed_ns']<e['completed_ns']and e['submitted_ns']<b['completed_ns']]
     gaps.append({'gap_ms':ms(gap),'previous':simple(a),'next':simple(b),'maintenance_in_gap':[event(e)for e in matching]})
   bursts=[];burst=[]
   for r in ordered:
    if burst and r['completed_ns']-burst[-1]['completed_ns']>1_000_000:bursts.append(burst);burst=[]
    burst.append(r)
   if burst:bursts.append(burst)
   burst_summary=[{'count':len(b),'completed_first_ms':ms(b[0]['completed_ns']),'completed_last_ms':ms(b[-1]['completed_ns']),'intended_first_ms':ms(min(r['intended_ns']for r in b)),'intended_last_ms':ms(max(r['intended_ns']for r in b)),'max_latency_ms':ms(max(r['latency']for r in b))}for b in sorted(bursts,key=len,reverse=True)[:8]]
   bins=[]
   for index in range(math.ceil(int(s['horizon_ns'])/2e9)):
    cohort=[r for r in selected if index*2_000_000_000<=r['intended_ns']<(index+1)*2_000_000_000]
    bins.append({'intended_from_ms':index*2000,**summary(cohort,events),'above500ms':sum(r['latency']>=500_000_000 for r in cohort),'above1000ms':sum(r['latency']>=1_000_000_000 for r in cohort)})
   storage=table(path/'storage.csv');physical={}
   for replica in [1,2,3]:
    a=next(r for r in storage if r['stage']=='before_measurement'and int(r['replica'])==replica);b=next(r for r in storage if r['stage']=='after_measurement'and int(r['replica'])==replica)
    physical[replica]={k:int(b[k])-int(a[k])for k in ['barrier_calls','barrier_ns','sync_calls','sync_ns','publish_calls','publish_ns']}
   eventkinds={kind:{'count':sum(e['kind']==kind for e in events),'max_elapsed_ms':ms(max(e['completed_ns']-e['submitted_ns']for e in events if e['kind']==kind))}for kind in ['checkpoint','reclaim']}
   cases.append({'host':host,'cell':cell,'protocol':protocol,'trial':trial,'arm':arm,'case':name,'selected_group':1 if cell=='moderate'else 2,'all_selected':summary(selected,events),'above500ms':summary([r for r in selected if r['latency']>=500_000_000],events),'above1000ms':summary([r for r in selected if r['latency']>=1_000_000_000],events),'own_p99_threshold_ms':ms(threshold),'own_p99_cohort':summary(slow,events),'p99_contiguous_intended_cohorts':cohorts,'largest_completion_gaps':sorted(gaps,key=lambda g:g['gap_ms'],reverse=True)[:8],'largest_observed_completion_bursts':burst_summary,'worst_originals':[simple(r)for r in sorted(selected,key=lambda r:r['latency'],reverse=True)[:6]],'intended_2s_bins':bins,'maintenance':eventkinds,'longest_maintenance_rows':[event(e)for e in sorted(events,key=lambda e:e['completed_ns']-e['submitted_ns'],reverse=True)[:8]],'snapshot_aggregate':{k:int(s[k])for k in ['snapshot_events','snapshot_supplies','follower_snapshot_installs','checkpoints','reclaims','maintenance_skips']},'physical_cumulative_measurement_deltas':physical,'refusals_by_group':{g:dict(collections.Counter(r['status']for r in offers if r['group']==g))for g in sorted({r['group']for r in offers})}})
for source in ['examples/benchmark/offered.rs','examples/benchmark/maintenance.rs','examples/native_benchmark.rs','src/native/worker.rs','src/native/log_store.rs']:read(Path(source))
report={'exit':0,'scope':'Existing481 raw-only timing correlations; no benchmark, profile, observer, remote work or production change.','clocks':'offers intended/dispatched/completed_ns and maintenance submitted/completed_ns share the native measurement start.elapsed nanoseconds. measure passes its sampled now into Maintenance::step after host polls/output collection/offers. Maintenance completion is host observation at that now, not a device timestamp; shared timestamps and poll quantization limit causal order. CSV replicas are one-based (writer adds1).','limits':['Receipt completion gaps are observational gaps, not proof the whole node/thread was idle or network packets were lost. Completion bursts use adjacent<=1ms intervals and are not literal persistence/proposal/packet batches.','Any long request naturally overlaps more maintenance windows; compare start-during counts/all-requests background rate, not overlap alone as causation. Reclaim operations across replicas overlap; elapsed sums must not be added as CPU.','snapshot_events/supplies are measurement-phase aggregates. Reported follower_snapshot_installs is replica3 aggregate (totals.snapshot_installs[2]), not all followers. No per-install timestamp is present, so no exact association of an individual tail with an install is identifiable.','storage.csv has before/after cumulative barrier/sync/publication counters and elapsed totals. It has no per-barrier timestamps or per-original barrier ownership, so a specific stall cannot be assigned to a barrier, disk flush, allocator or network cause.','These selected cells are regression investigation, not a universal performance or fullP9 acceptance claim.'],'inputs':inputs,'cases':cases}
(BASE/'REPORT.json').write_text(json.dumps(report,indent=2)+'\n')
for c in cases:
 print(c['host'],c['case'],'p99',round(c['all_selected']['intended_p99_ms'],3),'service',round(c['all_selected']['service_p99_ms'],3),'dispatch',round(c['all_selected']['dispatch_p99_ms'],3),'over1s',c['above1000ms']['count'],'p99overlap',c['own_p99_cohort']['service_overlaps_any_maintenance'],'of',c['own_p99_cohort']['count'],'snapinstalls',c['snapshot_aggregate']['follower_snapshot_installs'])
