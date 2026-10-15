"""Recompute compact Mac479 saved-profile contexts and original skew ledgers."""
from pathlib import Path
import collections,csv,gzip,hashlib,json,re
job=Path(__file__).resolve().parent;raw=job/'raw'
load=lambda p:json.loads(Path(p).read_text())
profile=json.loads(gzip.decompress((raw/'profile.json.gz').read_bytes()));analysis=load(job/'analysis/SUMMARY.json');total=analysis['cpu_denominator_us']
def short(s):
 if 'BTreeMap<voteboat::identity::OperationId, alloc::collections::btree::set_val::SetValZST>'in s:return 'reserved OperationId BTreeSet::insert'if'::insert'in s else'reserved OperationId BTreeSet'
 if 'BTreeMap<voteboat::identity::OperationId, ([u8; 8], voteboat::application::CounterOutcome)>'in s:return 'Counter.dedup BTreeMap::get'
 if 'BTreeMap<u64, voteboat::runtime::clients::Pending<'in s:return 'ClientRouter.pending BTreeMap::get'
 if s.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal'):return 'Counter::validate_proposal[ready]'if'check_ready_execution'in s else'Counter::validate_proposal[submit]'
 if s.startswith('voteboat::runtime::ready_batch::collect'):return 'ready_batch::collect'
 t=s
 while re.search(r'<[^<>]*>',t):t=re.sub(r'<[^<>]*>','',t)
 return t if len(t)<180 else t[:177]+'...'
checks={
 'ready_admission':lambda f:f.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal')and'check_ready_execution'in f,
 'submission_admission':lambda f:f.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal')and'check_ready_execution'not in f,
 'all_counter_admission':lambda f:f.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal'),
 'ready_batch_collect':lambda f:f.startswith('voteboat::runtime::ready_batch::collect'),
 'pending_receipt_get':lambda f:'BTreeMap<u64, voteboat::runtime::clients::Pending<'in f and'::get::'in f,
 'outcome_lookup':lambda f:'BTreeMap<voteboat::identity::OperationId, ([u8; 8], voteboat::application::CounterOutcome)>'in f,
 'reserved_set_insert':lambda f:'BTreeMap<voteboat::identity::OperationId, alloc::collections::btree::set_val::SetValZST>'in f and'::insert'in f}
contexts={}
for name,check in checks.items():
 stacks=[r for r in analysis['physical_callstacks']if any(check(f)for f in r['root_to_leaf'])];w=sum(r['cpu_us']for r in stacks)
 contexts[name]={'cpu_us':w,'percent':100*w/total,'child_context_us_nonadditive':{child:sum(r['cpu_us']for r in stacks if any(checks[child](f)for f in r['root_to_leaf']))for child in ['outcome_lookup','reserved_set_insert','pending_receipt_get']}}
compact={'cpu_unit':analysis['cpu_unit'],'cpu_us':total,'totals':analysis['totals'],'contexts_nonadditive':contexts,
 'top_self':[{'function':short(r['function']),'cpu_us':r['cpu_us'],'percent':r['percent_of_observed_selected_thread_cpu']}for r in analysis['self_native_functions'][:30]],
 'top_inclusive':[{'function':short(r['function']),'cpu_us':r['cpu_us'],'percent':r['percent_of_observed_selected_thread_cpu']}for r in analysis['inclusive_native_functions'][:35]],
 'top_callers':[{'caller':short(r['caller']),'callee':short(r['callee']),'cpu_us':r['cpu_us'],'percent':r['percent']}for r in analysis['caller_contexts'][:35]],
 'scope':'Each threadCPUDelta counted once, never multiplied by sample weight. Interval observations assigned to sampled stacks include startup/warmup/recovery/cleanup; wait-stack attribution is not an instruction cost. Inclusive contexts overlap. Wall coalesced sample weights are separate from CPU. Native debug0 limits source/inline; full exact labels and addresses remain in SUMMARY and original query responses.'}
(job/'COMPACT_CPU.json').write_text(json.dumps(compact,indent=2)+'\n')
def percentile(values,p):return sorted(values)[(len(values)*p+99)//100-1]
def dist(values):return {**{f'p{p}_ms':percentile(values,p)/1e6 for p in [50,95,99]},'max_ms':max(values)/1e6}
with(raw/'results/offers.csv').open()as f:ledger=list(csv.DictReader(f))
s=load(raw/'VALIDATION.json')['summary'];horizon=int(s['horizon_ns']);elapsed=int(s['elapsed_ns']);groups={}
assert sorted(int(r['operation'])for r in ledger)==list(range(65,8257))
for group in [1,2]:
 rows=[r for r in ledger if int(r['group'])==group];applied=[r for r in rows if r['status']=='applied'];counts=collections.Counter(r['status']for r in rows)
 assert all(int(r['group'])==(2 if(int(r['operation'])-1)%32==31 else 1)for r in rows)
 groups[group]={'offers':len(rows),'applied':len(applied),'refusals':{key:counts[key]for key in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending']},
  'completed_originals_s':len(applied)*1e9/elapsed,'during_originals_s':sum(int(r['completed_ns'])<horizon for r in applied)*1e9/horizon,
  'backlog_at_horizon':sum(int(r['dispatched_ns'])<horizon<=int(r['completed_ns'])for r in applied),
  'intended_latency':dist([int(r['completed_ns'])-int(r['intended_ns'])for r in applied]),
  'service_latency':dist([int(r['completed_ns'])-int(r['dispatched_ns'])for r in applied]),
  'dispatch_latency':dist([int(r['dispatched_ns'])-int(r['intended_ns'])for r in rows])}
counts=collections.Counter(r['status']for r in ledger);assert sum(g['applied']for g in groups.values())==int(s['applied'])
assert counts['applied']+counts['window_refused']==len(ledger)==8192
assert int(s['recovered_value'])==64+counts['applied']and s['retry_verified']==s['workers_joined']=='true'
nativepid={t['pid']for t in profile['threads']if t['name']=='native_benchmark'}
quality={'raw_profile_sha256':hashlib.sha256((raw/'profile.json.gz').read_bytes()).hexdigest(),'sample_units':profile['meta']['sampleUnits'],'profile_interval_ms':profile['meta']['interval'],
 'threads':[{'name':t['name'],'tid':t['tid'],'rows':t['samples']['length'],'first_sample_time_ms':t['samples']['time'][0]if t['samples']['length']else None,'first_cpu_delta_us':t['samples']['threadCPUDelta'][0]if t['samples']['length']else None}for t in profile['threads']if t['pid']in nativepid],
 'cpu_delta_us':total,'cpu_delta_once':True,'wall_sample_weight':analysis['totals']['wall_sample_weight'],'totals':analysis['totals'],
 'unresolved_leaf_percent':100*analysis['totals']['unresolved_leaf_cpu_us']/total,'any_unresolved_stack_percent':100*analysis['totals']['any_unresolved_stack_cpu_us']/total,
 'limits':['Whole-launch sample scope cannot be cropped precisely to native throughput timer with these raw files.','CPU deltas are interval observations assigned to sampled stacks; first intervals/startup, coalesced waits and unknown frames limit attribution.','No explicit lost/truncated warning appears in captured stderr; unmarked stack truncation/capture loss cannot be quantified from this profile.','Mac threadCPUDelta is not Linux cycles:u; host workloads also differ.','Native debug0 limits source/inline attribution. All raw addresses/query responses and unknowns retained.']}
workload={'exit':0,'profiled_diagnostic_only':True,'summary':s,'group_outcomes':groups,'counts':dict(counts),'nearest_rank_quantiles':True,'scope':'Completed rates use native elapsed; during rates use8s horizon; per-group completed denominators share hot drain. Six capacity/six provider readbacks,frozen validator,conservation,recovery,retries,joins and active maintenance pass. No unprofiled improvement/default acceptance.'}
(job/'QUALITY.json').write_text(json.dumps(quality,indent=2)+'\n');(job/'WORKLOAD_READOUT.json').write_text(json.dumps(workload,indent=2)+'\n')
print(json.dumps({'exit':0,'all_counter_admission':contexts['all_counter_admission'],'groups':groups,'sample_rows':analysis['totals']['samples'],'cpu_us':total,'wall_sample_weight':analysis['totals']['wall_sample_weight']},indent=2))
