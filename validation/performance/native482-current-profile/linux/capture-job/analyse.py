"""Read native-only perf periods and exact stacks, retaining unresolved identities."""
from pathlib import Path
from collections import Counter
import hashlib,json,re
job=Path(__file__).resolve().parent;out=job/'readouts'
load=lambda p:json.loads(Path(p).read_text());sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
resource=load(job/'capture/metadata/RESOURCES.json');pid=resource['pid'];samples=[]
header=re.compile(r'^(.*?) (\d+)/(\d+) ([\d.]+):\s+(\d+) cycles:u:\s*$')
frame=re.compile(r'^\s*([0-9a-f]+) (.*) \(([^()]*)\)$')
for block in(out/'stacks.stdout').read_text().strip().split('\n\n'):
 lines=block.splitlines();m=header.fullmatch(lines[0]);assert m,lines[0]
 comm,p,t,stamp,weight=m.groups();frames=[]
 for line in lines[1:]:
  f=frame.fullmatch(line);assert f,line
  address,symbol,dso=f.groups();frames.append({'address':address,'symbol':symbol,'dso':dso})
 samples.append({'comm':comm,'pid':int(p),'tid':int(t),'time_seconds':float(stamp),'period_weight':int(weight),'frames':frames})
leaf_header=re.compile(r'^\s*(.*?) (\d+)/(\d+) ([\d.]+):\s+(\d+) cycles:u:\s*(.*)$')
leaf_lines=[line for line in(out/'leaves.stdout').read_text().splitlines()if line.strip()];assert len(leaf_lines)==len(samples)
matched=0
for sample,line in zip(samples,leaf_lines):
 m=leaf_header.fullmatch(line);assert m,line
 comm,p,t,stamp,weight,leaf=m.groups()
 assert(int(p),int(t),float(stamp),int(weight))==(sample['pid'],sample['tid'],sample['time_seconds'],sample['period_weight'])
 f=frame.fullmatch(leaf);assert f,leaf
 address,symbol,dso=f.groups();leaf={'address':address,'symbol':symbol,'dso':dso}
 sample['raw_callchain_frames']=sample['frames']
 sample['raw_callchain_empty']=not sample['frames']
 if sample['frames']and sample['frames'][0]['address']==leaf['address']:
  sample['frames']=[leaf,*sample['frames'][1:]];matched+=1
 else:sample['frames']=[leaf,*sample['frames']]
selected=[s for s in samples if s['pid']==pid];total=sum(s['period_weight']for s in samples);den=sum(s['period_weight']for s in selected)
selfs=Counter();inclusive=Counter();callers=Counter();threads={};quality=Counter();folded=Counter();folded_counts=Counter();symbols={}
def identity(f):
 key=(f['symbol'],f['dso']);code=hashlib.sha256(json.dumps(key).encode()).hexdigest()[:16]
 assert code not in symbols or(symbols[code]['symbol'],symbols[code]['dso'])==key
 symbols[code]={'symbol':key[0],'dso':key[1]};return code
def unresolved(f):return f['symbol']=='[unknown]'or f['symbol'].startswith('0x')
for sample in selected:
 w=sample['period_weight'];frames=sample['frames'];ids=[identity(f)for f in frames]or[identity({'symbol':'[no captured frame]','dso':'[none]'})]
 selfs[ids[0]]+=w
 for k in set(ids):inclusive[k]+=w
 for child,parent in set(zip(ids,ids[1:])):callers[(parent,child)]+=w
 folded[';'.join(reversed(ids))]+=w;folded_counts[';'.join(reversed(ids))]+=1
 c=threads.setdefault((sample['tid'],sample['comm']),{'samples':0,'period_weight':0});c['samples']+=1;c['period_weight']+=w
 quality['samples']+=1;quality['period_weight']+=w
 for k,yes in [('raw_callchain_empty',sample['raw_callchain_empty']),('no_captured_frame',not frames),('single_frame',len(frames)==1),('unresolved_leaf',not frames or unresolved(frames[0])),('any_unresolved_frame',any(unresolved(f)for f in frames)),('known_entry_context',any(f['symbol']in['_start','main','__libc_start_main']or'::thread_start'in f['symbol']for f in frames))]:
  if yes:quality[k+'_samples']+=1;quality[k+'_weight']+=w
 quality['max_frames']=max(quality['max_frames'],len(frames))
rows=lambda c:[{'id':k,'period_weight':v,'native_percent':v*100/den,'symbol':symbols[k]['symbol'],'dso':symbols[k]['dso']}for k,v in c.most_common()]
# Explicit symbol predicates; inclusive unions overlap and are not an exhaustive
# stage partition. Missing optimized frames mean unknown, not zero work.
predicates={
 'pending_client_btree':lambda n:'runtime::clients::Pending<'in n and n.startswith('<alloc::collections::btree::'),
 'Counter_validate_proposal':lambda n:n.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal::'),
 'ready_execution_or_collection':lambda n:'check_ready_execution'in n or'::ready_batch::'in n,
 'pending_receipt':lambda n:'pending_receipt'in n,
 'client_reconcile':lambda n:'runtime::clients::ClientRouter<'in n and'>::reconcile::'in n,
 'peer_poll':lambda n:'::poll_peer_io::'in n or'::peers::PeerRoster<'in n and'>::poll'in n,
 'peer_reconcile':lambda n:'::reconcile_peer'in n or'::connection_replicas'in n,
 'LogEntry_clone_drop':lambda n:'voteboat::log::LogEntry'in n and(n.startswith('<alloc::vec::Vec<')and'::clone'in n or n.startswith('core::ptr::drop_glue::')),
 'allocator_named':lambda n:n in ['malloc','calloc','realloc','free','cfree']or'::__rust_alloc'in n or'::__rust_dealloc'in n,
 'core_raft':lambda n:n.startswith('<voteboat::raft::')or n.startswith('voteboat::raft::'),
 'native_log_worker':lambda n:'voteboat::native::worker::log'in n,
}
contexts={}
for name,predicate in predicates.items():
 leaf=[s for s in selected if s['frames']and predicate(s['frames'][0]['symbol'])];inside=[s for s in selected if any(predicate(f['symbol'])for f in s['frames'])]
 contexts[name]={'self_samples':len(leaf),'self_weight':sum(s['period_weight']for s in leaf),'self_native_percent':sum(s['period_weight']for s in leaf)*100/den,'inclusive_samples':len(inside),'inclusive_weight':sum(s['period_weight']for s in inside),'inclusive_native_percent':sum(s['period_weight']for s in inside)*100/den}
leaf_context_callers={}
for context,predicate in predicates.items():
 leaf_samples=[s for s in selected if s['frames']and predicate(s['frames'][0]['symbol'])];ancestry=Counter();direct=Counter()
 for sample in leaf_samples:
  frames=sample['frames'];w=sample['period_weight'];direct[identity(frames[1])if len(frames)>1 else '[no captured caller]']+=w
  for label,yes in [('reconcile_ancestor',any('runtime::clients::ClientRouter<'in f['symbol']and'>::reconcile'in f['symbol']for f in frames[1:])),('admission_check_ancestor',any('check_ready_execution'in f['symbol']for f in frames[1:])),('fixture_measure_ancestor',any('native_benchmark::offered::measure'in f['symbol']for f in frames[1:])),('no_captured_caller',len(frames)<2),('unresolved_direct_caller',len(frames)>1 and unresolved(frames[1]))]:
   if yes:ancestry[label]+=w
 leaf_context_callers[context]={'leaf_weight':sum(s['period_weight']for s in leaf_samples),'observed_ancestor_weights':dict(ancestry),'direct_callers':[{'id':k,'period_weight':v,'leaf_context_percent':v*100/sum(s['period_weight']for s in leaf_samples)}for k,v in direct.most_common()]}
report=(out/'self.stdout').read_text();assert 'Total Lost Samples: 0'in report
approx=int(re.search(r'Event count \(approx\.\): (\d+)',report).group(1));assert approx==total,(approx,total)
s=load(job/'VALIDATION.json')['settings'];d=job/'capture/perf.data'
result={'native_pid':pid,'all_samples':len(samples),'all_period_weight':total,'native_samples':len(selected),'native_period_weight':den,'native_weight_percent_of_whole_launch':den*100/total,'perf_report_approx_event_count':approx,'lost_samples_reported':0,'quality':dict(quality),'sampled_leaf_matches_first_unwind_address':matched,'leaf_method':'Explicit perf script -G sampled IP/symbol joined one-to-one by pid/tid/time/period to original DWARF callchain dump. Prefer explicit sampled leaf; remove duplicate first unwind address only when exactly equal. Empty callchain is missing ancestry, not a missing leaf sample.','threads':[{'tid':k[0],'comm':k[1],**v,'native_percent':v['period_weight']*100/den}for k,v in sorted(threads.items())],'self':rows(selfs),'inclusive':rows(inclusive),'caller_edges':[{'parent':k[0],'child':k[1],'period_weight':v,'native_percent':v*100/den}for k,v in callers.most_common()],'leaf_context_callers':leaf_context_callers,'symbol_context_unions':contexts,'profiled_summary':s,'whole_native_resources':resource,'raw_perf':{'path':str(d),'sha256':sha(d),'bytes':d.stat().st_size},'limits':['cycles:u period-weighted samples approximate sampled user CPU distribution, not elapsed CPU time, call counts or exact cycle counts. Counts also retained. Kernel execution, disk waiting and off-CPU time are excluded.','Whole launch includes warmup/startup/history/maintenance/recovery/cleanup and inherited resource-owner Python/env samples. Native-only attribution selects exact native PID across all worker TIDs, excluding wrapper samples. No exact measurement-only sample boundary was recorded.','Native480 debug0 means optimized/inlined/source detail and caller unwinding can be missing. Single-frame/unresolved indicators are reported; text conversion does not expose a definitive truncated-stack count, so none is invented. Raw16KiB DWARF records retained.','Top symbols and explicit type/name context predicates are observed function identities, not full source-line attribution. Inclusive contexts and caller edges overlap and must not be summed. Missing frames do not disprove the cost of admission.','Current loaded16000@2000 q64 capture is a profiling diagnostic, not unprofiled throughput acceptance. Actual inflight/refusal/latency totals are in profiled_summary; no prior479 scope values are reused.']}
save(out/'ANALYSIS.json',result);save(out/'SYMBOLS.json',symbols)
(out/'native.folded').write_text(''.join(f'{k} {v}\n'for k,v in folded.most_common()))
(out/'native.samples.folded').write_text(''.join(f'{k} {v}\n'for k,v in folded_counts.most_common()))
save(out/'PROVENANCE.json',{'analyse_sha256':sha(__file__),'inputs':{str(p):{'sha256':sha(p),'bytes':p.stat().st_size}for p in[out/'stacks.stdout',out/'leaves.stdout',out/'self.stdout',job/'VALIDATION.json',job/'capture/metadata/RESOURCES.json',d]},'folded_format':'Root-to-leaf frame IDs (full exact symbol+DSO in SYMBOLS.json); native.folded uses raw period weights and native.samples.folded sample counts. No SVG/browser required.'})
print(json.dumps({'all_samples':len(samples),'native_samples':len(selected),'all_weight':total,'native_weight':den,'quality':dict(quality),'contexts':contexts,'top_self':[{'native_percent':r['native_percent'],'symbol':r['symbol']if len(r['symbol'])<180 else r['symbol'][:95]+'...'+r['symbol'][-75:]}for r in result['self'][:12]]},indent=2))
