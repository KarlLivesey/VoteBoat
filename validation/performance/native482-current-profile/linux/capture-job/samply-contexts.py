"""Exact-frame native context unions from the existing saved Samply import."""
from pathlib import Path
from collections import Counter,defaultdict
import gzip,hashlib,json,re
job=Path(__file__).resolve().parent;folder=job/'samply-symbolication/data'
load=lambda p:json.loads(Path(p).read_text());save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
profile=json.load(gzip.open(job/'readouts/native.json.gz','rt'));symbols={(r['library'],r['address']):r for r in load(folder/'ADDRESS_SYMBOLS.json')}
pid=load(job/'capture/metadata/RESOURCES.json')['pid'];branch=str(pid)+'.1';threads=[t for t in profile['threads']if str(t['pid'])==branch]
header=re.compile(r'^\s*(.*?) (\d+)/(\d+) ([\d.]+):\s+(\d+) cycles:u:')
raw=defaultdict(list)
for line in(job/'readouts/leaves.stdout').read_text().splitlines():
 m=header.match(line);assert m
 comm,p,t,stamp,weight=m.groups()
 if int(p)==pid and comm!='python3.14':raw[int(t)].append((float(stamp),int(weight)))
contexts={
 'OperationId_set_insert':lambda n:n.startswith('<alloc::collections::btree::map::BTreeMap<voteboat::identity::OperationId, alloc::collections::btree::set_val::SetValZST>>::insert'),
 'Counter_applied_map_get':lambda n:n.startswith('<alloc::collections::btree::map::BTreeMap<voteboat::identity::OperationId, ([u8; 8], voteboat::application::CounterOutcome)>>::get'),
 'pending_client_btree':lambda n:n.startswith('<alloc::collections::btree::')and'runtime::clients::Pending<'in n,
 'Counter_validate_proposal':lambda n:n.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal::'),
 'client_reconcile':lambda n:n.startswith('<voteboat::runtime::clients::ClientRouter<')and'>::reconcile::'in n,
 'check_ready_execution':lambda n:'::check_ready_execution::'in n,
 'ready_collect':lambda n:'::ready_batch::'in n and'::collect::'in n,
 'client_check_submit':lambda n:n.startswith('<voteboat::runtime::clients::ClientRouter<')and'>::check_submit<'in n,
 'core_raft':lambda n:n.startswith('<voteboat::raft::')or n.startswith('voteboat::raft::'),
}
self_count=Counter();self_period=Counter();inclusive_count=Counter();inclusive_period=Counter();folded_period=Counter();folded_count=Counter();frame_names={};counts=Counter();weights=Counter();leaf_counts=Counter();leaf_weights=Counter();caller=defaultdict(Counter);qualities=Counter();den=weight_den=0;offset=None;maximum_delta=0
for t in threads:
 tid=int(str(t['tid']).split('.')[0]);samples=t['samples'];assert samples['weight']==[1]*samples['length']and len(raw[tid])==samples['length'],(tid,len(raw[tid]),samples['length'])
 for i,stack in enumerate(samples['stack']):
  stamp,period=raw[tid][i]
  if offset is None:offset=stamp*1000-samples['time'][i]
  delta=abs(stamp*1000-(samples['time'][i]+offset));maximum_delta=max(maximum_delta,delta);assert delta<=.0011,delta
  frames=[]
  while stack is not None:
   frame=t['stackTable']['frame'][stack];func=t['frameTable']['func'][frame];resource=t['funcTable']['resource'][func];lib=t['resourceTable']['lib'][resource]if resource is not None and resource>=0 else None;addr=t['frameTable']['address'][frame];r=symbols.get((lib,addr),{});frames.append(r.get('function','[unresolved] '+t['stringArray'][t['funcTable']['name'][func]]));stack=t['stackTable']['prefix'][stack]
  assert frames;den+=1;weight_den+=period
  self_count[frames[0]]+=1;self_period[frames[0]]+=period
  for n in set(frames):inclusive_count[n]+=1;inclusive_period[n]+=period
  ids=[]
  for n in reversed(frames):
   code=hashlib.sha256(n.encode()).hexdigest()[:16];assert code not in frame_names or frame_names[code]==n;frame_names[code]=n;ids.append(code)
  folded_period[';'.join(ids)]+=period;folded_count[';'.join(ids)]+=1
  for label,yes in [('single_frame',len(frames)==1),('synthetic_fun_leaf',frames[0].startswith('fun_')),('any_synthetic_fun',any(n.startswith('fun_')for n in frames)),('unresolved_leaf',frames[0].startswith('[unresolved]')),('any_unresolved',any(n.startswith('[unresolved]')for n in frames))]:
   if yes:qualities[label+'_count']+=1;qualities[label+'_period']+=period
  for label,predicate in contexts.items():
   if any(predicate(n)for n in frames):counts[label]+=1;weights[label]+=period
   if predicate(frames[0]):
    leaf_counts[label]+=1;leaf_weights[label]+=period;caller[label][frames[1]if len(frames)>1 else'[no captured caller]']+=period
  pending=any(contexts['pending_client_btree'](n)for n in frames)
  if pending:
   for label,predicate in contexts.items():
    if any(predicate(n)for n in frames):counts['pending_intersection_'+label]+=1;weights['pending_intersection_'+label]+=period
preexec=[(int(m.group(5)))for line in(job/'readouts/leaves.stdout').read_text().splitlines()if(m:=header.match(line))and int(m.group(2))==pid and m.group(1)=='python3.14']
assert den==load(job/'readouts/ANALYSIS.json')['native_samples']-len(preexec)and weight_den==load(job/'readouts/ANALYSIS.json')['native_period_weight']-sum(preexec)
rank=lambda c,other:[{'function':n,'period_weight':w,'period_percent':w*100/weight_den,'sample_count':other[n],'sample_count_percent':other[n]*100/den}for n,w in c.most_common()]
save(folder/'FRAME_NAMES.json',frame_names)
(folder/'native.period.folded').write_text(''.join(f'{s} {w}\n'for s,w in folded_period.most_common()))
(folder/'native.count.folded').write_text(''.join(f'{s} {w}\n'for s,w in folded_count.most_common()))
result={'native_exec_branch':branch,'sample_count':den,'period_weight':weight_den,'preexec_python_samples_excluded':len(preexec),'preexec_python_period_excluded':sum(preexec),'join':'Native-exec Samply samples map1:1 to nativePID perf leaves excluding actually observed preexec Python samples, by perTID ordinal plus every timestamp difference<=1.1us. No sample/cycle weights invented.','maximum_timestamp_difference_ms':maximum_delta,'contexts':{k:{'count':counts[k],'sample_count_percent':counts[k]*100/den,'period_weight':weights[k],'period_percent':weights[k]*100/weight_den,'self_count':leaf_counts[k],'self_period_weight':leaf_weights[k]}for k in counts},'quality':dict(qualities),'self':rank(self_period,self_count),'inclusive':rank(inclusive_period,inclusive_count),'direct_caller_period_weights':{k:[{'function':n,'period_weight':w,'percent_of_context_leaf_period':w*100/leaf_weights[k]}for n,w in c.most_common()]for k,c in caller.items()},'limits':['Physical full-frame exact-name unions only; no function/type-name substring is promoted to a caller. Inclusive/intersection contexts overlap.','Samply importer unwinds saved16KiB stacks much more deeply than this perf/libdw conversion; debug0 alone was not an adequate explanation for perf ancestry loss.','fun_<hex> functions are synthetic/unnamed and retained separately; debug0 still limits source/inlines. No complete-stack or zero-truncation claim.','Current loaded profile reached4096 pending and8219 applied/7781 window refusals. Whole-launch samples include startup, warmup, drain, recovery and cleanup. Thread names do not identify leader ownership.','Sample-count shares differ from perf-period shares. Both provided; neither is elapsed CPU time or blocked disk waiting.']}
save(folder/'CONTEXTS.json',result)
short=lambda n:n if len(n)<160 else n[:90]+'…'+n[-60:]
print(json.dumps({**{k:result[k]for k in ['native_exec_branch','sample_count','period_weight','preexec_python_samples_excluded','preexec_python_period_excluded','maximum_timestamp_difference_ms','contexts','quality']},'top_self':[{**r,'function':short(r['function'])}for r in result['self'][:15]],'top_inclusive':[{**r,'function':short(r['function'])}for r in result['inclusive'][:15]]},indent=2))
