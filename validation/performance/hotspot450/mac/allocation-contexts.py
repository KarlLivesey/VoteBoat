"""One-off physical-frame analysis of existing raw profiles; no recording/build."""
import gzip,json,importlib.util
from collections import Counter
from pathlib import Path
root=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('converter',root/'samply-collapse.py')
converter=importlib.util.module_from_spec(spec);spec.loader.exec_module(converter)
# Exact canonical native names keep receive_append distinct from receive_appended.
labels={'workload_client_write':'<voteboat_matched_framework::workload::Client>::write::{closure#0}', 'client_loop':'voteboat_matched_framework::workload::client_loop::{closure#0}', 'Actor_requests':'<voteboat_matched_framework::voteboat_engine::actor::Actor>::requests', 'Raft_append_for':'<voteboat::raft::Raft>::append_for'}
result={'scope':'Physical sampled stack contexts for unresolved malloc-library leaves or resolved allocation/growth functions. Library identity does not identify allocation vs deallocation, and these are not operation allocation counts or allocator causal costs. Contexts overlap; wait/delta/alias limitations remain.', 'cases':{}}
for cell in ['bulk','singleton']:
 p=json.loads(gzip.decompress((root/(cell+'-profile.json.gz')).read_bytes()))
 side=json.loads((root/(cell+'-profile.json.syms.json')).read_text());strings=side['string_table']
 symbols={(lib['debug_name'],converter.normalized_uuid(lib['debug_id'])):{addr:lib['symbol_table'][slot] for addr,slot in lib['known_addresses']} for lib in side['data']}
 weights={k:Counter() for k in ['unresolved_malloc_library_leaf','resolved_allocation_leaf']};callers={k:Counter() for k in weights};denom=0
 for t in p['threads']:
  if t['name'] not in {'voteboat-matched-framework','matched-client','matched-server'}:continue
  frames=t['frameTable'];functions=t['funcTable'];resources=t['resourceTable'];stacks=t['stackTable'];cache={};stack_cache={}
  def frame(i):
   if i in cache:return cache[i]
   f=frames['func'][i];rs=functions['resource'][f];info=None;library_name=''
   if rs is not None and rs>=0:
    li=resources['lib'][rs]
    if li is not None and li>=0:
     lib=p['libs'][li];library_name=lib['name']+'!';info=symbols.get((lib['debugName'],converter.normalized_uuid(lib['breakpadId'])),{}).get(frames['address'][i])
   name=strings[info['symbol']] if info is not None else '[unresolved] '+library_name+t['stringArray'][functions['name'][f]];cache[i]=name;return name
  def stack(i):
   if i in stack_cache:return stack_cache[i]
   rows=[];cursor=i;seen=set()
   while cursor is not None:
    assert cursor not in seen;seen.add(cursor);rows.append(frame(stacks['frame'][cursor]));cursor=stacks['prefix'][cursor]
   rows.reverse();stack_cache[i]=rows;return rows
  for idx,i in enumerate(t['samples']['stack']):
   cpu=t['samples']['threadCPUDelta'][idx]
   if cpu is None or cpu<=0:continue
   denom+=cpu
   if i is None:continue
   names=stack(i)
   selected={'unresolved_malloc_library_leaf':'[unresolved]' in names[-1] and 'libsystem_malloc' in names[-1], 'resolved_allocation_leaf':not names[-1].startswith('[unresolved]') and any(x in names[-1] for x in ['::finish_grow','::grow_one','::grow_zeroed','::alloc::alloc','malloc'])}
   predicates={key:label in names for key,label in labels.items()}
   for key,yes in selected.items():
    if not yes:continue
    weights[key]['all_cpu_us']+=cpu
    for label,value in predicates.items():
     if value:weights[key][label]+=cpu
    callers[key][names[-2] if len(names)>1 else '[root]']+=cpu
 summary=json.loads((root/(cell+'-analysis')/'SUMMARY.json').read_text());assert denom==summary['cpu_denominator_us']
 case={'cpu_denominator_us':denom,'groups':{}}
 for key,v in weights.items():
  total=v['all_cpu_us'];case['groups'][key]={'cpu_us':total,'percent_observed_cpu':100*total/denom,'physical_contexts_nonadditive':{label:{'cpu_us':v[label],'percent_observed_cpu':100*v[label]/denom,'percent_group':100*v[label]/total if total else 0} for label in [*labels]},'top_immediate_leaf_callers':[{'function':label,'cpu_us':weight,'percent_group':100*weight/total if total else 0} for label,weight in callers[key].most_common(8)]}
 result['cases'][cell]=case
 print(cell,'denom',denom)
 for key,v in case['groups'].items():print(key,v['cpu_us'],round(v['percent_observed_cpu'],3),'contexts',{k:round(x['percent_observed_cpu'],3) for k,x in v['physical_contexts_nonadditive'].items()})
(root/'ALLOCATION_CONTEXTS.json').write_text(json.dumps(result,indent=2)+'\n')
