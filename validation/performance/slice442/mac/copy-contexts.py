"""One-off physical-frame analysis of existing raw profiles; no recording/build."""
import gzip,json,importlib.util
from collections import Counter
from pathlib import Path
root=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('converter',root/'samply-collapse.py')
converter=importlib.util.module_from_spec(spec);spec.loader.exec_module(converter)
result={'scope':'Existing native442 raw physical stack unions weighted by observed threadCPUDelta us. Native Counter generic iterator type distinguishes ready versus submission monomorph; missing/inlined/unresolved frames mean this is resolved evidence, not a bound on all admission cost. Whole launch includes warmup/recovery or invalid singleton cleanup; intervals attributed to wait stacks are biased, not instruction costs.','cases':{}}
for cell in ['bulk','singleton']:
 p=json.loads(gzip.decompress((root/(cell+'-profile.json.gz')).read_bytes()))
 side=json.loads((root/(cell+'-profile.json.syms.json')).read_text());strings=side['string_table']
 symbols={(lib['debug_name'],converter.normalized_uuid(lib['debug_id'])):{addr:lib['symbol_table'][slot] for addr,slot in lib['known_addresses']} for lib in side['data']}
 weights=Counter();denom=0;thread_weights={}
 for t in p['threads']:
  if 'native_benchmark' not in t['name'] and not t['name'].startswith('voteboat-'):continue
  frames=t['frameTable'];functions=t['funcTable'];resources=t['resourceTable'];stacks=t['stackTable'];cache={};stack_cache={}
  def frame(i):
   if i in cache:return cache[i]
   f=frames['func'][i];rs=functions['resource'][f];info=None
   if rs is not None and rs>=0:
    li=resources['lib'][rs]
    if li is not None and li>=0:
     lib=p['libs'][li];info=symbols.get((lib['debugName'],converter.normalized_uuid(lib['breakpadId'])),{}).get(frames['address'][i])
   name=strings[info['symbol']] if info is not None else '[unresolved] '+t['stringArray'][functions['name'][f]];cache[i]=name;return name
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
   counter=lambda n:n.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal')
   cvs=[n for n in names if counter(n)]
   predicates={'Counter_validate_physical_union':bool(cvs),'Counter_ready_iterator_union':any('::check_ready_execution<' in n for n in cvs),'Counter_submit_iterator_union':any('::check_submit<' in n for n in cvs),'workload_physical_union':any(n.startswith('native_benchmark::workload')for n in names),'recovery_storage_union':any('::recover_storage::<' in n or n.endswith('::recover')for n in names),'verify_recovered_union':any(n.startswith('native_benchmark::verify_recovered_workload')for n in names),'Counter_apply_union':any(n.startswith('<voteboat::application::Counter as voteboat::application::StateMachine>::apply_batch')for n in names),'log_stage_union':any(n=='voteboat::log::stage_batch'for n in names),'log_apply_union':any(n=='voteboat::log::apply_batch'for n in names),'manifest_publication_union':any(n.startswith('<voteboat::native::log_store::file::FileLogIo as voteboat::native::log_store::JournalIo>::publish_manifest')for n in names),'park_wait_stack_union':any(n=='semaphore_timedwait_trap' or '::park_timeout' in n for n in names),'Counter_under_workload':bool(cvs)and any(n.startswith('native_benchmark::workload')for n in names),'Counter_under_recovery_verification':bool(cvs)and any(n.startswith('native_benchmark::verify_recovered_workload')for n in names)}
   for key,yes in predicates.items():
    if yes:weights[key]+=cpu
   row=thread_weights.setdefault(str(t['tid']),{'name':t['name'],'cpu_us':0,'unions':Counter()});row['cpu_us']+=cpu
   for key,yes in predicates.items():
    if yes:row['unions'][key]+=cpu
 summary=json.loads((root/(cell+'-analysis')/'SUMMARY.json').read_text());assert denom==summary['cpu_denominator_us']
 case={'cpu_denominator_us':denom,'unions_nonadditive':{k:{'cpu_us':v,'percent_observed_CPU':100*v/denom}for k,v in weights.items()},'threads':thread_weights}
 result['cases'][cell]=case
 print(cell,'CPU_us',denom,'unions',json.dumps(case['unions_nonadditive']))
(root/'NATIVE_CONTEXTS.json').write_text(json.dumps(result,indent=2)+'\n')
