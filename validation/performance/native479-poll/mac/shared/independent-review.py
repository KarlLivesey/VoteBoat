"""Independent numerical 479 review. Reads retained data; launches nothing."""
import collections,gzip,hashlib,json,re
from pathlib import Path
base=Path(__file__).resolve().parent;inputs={}
def read(p):
 inputs[str(p)]={'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size}
 return p.read_bytes()
def load(p):return json.loads(read(p))
mac=base/'mac';profile=json.loads(gzip.decompress(read(mac/'raw/profile.json.gz')))
symbols={(r['library'],r['address']):r for r in load(mac/'symbolication/ADDRESS_SYMBOLS.json')}
assert profile['meta']['sampleUnits']['threadCPUDelta']=='µs'
cpu=collections.Counter();flat=collections.Counter();threads=[]
for thread in profile['threads']:
 samples=thread['samples'];st=thread['stackTable'];fr=thread['frameTable'];fn=thread['funcTable'];res=thread['resourceTable'];cache={}
 assert samples['weightType']=='samples'
 def name(i):
  f=fr['func'][i];r=fn['resource'][f];lib=res['lib'][r]if r is not None and r>=0 else None
  return symbols.get((lib,fr['address'][i]),{}).get('function','[unresolved]')
 def chain(index):
  if index in cache:return cache[index]
  result=[];seen=set();cursor=index
  while cursor is not None:
   assert cursor not in seen and 0<=cursor<st['length'];seen.add(cursor)
   result.append(name(st['frame'][cursor]));cursor=st['prefix'][cursor]
  cache[index]=result;return result
 tcpu=0
 for i,stack in enumerate(samples['stack']):
  delta=samples['threadCPUDelta'][i]or 0;assert isinstance(delta,int)and delta>=0
  tcpu+=delta;cpu['all_us']+=delta;cpu['rows']+=1;cpu['wall_weight']+=samples['weight'][i]
  if stack is None:cpu['null_stack_us']+=delta;continue
  ns=chain(stack);flat[ns[0]]+=delta
  if '[unresolved]'in ns:cpu['any_unknown_us']+=delta
  if ns[0]=='[unresolved]':cpu['unknown_leaf_us']+=delta
  matches=[n for n in ns if n.startswith('<voteboat::application::Counter as voteboat::application::ProposalAdmission>::validate_proposal::<')]
  if matches:
   cpu['counter_admission_union_us']+=delta
   cpu['counter_ready_us'if any('check_ready_execution'in n for n in matches)else 'counter_submit_us']+=delta
 threads.append({'name':thread['name'],'tid':thread['tid'],'cpu_us':tcpu})
owner=load(mac/'analysis/SUMMARY.json');assert cpu['all_us']==owner['cpu_denominator_us']==4238938
for k,other in [('rows','samples'),('wall_weight','wall_sample_weight'),('any_unknown_us','any_unresolved_stack_cpu_us'),('unknown_leaf_us','unresolved_leaf_cpu_us')]:assert cpu[k]==owner['totals'][other]
for row in owner['self_native_functions']:
 if not row['function'].startswith('[unresolved]'):assert flat[row['function']]==row['cpu_us']
assert cpu['counter_ready_us']+cpu['counter_submit_us']==cpu['counter_admission_union_us']
linux=base/'linux';owner_linux=load(linux/'readouts/ANALYSIS.json');pid=load(linux/'capture/metadata/RESOURCES.json')['pid']
head=re.compile(r'^\s*(.*?) (\d+)/(\d+) ([\d.]+):\s+(\d+) cycles:u:\s*(.*)$')
def frame(s):
 text,dso=s.strip().rsplit(' (',1);assert dso.endswith(')');address,symbol=text.split(' ',1)
 return address,symbol,dso[:-1]
blocks=read(linux/'readouts/stacks.stdout').decode().strip().split('\n\n');leaves=read(linux/'readouts/leaves.stdout').decode().splitlines();leaves=[x for x in leaves if x.strip()]
assert len(blocks)==len(leaves)
quality=collections.Counter();all_period=0;leaf_totals=collections.Counter()
for block,line in zip(blocks,leaves,strict=True):
 ls=block.splitlines();bh=head.fullmatch(ls[0]);lh=head.fullmatch(line);assert bh and lh
 assert bh.groups()[:5]==lh.groups()[:5]
 period=int(lh[5]);all_period+=period
 if int(lh[2])!=pid:continue
 leaf=frame(lh[6]);frames=[frame(x)for x in ls[1:]]
 if frames and frames[0][0]==leaf[0]:frames[0]=leaf
 else:frames.insert(0,leaf)
 quality['native_samples']+=1;quality['native_period']+=period;leaf_totals[(leaf[1],leaf[2])]+=period
 if len(frames)==1:quality['single_frame_period']+=period
 if leaf[1]=='[unknown]'or leaf[1].startswith('0x'):quality['unresolved_leaf_period']+=period
assert all_period==owner_linux['all_period_weight']==5123877853
assert quality['native_samples']==owner_linux['native_samples']==2457 and quality['native_period']==owner_linux['native_period_weight']==5017802724
assert quality['single_frame_period']==owner_linux['quality']['single_frame_weight']
assert quality['unresolved_leaf_period']==owner_linux['quality']['unresolved_leaf_weight']
assert leaf_totals==collections.Counter({(r['symbol'],r['dso']):r['period_weight']for r in owner_linux['self']})
result={'exit':0,'scope':'Independent raw numerical/profile conversion review only; no captures, tools, production/source edits or policy changes. Whole-launch platform sampling scopes differ.','inputs':inputs,
 'mac':{'raw_cpu_and_resolved_leaf_match_owner':True,'weights':dict(cpu),'threads':threads,'counter_admission_percent':100*cpu['counter_admission_union_us']/cpu['all_us'],'ready_execution_percent':100*cpu['counter_ready_us']/cpu['all_us'],'submission_percent':100*cpu['counter_submit_us']/cpu['all_us'],'interval_wait_stack_cpu_us':flat['semaphore_timedwait_trap'],'limits':'threadCPUDelta is an interval attributed to one sampled stack, not instruction-level CPU. Coalesced wall weights are separate and never multiplied. Debug0 inline detail and27.914% any-unresolved ancestry limit caller certainty;1.137% leaf unresolved. No null/malformed stacks observed; implicit truncation cannot be counted.'},
 'linux':{'raw_join_denominator_quality_resolved_leaf_match_owner':True,'all_period':all_period,'quality':dict(quality),'limits':'Offer-capped maximum65 pending,1022.769 completed/s; not478 backlog.92.961% native period single-frame and10.069% unresolved leaf. Pending-map leaf callers mostly missing; no admission-vs-reconcile attribution. Corrected final-DSO symbol parser preserves full Rust tuple names.'},
 'decision':'Mac sampled Counter reservation/dedup validation confirms the source-grounded repeated-work target. A lazy batch-local context can reduce ready-prefix validation; submission remains separate. Do not change defaults or claim universal/fullP9 gain before fresh paired outcome/tail/refusal/skew measurements.'}
(base/'independent-review.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({'exit':0,'mac_cpu_us':cpu['all_us'],'counter_ready_percent':result['mac']['ready_execution_percent'],'counter_submit_percent':result['mac']['submission_percent'],'linux_all_period':all_period,'linux_native_period':quality['native_period'],'inputs':len(inputs)}))
