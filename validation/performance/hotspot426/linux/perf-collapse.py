from pathlib import Path
from collections import Counter
import json,re
base=Path(__file__).resolve().parent/'perf-quiet'
head=re.compile(r'^\s*(.*?)\s+(\d+)\s+(\d+\.\d+):\s+(\d+)\s+cycles:u:\s*(.*)$')
frame=re.compile(r'^\s*([0-9a-f]+)\s+(.*?)\s+\((.*?)\)\s*$')
def label(match):
 pc,symbol,dso=match.groups()
 if symbol=='[unknown]': symbol=f'[unresolved {Path(dso).name}+ip={pc}]'
 else:symbol=re.sub(r'\+0x[0-9a-f]+$','',symbol)
 return symbol.replace(';',':').replace('\n',' ')
report={}
for name in ('bulk','singleton'):
 flat=[]
 for line in (base/(name+'.flat.txt')).read_text().splitlines():
  m=head.match(line)
  if not m: assert not line.strip(),line;continue
  comm,tid,time,period,tail=m.groups();f=frame.match(tail)
  assert f,(name,line)
  flat.append(((comm,tid,time,int(period)),label(f)))
 chains=[]
 for block in (base/(name+'.stacks.txt')).read_text().split('\n\n'):
  lines=block.splitlines()
  if not lines:continue
  m=head.match(lines[0]);assert m,(name,lines[0])
  comm,tid,time,period,tail=m.groups();assert not tail.strip()
  frames=[]
  for line in lines[1:]:
   if not line.strip():continue
   f=frame.match(line);assert f,(name,line)
   frames.append(label(f))
  chains.append(((comm,tid,time,int(period)),frames))
 assert len(flat)==len(chains),(name,len(flat),len(chains))
 folded=Counter();self=Counter();inclusive=Counter();threads=Counter();raw_samples=0;shallow_weight=0;leaf_fallback_weight=0;flat_mismatch_weight=0
 for (key,leaf),(key2,frames) in zip(flat,chains):
  assert key==key2,(key,key2)
  comm,tid,time,period=key;raw_samples+=1;threads[comm]+=period
  if len(frames)<=2:shallow_weight+=period
  if not frames:frames=[leaf];leaf_fallback_weight+=period
  elif frames[0]!=leaf:flat_mismatch_weight+=period
  # Never invent callers. Mark boundary explicitly for all incomplete recordings.
  stack=[comm,'[recorded stack boundary; callers may be missing]',*reversed(frames)]
  folded[';'.join(stack)]+=period;self[leaf]+=period
  for f in set(frames):inclusive[f]+=period
 total=sum(threads.values());assert total==sum(folded.values())==sum(self.values())
 (base/(name+'.complete.folded')).write_text(''.join(f'{k} {v}\n' for k,v in sorted(folded.items())))
 rank=lambda counter:[{'function':f,'user_cycles':n,'percentage':100*n/total} for f,n in counter.most_common(30)]
 r=json.loads((base/(name+'.stdout')).read_text());assert r['completed_originals']==r['settings']['operations'];assert r['applied_frontiers']==[r['completed_originals']+1025]*3
 report[name]={'denominator':'recorded cycles:u sample periods, whole process including warmup/setup/latency sorting/shutdown; not leader-only elapsed or CPU duration','recorded_user_cycle_weight':total,'raw_sample_events':raw_samples,'threads':dict(threads),'at_most_two_recorded_frames_cycle_percentage':100*shallow_weight/total,'empty_chain_flat_leaf_fallback_cycle_percentage':100*leaf_fallback_weight/total,'flat_vs_chain_leaf_mismatch_cycle_percentage':100*flat_mismatch_weight/total,'self_functions':rank(self),'inclusive_recorded_functions':rank(inclusive),'completed_originals':r['completed_originals'],'completed_requests':r['completed_requests'],'profiled_diagnostic_originals_per_second':r['completed_originals']/r['seconds'],'request_p99_ns':r['request_p99_ns'],'lost_sample_count':14 if name=='bulk' else 'see perf report/header; not inferred','limitation':'DWARF caller unwinding is incomplete; no precise full-path or inline attribution claimed. Flat sampled leaf comes from perf -G. Unknown libc symbol addresses remain explicit.'}
 print(name,'cycles',total,'samples',raw_samples,'shallow%',round(100*shallow_weight/total,2),'flatfallback%',round(100*leaf_fallback_weight/total,2))
 print([(x['function'][:115],round(x['percentage'],2)) for x in report[name]['self_functions'][:6]])
(base/'CPU_SUMMARY.json').write_text(json.dumps(report,indent=2)+'\n')
