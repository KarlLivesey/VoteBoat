"""Read actual native CSV/output and cumulative publication deltas; no run."""
import csv,json,re
from pathlib import Path
j=Path(__file__).resolve().parent
read=lambda f:list(csv.DictReader(f.open()))
r={}
for cell,count,w in [('bulk',15000,32),('singleton',4096,1)]:
 root=j/(cell+'-workload');exit=int((j/(cell+'.exit')).read_text());text=(j/(cell+'.stdout')).read_text();row={'exit':exit,'requested_originals':count,'warmup':64,'window':w,'refusals_not_separately_reported':True,'capture_scope':'whole launch incl warmup/recovery or timeout/cleanup; profiling diagnostic, not acceptance'}
 if exit==0:
  fields=dict(re.findall(r'(\w+)=([^\s]+)',text));samples=read(root/'samples.csv');assert len(samples)==count
  assert sorted(int(x['operation'])for x in samples)==list(range(65,count+65))
  assert all(int(x['value'])==int(x['operation'])and int(x['group'])==1 and int(x['completed_ns'])>=int(x['submitted_ns'])and int(x['latency_ns'])==int(x['completed_ns'])-int(x['submitted_ns'])for x in samples)
  assert fields['workers_joined']==fields['retry_verified']=='true'and int(fields['recovered_value'])==count+64 and int(fields['operations'])==count and int(fields['max_inflight'])<=w and fields['ready_visit_items_effective']=='16'and fields['core_proposal_entries']=='64'
  batches=[x for x in read(root/'proposal-batches.csv')if x['phase']=='measurement'];widths={}
  for x in batches:
   width=int(x['width']);visits=int(x['visits']);assert int(x['original_proposals'])==width*visits
   if width:widths[str(width)]=widths.get(str(width),0)+visits
  assert sum(int(k)*v for k,v in widths.items())==count
  journal=read(root/'journal.csv');deltas=[];pub=read(root/'publication.csv')
  for replica in ['1','2','3']:
   before=next(x for x in journal if x['stage']=='before_measurement'and x['replica']==replica);after=next(x for x in journal if x['stage']=='after_measurement'and x['replica']==replica)
   d={k:int(after[k])-int(before[k])for k in ['sync_calls','sync_ns','publish_calls','publish_ns','barrier_calls','barrier_ns','append_commands','errors']};d['replica']=int(replica);d['publication_steps']={}
   for step in ['open','write','file_sync','rename','directory_sync']:
    b=next(x for x in pub if x['stage']=='before_measurement'and x['replica']==replica and x['step']==step);a=next(x for x in pub if x['stage']=='after_measurement'and x['replica']==replica and x['step']==step)
    d['publication_steps'][step]={k:int(a[k])-int(b[k])for k in ['calls','errors','elapsed_ns']}
   deltas.append(d)
  row.update({'valid_measurement':True,'completed_originals':count,'all_sample_values_operations_latencies_checked':True,'applied_index_range':[min(int(x['applied_index'])for x in samples),max(int(x['applied_index'])for x in samples)],'originals_per_second':float(fields['applied_ops_s']),'p99_us':float(fields['p99_us']),'recovered_value':int(fields['recovered_value']),'recovery_retries':int(fields['recovery_retries']),'worker_joins_and_first_last_retries_verified':True,'proposal_width_histogram':widths,'journal_measurement_deltas':deltas})
 else:
  partial=read(root/'measurement-partial.csv');unresolved=read(root/'measurement-unresolved.csv');assert len(partial)==1456 and len(unresolved)==1
  row.update({'valid_measurement':False,'retained_completed_prefix':len(partial),'unresolved_originals':len(unresolved),'failure':(root/'measurement-failure.txt').read_text(),'cleanup':(root/'failure-cleanup.txt').read_text(),'throughput_and_recovery_success_unavailable':True,'unchanged_drive_deadline_seconds':120})
 r[cell]=row
(j/'COMPLETION.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
