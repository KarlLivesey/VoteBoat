"""Current487 saved-data Mac summary; no workloads or timer/CPU boundary mixing."""
from pathlib import Path
import json,hashlib,statistics
j=Path(__file__).resolve().parent;b=j.parent.parent
r=json.loads((b/'review/actual487-mac/READOUT.json').read_text());assert r['valid_cases']==48 and not r['failures']
median=statistics.median
cells=[]
for cell in ['singleton','moderate','loaded','skew']:
 for protocol in ['tcp','quic']:
  entry={'cell':cell,'protocol':protocol}
  records={}
  for arm in ['baseline','candidate']:
   rows=sorted([x for x in r['records']if x['cell']==cell and x['protocol']==protocol and x['arm']==arm],key=lambda x:x['trial']);assert len(rows)==3
   records[arm]=rows
   entry[arm]={'completed_s':median(x['rates']['completed_ops_s']for x in rows),'during_s':median(x['rates']['during_ops_s']for x in rows),'p99_ms':median(x['quantiles']['intended']['p99_ms']for x in rows),'window_refused':[x['statuses']['window_refused']for x in rows],'cpu_us_per_completed':median(x['wholechild_resources']['cpu_us_per_completed_original']for x in rows),'average_busy_cores':median((x['wholechild_resources']['user_seconds']+x['wholechild_resources']['system_seconds'])/x['wholechild_resources']['elapsed_seconds']for x in rows),'rss_bytes':median(x['wholechild_resources']['max_rss_bytes']for x in rows),'per_trial_resources':[{'case':x['case'],'cpu_seconds':x['wholechild_resources']['user_seconds']+x['wholechild_resources']['system_seconds'],'same_child_wall_seconds':x['wholechild_resources']['elapsed_seconds'],'completed':x['completed'],'average_busy_cores':(x['wholechild_resources']['user_seconds']+x['wholechild_resources']['system_seconds'])/x['wholechild_resources']['elapsed_seconds'],'cpu_us_per_completed':x['wholechild_resources']['cpu_us_per_completed_original']}for x in rows]}
   if cell=='skew':entry[arm]['cold']={'p99_ms':[x['groups']['2']['quantiles']['intended']['p99_ms']for x in rows],'completed':[x['groups']['2']['completed']for x in rows],'refusals':[x['groups']['2']['statuses']['window_refused']for x in rows]}
  pairs=sorted([x for x in r['pairs']if x['cell']==cell and x['protocol']==protocol],key=lambda x:x['trial'])
  entry['rate_ratios']=[x['rates']['completed_ops_s']['ratio']for x in pairs];entry['p99_ratios']=[x['latency_ratios']['intended']['p99_ms']for x in pairs];entry['cpu_per_completed_ratios']=[x['cpu_us_per_completed_original_ratio']for x in pairs]
  cells.append(entry)
out={'exit':0,'cases':48,'totals':r['totals'],'cells':cells,'raw_readout_sha256':hashlib.sha256((b/'review/actual487-mac/READOUT.json').read_bytes()).hexdigest(),'scope':'Fixed configured workers, no affinity restriction/one-core claim. Whole-child user+sys includes warmup/drain/recovery/cleanup; CPU/completed denominator counts measured applied originals. Average busy cores uses exactly the same resource-helper child wall, never native timer elapsed. Finite offered workloads, not full P9 acceptance.'}
(j/'COMPACT_READOUT.json').write_text(json.dumps(out,indent=2)+'\n')
for c in cells:
 a,z=c['baseline'],c['candidate'];print(c['cell'],c['protocol'],'PUTS/s',round(a['completed_s'],3),'->',round(z['completed_s'],3),'p99ms',round(a['p99_ms'],3),'->',round(z['p99_ms'],3),'CPUus/op',round(a['cpu_us_per_completed'],3),'->',round(z['cpu_us_per_completed'],3),'avgcores',round(a['average_busy_cores'],4),'->',round(z['average_busy_cores'],4),'rate ratios',c['rate_ratios'],'p99 ratios',c['p99_ratios'],'CPU ratios',c['cpu_per_completed_ratios'],'refused',a['window_refused'],'->',z['window_refused'])
 if 'cold'in a:print('cold',a['cold'],'->',z['cold'])
