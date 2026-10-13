"""Summarize all frozen Mac incremental matched trials, including regressions."""
import json,statistics,sys
from pathlib import Path
raw,out=map(Path,sys.argv[1:]);v=json.loads((raw/'RESULTS.json').read_text())
assert len(v['cases'])==18 and v['source_artifact_guards'] and v['every_original_guard']
cells=[]
for clients,batch in [(1,1),(4096,4)]:
 cell={'clients':clients,'batch':batch,'variants':{},'pairs':[]}
 for variant in ['baseline','candidate','untouched']:
  cases=[c for c in v['cases'] if c['clients']==clients and c['batch']==batch and c['variant']==variant]
  assert len(cases)==3
  rows=[c['json'] for c in cases]
  cell['variants'][variant]={
   'median_completed_originals_s':statistics.median(x['operations_per_second'] for x in rows),
   'median_request_p99_ns':statistics.median(x['request_p99_ns'] for x in rows),
   'trials':[{'trial':c['trial'],'completed_originals_s':c['json']['operations_per_second'],
              'request_p99_ns':c['json']['request_p99_ns'],'seconds':c['json']['seconds'],
              'diagnostics':c['json'].get('diagnostics')} for c in cases],
  }
 for trial in [1,2,3]:
  pair={c['variant']:c['json'] for c in v['cases'] if c['trial']==trial and c['clients']==clients and c['batch']==batch}
  cell['pairs'].append({'trial':trial,
    'candidate_baseline_completed_ratio':pair['candidate']['operations_per_second']/pair['baseline']['operations_per_second'],
    'candidate_baseline_p99_ratio':pair['candidate']['request_p99_ns']/pair['baseline']['request_p99_ns'],
    'candidate_upstream_completed_ratio':pair['candidate']['operations_per_second']/pair['untouched']['operations_per_second'],
  })
 for metric in ['completed','p99']:
  cell[f'median_paired_candidate_baseline_{metric}_ratio']=statistics.median(x[f'candidate_baseline_{metric}_ratio'] for x in cell['pairs'])
 cells.append(cell)
out.write_text(json.dumps({'scope':v['scope'],'cases':18,'originals_per_case':131072,'warmup_originals_per_case':1024,
 'method':'absolute metrics are medians of three trials; ratios are computed within each trial before taking median; no trial removed',
 'limitations':['volatile minimized store, zero-byte payload, in-process actors, one host; no durable-service or wrapper416 claim',
                'only two client/batch cells and short runs; no published OpenRaft floor qualification',
                'original pinned upstream control exposes original completion/latency counts but not the additional per-replica diagnostics'],
 'cells':cells},indent=2)+'\n')
