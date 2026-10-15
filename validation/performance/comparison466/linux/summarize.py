import json,statistics,sys
from pathlib import Path
p=Path(sys.argv[1]);d=json.loads((p/'RESULTS.json').read_text());out={'scope':d['scope'],'cases':len(d['cases']),'failures':d['failures'],'cells':[]}
for clients,batch in sorted({(v["clients"],v["batch"]) for v in d["cases"]}):
 cell={'clients':clients,'batch':batch,'variants':{}}
 for variant in ['baseline','candidate','on','openraft']:
  xs=[v for v in d['cases'] if v['clients']==clients and v['variant']==variant];rs=[x['report'] for x in xs]
  cell['variants'][variant]={'rates':[r['operations_per_second'] for r in rs],'median_completed_originals_per_second':statistics.median(r['operations_per_second'] for r in rs),'median_request_p50_ms':statistics.median(r['request_p50_ns']/1e6 for r in rs),'median_request_p99_ms':statistics.median(r['request_p99_ns']/1e6 for r in rs),'request_p99_ms':[r['request_p99_ns']/1e6 for r in rs],'entry_allocation_observations':([{key:rec[key] for key in ['replica_slot','core_entry_allocations','store_entry_allocations']} for r in rs for rec in r['attribution']] if variant=='on' else 'N/A; no ON capture requested or upstream diagnostics unavailable')}
 base=cell['variants']['baseline']['rates'];candidate=cell['variants']['candidate']['rates']
 cell['paired_ratios']=[c/b for b,c in zip(base,candidate)]
 cell['paired_p99_ratios']=[c/b for b,c in zip(cell['variants']['baseline']['request_p99_ms'],cell['variants']['candidate']['request_p99_ms'])]
 cell['leader_on_phases_ms']=[{v['phase']:v['total_elapsed_ns']/1e6 for v in x['report']['attribution'][0]['phases']} for x in d['cases'] if x['clients']==clients and x['variant']=='on']
 out['cells'].append(cell)
(p/'SUMMARY.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k!='cells'},indent=2))
