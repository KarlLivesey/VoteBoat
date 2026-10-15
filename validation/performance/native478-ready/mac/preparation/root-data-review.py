import csv, hashlib, json, collections, math
from pathlib import Path
base=Path(__file__).resolve().parent
inputs={}; records=[]
def read(p):
 b=p.read_bytes(); inputs[str(p)]={'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b)}; return b.decode()
for host,root in [('linux',base/'linux'),('mac',base/'mac-collection/raw')]:
 terminal=json.loads(read(root/'TERMINAL.json')); assert terminal['exit']==0 and terminal['all_timing_ended']
 campaign=json.loads(read(root/'campaign/CAMPAIGN.json')); assert len(campaign['cases'])==48
 assert not campaign['post_timing_errors']
 for c in campaign['cases']:
  assert c['exit']==c['validator_exit']==c['readbacks_exit']==0
  wal=Path(c['wal_root']) if host=='linux' else root/'results'/c['name']
  s=dict(v.split('=',1) for v in read(wal/'summary.txt').split())
  rows=list(csv.DictReader(read(wal/'offers.csv').splitlines()))
  statuses=collections.Counter(r['status'] for r in rows)
  assert len(rows)==c['offers'] and len({r['operation'] for r in rows})==len(rows)
  assert set(statuses)<= {'applied','window_refused'}
  applied=[r for r in rows if r['status']=='applied']
  assert len(applied)==int(s['applied'])==int(s['admitted'])
  assert int(s['recovered_value'])==64+len(applied)
  assert s['retry_verified']==s['workers_joined']=='true'
  assert int(s['ready_visit_items_requested'])==int(s['ready_visit_items_effective'])==c['ready_items']
  elapsed=int(s['elapsed_ns']); horizon=int(s['horizon_ns'])
  assert sum(int(r['completed_ns'])<horizon for r in applied)==int(s['applied_during'])
  assert sum(int(r['dispatched_ns'])<horizon<=int(r['completed_ns']) for r in applied)==int(s['backlog_at_horizon'])
  groups={}
  for group in sorted({r['group'] for r in rows}):
   selected=[r for r in applied if r['group']==group]; offers=[r for r in rows if r['group']==group]
   lat=sorted(int(r['completed_ns'])-int(r['intended_ns']) for r in selected)
   groups[group]={'offers':len(offers),'applied':len(selected),'refused':sum(r['status']=='window_refused' for r in offers),'p50_ms':lat[(len(lat)*50+99)//100-1]/1e6,'p95_ms':lat[(len(lat)*95+99)//100-1]/1e6,'p99_ms':lat[(len(lat)*99+99)//100-1]/1e6,'max_ms':max(lat)/1e6}
  lat=sorted(int(r['completed_ns'])-int(r['intended_ns']) for r in applied)
  quant={f'p{p}_ms':lat[(len(lat)*p+99)//100-1]/1e6 for p in [50,95,99]}
  quant['max_ms']=max(lat)/1e6
  assert math.isclose(quant['p99_ms'],float(s['p99_us'])/1000,abs_tol=.0000011)
  records.append({'host':host,'case':c['name'],'ready_items':c['ready_items'],'offers':len(rows),'applied':len(applied),'refused':statuses['window_refused'],'completed_ops_s':len(applied)*1e9/elapsed,'quantiles':quant,'groups':groups})
out={'scope':'Root recomputation directly from all96 original raw offer ledgers and summaries after both actual terminals. No workloads rerun. Successful intended latency nearest rank; refusals retained separately.','inputs':inputs,'records':records}
(base/'ROOT_DATA_REVIEW.json').write_text(json.dumps(out,indent=2)+'\n')
for host in ['linux','mac']:
 rs=[r for r in records if r['host']==host]
 print(host,len(rs),'offers',sum(r['offers']for r in rs),'applied',sum(r['applied']for r in rs),'refused',sum(r['refused']for r in rs))
