"""Independent, post-terminal collection of original475 Mac APFS results; never executes workloads."""
from pathlib import Path
import collections,csv,datetime,hashlib,json,math,statistics

job=Path(__file__).resolve().parent
load=lambda p:json.loads(Path(p).read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
raw=job/'collection/raw'
sweep=raw/'sweep'
campaign=load(sweep/'campaign/CAMPAIGN.json');config=load(sweep/'CAMPAIGN_CONFIG.json');collected=load(raw/'COLLECTED.json')
assert (raw/'STATUS').read_text().strip()=='0' and (raw/'STAGE').read_text().strip()=='DONE'
assert (sweep/'CAMPAIGN_STATUS').read_text().strip()=='0' and (sweep/'CAMPAIGN_STAGE').read_text().strip()=='ALL_TIMING_END'
assert len(campaign['cases'])==len(config['cases'])==54
assert sha(sweep/'CAMPAIGN_CONFIG.json')==load(sweep/'READY.json')['campaign_config_sha256']
assert sha(sweep/'campaign.py')==load(sweep/'READY.json')['campaign_driver_sha256']
assert all(sha(raw/p)==v['sha256'] and (raw/p).stat().st_size==v['bytes'] for p,v in collected['files'].items())
records=[];maintenance_status=collections.Counter();totals=collections.Counter()
for c in campaign['cases']:
 name=c['name'];root=sweep/'campaign-wal'/name;s=dict(x.split('=',1) for x in (root/'summary.txt').read_text().split());n=lambda k:int(s[k])
 assert c['exit']==c['validator_exit']==0 and (sweep/'campaign'/f'{name}.exit').read_text().strip()==(sweep/'campaign'/f'{name}-check.exit').read_text().strip()=='0'
 assert (sweep/'campaign'/f'{name}.stdout').read_bytes()==(root/'summary.txt').read_bytes()
 rows=list(csv.DictReader((root/'offers.csv').open()));counts=collections.Counter(r['status']for r in rows);applied=[r for r in rows if r['status']=='applied']
 assert len(rows)==n('offers')==8192 and len(applied)==n('applied')==n('admitted') and set(counts)<= {'applied','window_refused'}
 assert counts['window_refused']==n('window_refused') and len(rows)==len(applied)+n('window_refused')
 assert all(n(k)==0 for k in ['unknown','pending','no_leader','admission_refused','not_proposed'])
 assert n('recovered_value')==64+n('applied') and s['retry_verified']==s['workers_joined']=='true'
 horizon=n('horizon_ns');elapsed=n('elapsed_ns')
 assert sum(int(r['completed_ns'])<horizon for r in applied)==n('applied_during')
 assert n('applied_drain')==len(applied)-n('applied_during') and elapsed-horizon==n('drain_ns')
 assert sum(int(r['dispatched_ns'])<horizon<=int(r['completed_ns']) for r in applied)==n('backlog_at_horizon')
 def p99(values):
  values.sort();return values[(len(values)*99+99)//100-1]/1e6
 latencies={'p99_ms':p99([int(r['completed_ns'])-int(r['intended_ns'])for r in applied]),'service_p99_ms':p99([int(r['completed_ns'])-int(r['dispatched_ns'])for r in applied]),'dispatch_p99_ms':p99([int(r['dispatched_ns'])-int(r['intended_ns'])for r in rows])}
 for key,field in [('p99_ms','p99_us'),('service_p99_ms','service_p99_us'),('dispatch_p99_ms','dispatch_p99_us')]:assert abs(latencies[key]-float(s[field])/1000)<.0000011,(name,key,latencies[key],s[field])
 storage=list(csv.DictReader((root/'storage.csv').open()));stages={stage:{int(r['replica']):r for r in storage if r['stage']==stage}for stage in ['before_measurement','after_measurement','after_create_join','after_recover_join']};assert all(len(v)==3 for v in stages.values())
 fields=['append_calls','append_units','append_commands','append_ns','barrier_calls','barrier_tickets','barrier_ns','errors','encoded_bytes','io_append_calls','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']
 deltas={replica:{k:int(stages['after_measurement'][replica][k])-int(stages['before_measurement'][replica][k])for k in fields}for replica in range(1,4)}
 assert all(v>=0 for d in deltas.values()for v in d.values()) and all(d['errors']==0 for d in deltas.values())
 leader=int(s['leader_placement'].split(':')[1]);d=deltas[leader]
 leader_metrics={'commands_per_append_unit':d['append_commands']/d['append_units'],'barriers_per_original':d['barrier_calls']/n('applied'),'mean_barrier_ms':d['barrier_ns']/d['barrier_calls']/1e6,'mean_wal_sync_ms':d['sync_ns']/d['sync_calls']/1e6,'mean_manifest_publication_ms':d['publish_ns']/d['publish_calls']/1e6,'barrier_wall_fraction':d['barrier_ns']/elapsed,'wal_sync_wall_fraction':d['sync_ns']/elapsed,'manifest_publication_wall_fraction':d['publish_ns']/elapsed}
 maintenance=list(csv.DictReader((root/'maintenance.csv').open()));mc=collections.Counter((r['kind'],r['status'])for r in maintenance);maintenance_status.update(mc)
 assert mc[('checkpoint','completed')]==n('checkpoints') and mc[('reclaim','completed')]==n('reclaims')
 resource=load(sweep/'campaign-metadata'/name/'RESOURCES.json');assert resource['exit']==0
 readbacks=[r for r in (sweep/'campaign'/f'{name}.stderr').read_text().splitlines()if 'capacity' in r];assert len(readbacks)==6
 rec={'case':name,'protocol':c['protocol'],'offered_rate':c['offered_rate'],'window':c['window'],'trial':c['trial'],'completed_originals_per_second':n('applied')*1e9/elapsed,'during_originals_per_second':n('applied_during')*1e9/horizon,**latencies,'applied':n('applied'),'window_refused':n('window_refused'),'max_inflight':n('max_inflight'),'backlog_at_horizon':n('backlog_at_horizon'),'drain_ms':n('drain_ns')/1e6,'checkpoints':n('checkpoints'),'reclaims':n('reclaims'),'maintenance_skips':n('maintenance_skips'),'reclaimed_bytes':n('reclaimed_bytes'),'leader':leader,'leader_metrics':leader_metrics,'replica_deltas':deltas,'wholeprocess_resources':resource}
 records.append(rec);totals.update({'offers':len(rows),'applied':len(applied),'window_refused':n('window_refused')})
cells=[]
for protocol in ['tcp','quic']:
 for rate in [32,128,512]:
  for window in [32,256,4096]:
   rs=sorted([r for r in records if (r['protocol'],r['offered_rate'],r['window'])==(protocol,rate,window)],key=lambda r:r['trial']);assert len(rs)==3
   stats=lambda xs:{'median':statistics.median(xs),'min':min(xs),'max':max(xs),'trials':xs}
   keys=['completed_originals_per_second','during_originals_per_second','p99_ms','service_p99_ms','dispatch_p99_ms','applied','window_refused','max_inflight','backlog_at_horizon','drain_ms','checkpoints','reclaims','maintenance_skips','reclaimed_bytes']
   cells.append({'protocol':protocol,'offered_rate':rate,'window':window,'metrics':{k:stats([r[k]for r in rs])for k in keys},'leader_metrics':{k:stats([r['leader_metrics'][k]for r in rs])for k in rs[0]['leader_metrics']},'wholeprocess_rss_bytes':stats([r['wholeprocess_resources']['max_rss_bytes']for r in rs])})
output={'original_handle':29711,'terminal_actual_exit':0,'workloads':54,'validators':54,'totals':dict(totals),'source_commit':config['candidate']['source_commit'],'artifact_sha256':config['candidate']['artifact']['sha256'],'host_before_sha256':sha(sweep/'before-host.json'),'host_after_sha256':sha(sweep/'after-host.json'),'campaign_last_process_end_unix':campaign['cases'][-1]['end_unix'],'collection_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'maintenance_status_counts':{str(k):v for k,v in maintenance_status.items()},'cells':cells,'records':records,'scope':'Independent recomputation from all54 original ledgers/storage/resources after original29711 remote terminal. No benchmark rerun or changed raw. P99 is successful originals only; mean physical width is commands/appendunit, not proposal maximum. Storage is nested elapsed wall, not additive CPU. RSS covers whole process including warmup/recovery and is not a continuous bounded-memory/debt proof. Three voters share this APFS internal SSD. Finite8192-offer capacity diagnostic; not20M acceptance, universal filesystem gain or refusal-free headroom.'}
(job/'collection/INDEPENDENT_ANALYSIS.json').write_text(json.dumps(output,indent=2)+'\n')
print(json.dumps({'totals':dict(totals),'cells':len(cells),'maintenance':output['maintenance_status_counts'],'independent_all54_raw_recomputation':'PASS'}))
