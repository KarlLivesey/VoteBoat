"""Independent post-terminal477 raw recomputation. Never launches workloads."""
import collections, csv, hashlib, json, math, statistics
from pathlib import Path

base=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
load=lambda p:json.loads(p.read_text())
inputs={};records=[]
def retained(p):
    inputs[str(p)]={'sha256':sha(p),'bytes':p.stat().st_size}
    return p
def percentile(values,p):
    values=sorted(values)
    return values[(len(values)*p+99)//100-1]/1e6
def histogram(s):return {int(k):int(v)for k,v in (x.split(':')for x in s.split('|')if x)}
for host,root in [('linux',base/'linux3'),('mac',base/'mac-collection/raw')]:
    terminal=load(retained(root/'TERMINAL.json')); assert terminal['exit']==0 and terminal['all_timing_ended']
    campaign=load(retained(root/'campaign/CAMPAIGN.json'));assert len(campaign['cases'])==48
    for c in campaign['cases']:
        assert c['exit']==c['validator_exit']==c['readbacks_exit']==0
        wal=Path(c['wal_root'])if host=='linux'else root/'results'/c['name']
        s=dict(t.split('=',1)for t in retained(wal/'summary.txt').read_text().split())
        rows=list(csv.DictReader(retained(wal/'offers.csv').open()));status=collections.Counter(r['status']for r in rows)
        applied=[r for r in rows if r['status']=='applied'];assert len(rows)==c['offers']==int(s['offers'])
        assert len(applied)==int(s['applied'])==int(s['admitted'])
        assert set(status)<={'applied','window_refused'} and sum(status.values())==len(rows)
        assert all(s[k]=='0'for k in ['no_leader','admission_refused','not_proposed','unknown','pending'])
        assert int(s['recovered_value'])==64+len(applied) and s['retry_verified']==s['workers_joined']=='true'
        horizon=int(s['horizon_ns']);elapsed=int(s['elapsed_ns']);assert horizon==int(c['horizon_seconds']*1e9)
        assert sum(int(r['completed_ns'])<horizon for r in applied)==int(s['applied_during'])
        assert sum(int(r['dispatched_ns'])<horizon<=int(r['completed_ns'])for r in applied)==int(s['backlog_at_horizon'])
        latencies=[int(r['completed_ns'])-int(r['intended_ns'])for r in applied]
        p99=percentile(latencies,99);assert abs(p99-float(s['p99_us'])/1000)<.0000011
        storage=list(csv.DictReader(retained(wal/'storage.csv').open()));stages={stage:{int(r['replica']):r for r in storage if r['stage']==stage}for stage in ['before_measurement','after_measurement','after_create_join']}
        leader=int(s['leader_placement'].split(':')[1]);before=stages['before_measurement'][leader];after=stages['after_measurement'][leader]
        fields=['append_calls','append_units','append_commands','append_ns','barrier_calls','barrier_tickets','barrier_ns','errors','encoded_bytes','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']
        delta={k:int(after[k])-int(before[k])for k in fields};assert all(v>=0 for v in delta.values())and delta['errors']==0
        h0=histogram(before['batch_histogram']);h1=histogram(after['batch_histogram']);hist={k:h1.get(k,0)-h0.get(k,0)for k in h1}
        assert sum(hist.values())==delta['append_calls'] and sum(k*v for k,v in hist.items())==delta['append_units']
        resource=root/'campaign-metadata'/c['name']/'RESOURCES.json';resource=load(retained(resource));assert resource['exit']==resource['helper_exit']==0
        record={'host':host,'case':c['name'],'trial':c['trial'],'cell':c['cell'],'protocol':c['protocol'],'publication':c['publication'],
                'completed_ops_s':len(applied)*1e9/elapsed,'during_ops_s':int(s['applied_during'])*1e9/horizon,
                'service_p99_ms':percentile([int(r['completed_ns'])-int(r['dispatched_ns'])for r in applied],99),'dispatch_p99_ms':percentile([int(r['dispatched_ns'])-int(r['intended_ns'])for r in rows],99),'late_decisions':int(s['late_decisions']),'p50_ms':percentile(latencies,50),'p95_ms':percentile(latencies,95),'p99_ms':p99,'max_ms':max(latencies)/1e6,
                'applied':len(applied),'refused':status['window_refused'],'max_pending':int(s['max_inflight']),
                'backlog':int(s['backlog_at_horizon']),'drain_ms':int(s['drain_ns'])/1e6,'host_poll_ms':float(s['host_poll_ms']),
                'max_host_poll_ms':float(s['max_host_poll_ms']),'host_poll_wall_fraction':float(s['host_poll_ms'])/(elapsed/1e6),
                'leader':leader,'leader_delta':delta,'append_unit_histogram_delta':hist,
                'leader_commands_per_append_unit':delta['append_commands']/delta['append_units'],
                'leader_barriers_per_original':delta['barrier_calls']/len(applied),
                'leader_mean_barrier_ms':delta['barrier_ns']/delta['barrier_calls']/1e6,
                'leader_mean_sync_ms':delta['sync_ns']/delta['sync_calls']/1e6,
                'leader_mean_publication_ms':delta['publish_ns']/delta['publish_calls']/1e6,
                'leader_barrier_wall_fraction':delta['barrier_ns']/elapsed,
                'wholeprocess_cpu_seconds':resource['user_seconds']+resource['system_seconds'],
                'wholeprocess_elapsed_seconds':resource['elapsed_seconds'],
                'wholeprocess_cpu_core_equivalent':(resource['user_seconds']+resource['system_seconds'])/resource['elapsed_seconds'],
                'max_rss_bytes':resource['max_rss_bytes'],'checkpoints':int(s['checkpoints']),'reclaims':int(s['reclaims']),
                'maintenance_skips':int(s['maintenance_skips']),'reclaimed_bytes':int(s['reclaimed_bytes'])}
        records.append(record)
cells=[]
for host in ['linux','mac']:
 for protocol in ['tcp','quic']:
  for cell in ['singleton','moderate','bulk','overload']:
    selected=[r for r in records if(r['host'],r['protocol'],r['cell'])==(host,protocol,cell)]
    paired=[]
    for trial in [1,2,3]:
        a=next(r for r in selected if r['trial']==trial and r['publication']=='atomic');w=next(r for r in selected if r['trial']==trial and r['publication']=='witness')
        paired.append({'trial':trial,'completed_ratio':w['completed_ops_s']/a['completed_ops_s'],'during_ratio':w['during_ops_s']/a['during_ops_s'],'p99_ratio':w['p99_ms']/a['p99_ms'],'atomic':a['case'],'witness':w['case']})
    keys=['completed_ops_s','during_ops_s','p50_ms','p95_ms','p99_ms','max_ms','applied','refused','max_pending','backlog','drain_ms','max_host_poll_ms','host_poll_wall_fraction','leader_commands_per_append_unit','leader_barriers_per_original','leader_mean_barrier_ms','leader_mean_sync_ms','leader_mean_publication_ms','leader_barrier_wall_fraction','wholeprocess_cpu_core_equivalent','max_rss_bytes','checkpoints','reclaims']
    cells.append({'host':host,'protocol':protocol,'cell':cell,'arms':{arm:{k:{'median':statistics.median(r[k]for r in selected if r['publication']==arm),'trials':[r[k]for r in sorted(selected,key=lambda x:x['trial'])if r['publication']==arm]}for k in keys}for arm in ['atomic','witness']},'pairs':paired})
save={'scope':'Independent post-terminal raw477 recomputation. Histogram is units per append call, not commands per proposal. Exact command-width/zero-command transition histograms are not recorded. Inclusive/overlapping elapsed scopes are not CPU. Resource CPU/RSS is wholeprocess including warmup/recovery/cleanup, with a different boundary from native elapsed. No production/default/threefilesystem gain claim.','inputs':inputs,'records':records,'cells':cells}
(base/'INDEPENDENT_READOUT.json').write_text(json.dumps(save,indent=2)+'\n')
for c in cells:
 print(c['host'],c['protocol'],c['cell'],'; '.join(arm+' '+str({k:round(c['arms'][arm][k]['median'],3)for k in ['completed_ops_s','p99_ms','refused','leader_commands_per_append_unit','leader_mean_barrier_ms','host_poll_wall_fraction']})for arm in ['atomic','witness']))
