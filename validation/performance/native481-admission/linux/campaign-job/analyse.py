"""Independent recomputation of original481 Linux histories after both quiet windows ended."""
from pathlib import Path
import collections,csv,datetime,hashlib,json,statistics,sys

sys.dont_write_bytecode=True
job=Path(__file__).resolve().parent
sys.path.insert(0,str(job))
from common import check_readbacks, check_settings
load=lambda p:json.loads(Path(p).read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
quiet=load(job.parent/'ROOT_QUIET_END.json');assert quiet['linux']['actual_terminal_exit']==quiet['mac']['actual_terminal_exit']==0
assert quiet['linux']['original_handle']==1858
terminal=load(job/'TERMINAL.json');assert terminal['exit']==0 and terminal['cases_terminated']==48 and terminal['all_timing_ended']
assert(job/'controller.exit').read_text().strip()==(job/'CAMPAIGN_STATUS').read_text().strip()=='0'
assert(job/'CAMPAIGN_STAGE').read_text().strip()=='ALL_TIMING_END'
config=load(job/'CAMPAIGN_CONFIG.json');assert sha(job/'CAMPAIGN_CONFIG.json')==load(job/'READY.json')['campaign_config_sha256']
campaign=load(job/'campaign/CAMPAIGN.json');assert len(campaign['cases'])==48 and not campaign['post_timing_errors']
before=load(job/'before-host.json');after=load(job/'after-host.json');assert before['device_identity']==after['device_identity'] and before['cpu_policy']==after['cpu_policy']
inputs={};records=[];all_status=collections.Counter();all_maintenance=collections.Counter()
def remember(p):
 p=Path(p);inputs[str(p)]={'sha256':sha(p),'bytes':p.stat().st_size,'mode':oct(p.stat().st_mode&0o7777)}
def quantiles(values):
 values=sorted(values);assert values
 return {**{f'p{percent}_ms':values[(len(values)*percent+99)//100-1]/1e6 for percent in [50,95,99]},'max_ms':values[-1]/1e6,'samples':len(values)}
def histogram(text):return {int(k):int(v)for k,v in(x.split(':')for x in text.split('|')if x)}
def hist_delta(a,b):
 result={k:b.get(k,0)-v for k,v in a.items()};result.update({k:v for k,v in b.items()if k not in a});assert all(v>=0 for v in result.values());return result
fields=['append_calls','append_units','append_commands','append_ns','barrier_calls','barrier_tickets','barrier_ns','errors','encoded_bytes','io_append_calls','io_append_ns','sync_calls','sync_ns','publish_calls','publish_ns']
stages=['before_measurement','after_measurement','after_create_join','after_recover_join']
for case in campaign['cases']:
 name=case['name'];root=Path(case['wal_root']);meta=Path(case['metadata']);prefix=job/'campaign'/name
 assert case['exit']==case['validator_exit']==case['readbacks_exit']==0
 for suffix in ['.exit','-check.exit']:assert prefix.with_name(name+suffix).read_text().strip()=='0'
 assert load(prefix.with_name(name+'.native-command.json'))==case['command']
 assert load(prefix.with_name(name+'.command.json'))==case['resource_command']
 s=dict(x.split('=',1)for x in(root/'summary.txt').read_text().split());n=lambda k:int(s[k])
 assert (root/'summary.txt').read_bytes()==prefix.with_name(name+'.stdout').read_bytes()
 assert s['publication_effective']==s['publication_requested']==case['publication']
 assert s['publication_provider']==('FileLogIo'if case['publication']=='atomic'else'WitnessLogIo') and s['publication_timings']=='native'
 assert n('offers')==case['offers']and n('offered_rate')==case['rate']and n('window')==case['window']and n('warmup')==64 and n('groups')==case['groups']
 check_settings(case,root)
 readbacks=check_readbacks(prefix.with_name(name+'.stderr').read_text().splitlines(),case['publication'])
 assert readbacks==load(prefix.with_name(name+'.readbacks.json'))
 rows=list(csv.DictReader((root/'offers.csv').open()));statuses=collections.Counter(r['status']for r in rows)
 assert len(rows)==case['offers'];all_status.update(statuses)
 numeric=['operation','group','intended_ns','dispatched_ns','completed_ns','replica','sequence','applied_index','value']
 for i,r in enumerate(rows):
  for field in numeric:r[field]=int(r[field])
  assert r['operation']==65+i and r['intended_ns']==i*1_000_000_000//case['rate']
  expected_group=1+(int((r['operation']-1)%32==31) if case['groups']==2 else 0)
  assert r['group']==expected_group
  assert r['intended_ns']<=r['dispatched_ns']<=r['completed_ns']<=n('elapsed_ns')
 assert all(rows[i]['dispatched_ns']>=rows[i-1]['dispatched_ns']for i in range(1,len(rows)))
 applied=[r for r in rows if r['status']=='applied'];admitted=[r for r in rows if r['sequence']>0]
 for status in ['applied','window_refused','no_leader','admission_refused','not_proposed','unknown','pending']:assert statuses[status]==n(status),status
 assert set(statuses)<={'applied','window_refused','no_leader','admission_refused','not_proposed','unknown','pending'}
 assert len(applied)==n('applied')and len(admitted)==n('admitted')==len(applied)+n('not_proposed')
 assert n('unknown')==n('pending')==0 and n('recovered_value')==64+len(applied)and s['retry_verified']==s['workers_joined']=='true'
 assert len({(r['replica'],r['sequence'])for r in admitted})==len(admitted)
 for group in range(1,case['groups']+1):
  history=sorted([r for r in applied if r['group']==group],key=lambda r:r['applied_index']);assert all(r['value']==64//case['groups']+1+i for i,r in enumerate(history))
  assert all(history[i]['applied_index']>history[i-1]['applied_index']for i in range(1,len(history)))
 horizon=n('horizon_ns');elapsed=n('elapsed_ns');drain=n('drain_ns');assert horizon==case['offers']*1_000_000_000//case['rate']and elapsed-horizon==drain
 during=sum(r['completed_ns']<horizon for r in applied);assert during==n('applied_during')and len(applied)-during==n('applied_drain')
 assert sum(r['dispatched_ns']<horizon<=r['completed_ns']for r in admitted)==n('backlog_at_horizon')
 assert sum(r['dispatched_ns']<horizon for r in admitted)==n('admitted_during')
 assert sum(r['dispatched_ns']>=horizon for r in rows)==n('late_decisions')
 events=sorted([(r['dispatched_ns'],0,i,r)for i,r in enumerate(rows)]+[(r['completed_ns'],-1,i,r)for i,r in enumerate(rows)if r['sequence']>0],key=lambda e:e[:3]);live=peak=0;group_live=collections.Counter();group_peak=collections.Counter();ceiling=(case['window']+case['groups']-1)//case['groups']
 for _,kind,_,r in events:
  group=r['group']
  if kind==0 and r['status']=='window_refused':assert live>=case['window'] or group_live[group]>=ceiling;continue
  if kind==0 and not r['sequence']:continue
  delta=-1 if kind==-1 else 1;live+=delta;group_live[group]+=delta
  assert 0<=live<=case['window'] and 0<=group_live[group]<=ceiling
  peak=max(peak,live);group_peak[group]=max(group_peak[group],group_live[group])
 assert live==0 and not any(group_live.values())and peak==n('max_inflight')
 latency={'intended':quantiles([r['completed_ns']-r['intended_ns']for r in applied]),'service':quantiles([r['completed_ns']-r['dispatched_ns']for r in applied]),'dispatch':quantiles([r['dispatched_ns']-r['intended_ns']for r in rows])}
 for key,summary in [('intended','p99_us'),('service','service_p99_us'),('dispatch','dispatch_p99_us')]:assert abs(latency[key]['p99_ms']-float(s[summary])/1000)<=.00000051
 rates={'completed_originals_per_second':len(applied)*1e9/elapsed,'during_originals_per_second':during*1e9/horizon,'drain_originals_per_second':(len(applied)-during)*1e9/drain if drain else None,'offered_originals_per_second':len(rows)*1e9/horizon,'admitted_during_originals_per_second':n('admitted_during')*1e9/horizon}
 for key,summary in [('completed_originals_per_second','applied_total_ops_s'),('during_originals_per_second','applied_during_ops_s'),('offered_originals_per_second','offered_ops_s')]:assert abs(rates[key]-float(s[summary]))<=.00051
 storage=list(csv.DictReader((root/'storage.csv').open()));snapshots={stage:{int(r['replica']):r for r in storage if r['stage']==stage}for stage in stages};assert all(len(v)==3 for v in snapshots.values())
 per_replica={}
 for replica in range(1,4):
  d={k:int(snapshots['after_measurement'][replica][k])-int(snapshots['before_measurement'][replica][k])for k in fields}
  join={k:int(snapshots['after_create_join'][replica][k])-int(snapshots['before_measurement'][replica][k])for k in fields}
  assert all(v>=0 for v in d.values())and all(join[k]>=d[k]for k in fields)and d['errors']==join['errors']==0
  for field in ['batch_histogram','group_units']:d[field]=hist_delta(histogram(snapshots['before_measurement'][replica][field]),histogram(snapshots['after_measurement'][replica][field]))
  assert sum(d['batch_histogram'].values())==d['append_calls']and sum(k*v for k,v in d['batch_histogram'].items())==d['append_units']
  per_replica[replica]={'measurement_delta':d,'after_create_join_delta':join,'after_recover_join_absolute':snapshots['after_recover_join'][replica]}
 publication=list(csv.DictReader((root/'publication.csv').open()));assert len(publication)==60;primitive={}
 for replica in range(1,4):
  primitive[replica]={}
  for step in ['open','write','file_sync','rename','directory_sync']:
   rs={r['stage']:r for r in publication if int(r['replica'])==replica and r['step']==step};assert set(rs)==set(stages)
   d={k:int(rs['after_measurement'][k])-int(rs['before_measurement'][k])for k in ['calls','errors','elapsed_ns','manifest_calls','manifest_errors','manifest_ns']};assert all(v>=0 for v in d.values())and d['errors']==d['manifest_errors']==0
   if case['publication']=='witness'and step in ['open','rename','directory_sync']:assert d['calls']==d['elapsed_ns']==0
   primitive[replica][step]={'measurement_delta':d,'after_create_join':rs['after_create_join'],'after_recover_join':rs['after_recover_join']}
 leader=int(s['leader_placement'].split(',')[0].split(':')[1]);d=per_replica[leader]['measurement_delta']
 ratio=lambda a,b:a/b if b else None
 leader_metrics={'multiunit_append_calls':sum(v for k,v in d['batch_histogram'].items()if k>1),'max_observed_append_units':max(d['batch_histogram']),'commands_per_append_unit':ratio(d['append_commands'],d['append_units']),'units_per_append_call':ratio(d['append_units'],d['append_calls']),'barriers_per_original':ratio(d['barrier_calls'],len(applied)),'mean_barrier_ms':ratio(d['barrier_ns'],d['barrier_calls']*1e6),'mean_wal_sync_ms':ratio(d['sync_ns'],d['sync_calls']*1e6),'mean_publication_ms':ratio(d['publish_ns'],d['publish_calls']*1e6),'barrier_wall_fraction':d['barrier_ns']/elapsed,'wal_sync_wall_fraction':d['sync_ns']/elapsed,'publication_wall_fraction':d['publish_ns']/elapsed,'wal_sync_calls':d['sync_calls'],'publication_calls':d['publish_calls'],'barrier_calls':d['barrier_calls']}
 maintenance=list(csv.DictReader((root/'maintenance.csv').open()));mc=collections.Counter((r['kind'],r['status'])for r in maintenance);all_maintenance.update(mc)
 assert mc[('checkpoint','completed')]==n('checkpoints')and mc[('reclaim','completed')]==n('reclaims')and mc[('skip','skipped')]==n('maintenance_skips')
 cold={kind:{'count':mc[(kind,'completed')],'queue_plus_operation_ms_sum':sum(int(r['completed_ns'])-int(r['intended_ns'])for r in maintenance if(r['kind'],r['status'])==(kind,'completed'))/1e6,'submitted_to_complete_ms_sum':sum(int(r['completed_ns'])-int(r['submitted_ns'])for r in maintenance if(r['kind'],r['status'])==(kind,'completed'))/1e6}for kind in ['checkpoint','reclaim']}
 reclaimed=sum(int(r['before_bytes'])-int(r['after_bytes'])for r in maintenance if(r['kind'],r['status'])==('reclaim','completed'));assert reclaimed==n('reclaimed_bytes')
 resource=load(meta/'RESOURCES.json');assert resource['exit']==resource['helper_exit']==0 and resource['helper_error']is None and resource['max_rss_unit']=='KiB'and resource['max_rss_bytes']==resource['max_rss_raw']*1024
 resource['user_us_per_completed_original']=resource['user_seconds']*1e6/len(applied)
 resource['system_us_per_completed_original']=resource['system_seconds']*1e6/len(applied)
 resource['cpu_us_per_completed_original']=(resource['user_seconds']+resource['system_seconds'])*1e6/len(applied)
 host_poll={'rounds':n('poll_rounds'),'elapsed_ms':float(s['host_poll_ms']),'max_ms':float(s['max_host_poll_ms']),'ms_per_completed_original':float(s['host_poll_ms'])/len(applied),'rounds_per_completed_original':n('poll_rounds')/len(applied),'wall_fraction':float(s['host_poll_ms'])*1e6/elapsed}
 per_group={}
 for group in range(1,case['groups']+1):
  gr=[r for r in rows if r['group']==group];ga=[r for r in gr if r['status']=='applied'];gd=sum(r['completed_ns']<horizon for r in ga);gs=collections.Counter(r['status']for r in gr)
  per_group[group]={'offers':len(gr),'applied':len(ga),'statuses':{k:gs[k]for k in ['applied','window_refused','no_leader','admission_refused','not_proposed','unknown','pending']},'completed_originals_per_second':len(ga)*1e9/elapsed,'during_originals_per_second':gd*1e9/horizon,'drain_originals_per_second':(len(ga)-gd)*1e9/drain if drain else None,'offered_originals_per_second':len(gr)*1e9/horizon,'backlog_at_horizon':sum(r['dispatched_ns']<horizon<=r['completed_ns']for r in gr if r['sequence']>0),'max_inflight':group_peak[group],'pending_ceiling':ceiling,'latencies':{'intended':quantiles([r['completed_ns']-r['intended_ns']for r in ga]),'service':quantiles([r['completed_ns']-r['dispatched_ns']for r in ga]),'dispatch':quantiles([r['dispatched_ns']-r['intended_ns']for r in gr])}}
  if case['groups']==2:assert len(gr)==(7936 if group==1 else 256)
 rec={'host_poll':host_poll,'arm':case['arm'],'ready_items':case['ready_items'],'per_group':per_group,'case':name,'pair':case['pair'],'trial':case['trial'],'cell':case['cell'],'protocol':case['protocol'],'publication':case['publication'],'actual_process_exit':case['exit'],'validator_exit':case['validator_exit'],'readbacks_exit':case['readbacks_exit'],'summary':s,'rates':rates,'latencies':latency,'offers':len(rows),'applied':len(applied),'refusals':{k:statuses[k]for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending']},'elapsed_ms':elapsed/1e6,'horizon_ms':horizon/1e6,'drain_ms':drain/1e6,'max_inflight':peak,'backlog_at_horizon':n('backlog_at_horizon'),'cold_maintenance':cold,'maintenance_skips':n('maintenance_skips'),'reclaimed_logical_bytes':reclaimed,'leader_configured':leader,'leader_metrics':leader_metrics,'replica_ordinary_storage':per_replica,'replica_publication_steps':primitive,'wholeprocess_resources':resource}
 records.append(rec)
 for p in [*(p for p in root.iterdir()if p.is_file()),meta/'RESOURCES.json',prefix.with_name(name+'.stdout'),prefix.with_name(name+'.stderr'),prefix.with_name(name+'.exit'),prefix.with_name(name+'-check.stdout'),prefix.with_name(name+'-check.stderr'),prefix.with_name(name+'-check.exit'),prefix.with_name(name+'.readbacks.json')]:remember(p)
stats=lambda values:{'median':statistics.median(values),'min':min(values),'max':max(values),'trials':values}
cells=[];pairs=[]
for cell in ['singleton','moderate','loaded','skew']:
 for protocol in ['tcp','quic']:
  for arm in ['baseline','candidate']:
   rs=sorted([r for r in records if(r['cell'],r['protocol'],r['arm'])==(cell,protocol,arm)],key=lambda r:r['trial']);assert len(rs)==3
   cells.append({'cell':cell,'protocol':protocol,'arm':arm,'ready_items':rs[0]['ready_items'],'rates':{k:stats([r['rates'][k]for r in rs])for k in rs[0]['rates']},'latencies':{kind:{metric:stats([r['latencies'][kind][metric]for r in rs])for metric in ['p50_ms','p95_ms','p99_ms','max_ms']}for kind in ['intended','service','dispatch']},'counts':{k:stats([r[k]for r in rs])for k in ['applied','max_inflight','backlog_at_horizon','maintenance_skips','reclaimed_logical_bytes']},'refusals':{k:stats([r['refusals'][k]for r in rs])for k in rs[0]['refusals']},'elapsed_ms':stats([r['elapsed_ms']for r in rs]),'drain_ms':stats([r['drain_ms']for r in rs]),'leader_metrics':{k:stats([r['leader_metrics'][k]for r in rs])for k in rs[0]['leader_metrics']},'host_poll':{k:stats([r['host_poll'][k]for r in rs])for k in rs[0]['host_poll']},'wholeprocess_resources':{k:stats([r['wholeprocess_resources'][k]for r in rs])for k in ['elapsed_seconds','user_seconds','system_seconds','max_rss_bytes','voluntary_context_switches','involuntary_context_switches','block_inputs','block_outputs','user_us_per_completed_original','system_us_per_completed_original','cpu_us_per_completed_original']},'wholeprocess_core_equivalent':stats([(r['wholeprocess_resources']['user_seconds']+r['wholeprocess_resources']['system_seconds'])/r['wholeprocess_resources']['elapsed_seconds']for r in rs]),'wholeprocess_peak_rss_bytes':stats([r['wholeprocess_resources']['max_rss_bytes']for r in rs])})
  for trial in [1,2,3]:
   a=next(r for r in records if(r['cell'],r['protocol'],r['trial'],r['arm'])==(cell,protocol,trial,'baseline'));w=next(r for r in records if(r['cell'],r['protocol'],r['trial'],r['arm'])==(cell,protocol,trial,'candidate'))
   pairs.append({'cell':cell,'protocol':protocol,'trial':trial,'baseline_case':a['case'],'candidate_case':w['case'],'completed_rate_ratio':w['rates']['completed_originals_per_second']/a['rates']['completed_originals_per_second'],'during_rate_ratio':w['rates']['during_originals_per_second']/a['rates']['during_originals_per_second'],'latency_ratios':{kind:{k:w['latencies'][kind][k]/a['latencies'][kind][k]for k in ['p50_ms','p95_ms','p99_ms','max_ms']}for kind in ['intended','service','dispatch']},'barriers_per_original_ratio':w['leader_metrics']['barriers_per_original']/a['leader_metrics']['barriers_per_original'],'commands_per_append_unit_ratio':w['leader_metrics']['commands_per_append_unit']/a['leader_metrics']['commands_per_append_unit'],'per_group':{g:{'completed_rate_ratio':w['per_group'][g]['completed_originals_per_second']/a['per_group'][g]['completed_originals_per_second'],'latency_ratios':{kind:{metric:w['per_group'][g]['latencies'][kind][metric]/a['per_group'][g]['latencies'][kind][metric]for metric in ['p50_ms','p95_ms','p99_ms','max_ms']}for kind in ['intended','service','dispatch']},'during_rate_ratio':w['per_group'][g]['during_originals_per_second']/a['per_group'][g]['during_originals_per_second']if a['per_group'][g]['during_originals_per_second']else None,'during_baseline':a['per_group'][g]['during_originals_per_second'],'during_candidate':w['per_group'][g]['during_originals_per_second'],'p99_baseline_ms':a['per_group'][g]['latencies']['intended']['p99_ms'],'p99_candidate_ms':w['per_group'][g]['latencies']['intended']['p99_ms'],'statuses_baseline':a['per_group'][g]['statuses'],'statuses_candidate':w['per_group'][g]['statuses'],'backlog_baseline':a['per_group'][g]['backlog_at_horizon'],'backlog_candidate':w['per_group'][g]['backlog_at_horizon']}for g in a['per_group']},'host_poll_ratios':{k:w['host_poll'][k]/a['host_poll'][k]if a['host_poll'][k]else None for k in a['host_poll']},'wholeprocess_resource_ratios':{k:w['wholeprocess_resources'][k]/a['wholeprocess_resources'][k]if a['wholeprocess_resources'][k]else None for k in ['user_seconds','system_seconds','user_us_per_completed_original','system_us_per_completed_original','cpu_us_per_completed_original','max_rss_bytes']},'replica_counter_ratios':{replica:{k:w['replica_ordinary_storage'][replica]['measurement_delta'][k]/a['replica_ordinary_storage'][replica]['measurement_delta'][k]if a['replica_ordinary_storage'][replica]['measurement_delta'][k]else None for k in fields}for replica in a['replica_ordinary_storage']},'primitive_ratios':{replica:{step:{k:w['replica_publication_steps'][replica][step]['measurement_delta'][k]/a['replica_publication_steps'][replica][step]['measurement_delta'][k]if a['replica_publication_steps'][replica][step]['measurement_delta'][k]else None for k in ['calls','elapsed_ns','manifest_calls','manifest_ns']}for step in a['replica_publication_steps'][replica]}for replica in a['replica_publication_steps']},'mean_barrier_ratio':w['leader_metrics']['mean_barrier_ms']/a['leader_metrics']['mean_barrier_ms'],'mean_wal_sync_ratio':w['leader_metrics']['mean_wal_sync_ms']/a['leader_metrics']['mean_wal_sync_ms'],'mean_publication_ratio':w['leader_metrics']['mean_publication_ms']/a['leader_metrics']['mean_publication_ms'],'during_rate_regression':w['rates']['during_originals_per_second']<a['rates']['during_originals_per_second'],'rate_regression':w['rates']['completed_originals_per_second']<a['rates']['completed_originals_per_second'],'p99_regression':w['latencies']['intended']['p99_ms']>a['latencies']['intended']['p99_ms'],'window_refusals_baseline':a['refusals']['window_refused'],'window_refusals_candidate':w['refusals']['window_refused']})
group_cells=[]
for cell in ['singleton','moderate','loaded','skew']:
 for protocol in ['tcp','quic']:
  for arm in ['baseline','candidate']:
   rs=sorted([r for r in records if(r['cell'],r['protocol'],r['arm'])==(cell,protocol,arm)],key=lambda r:r['trial'])
   for group in rs[0]['per_group']:
    gs=[r['per_group'][group]for r in rs]
    group_cells.append({'cell':cell,'protocol':protocol,'arm':arm,'ready_items':rs[0]['ready_items'],'group':group,'metrics':{k:stats([g[k]for g in gs])for k in ['offers','applied','completed_originals_per_second','during_originals_per_second','drain_originals_per_second','offered_originals_per_second','backlog_at_horizon','max_inflight','pending_ceiling']},'statuses':{k:stats([g['statuses'][k]for g in gs])for k in gs[0]['statuses']},'latencies':{kind:{metric:stats([g['latencies'][kind][metric]for g in gs])for metric in ['p50_ms','p95_ms','p99_ms','max_ms']}for kind in ['intended','service','dispatch']}})
readout={'collection_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'original_handle':1858,'actual_terminal_exit':0,'actual_processes':48,'actual_validators':48,'actual_readback_checks':48,'source_commits':config['source_commits'],'config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),'host_policy_device_unchanged':True,'totals':dict(all_status),'maintenance_totals':{str(k):v for k,v in all_maintenance.items()},'records':records,'group_cells':group_cells,'cells':cells,'pairs':pairs,'regressions':[p for p in pairs if p['rate_regression']or p['p99_regression']or p['during_rate_regression']],'scope':'Finite one/twogroup threevoters sameBtrfsdevice paired published476/480 Witness admission diagnostic. Skew cold256 offers, pergroup2048 fixture ceiling; aggregate is not onegroup floor. Quantiles nearest-rank from exact raw ns; intended/service successful originals only, dispatch all scheduled offers. Totalcompletion boundary is existing native measure-loop end before verification/join/recovery; resources wholechild including those phases. Ordinary counters are nested elapsed wall, not additiveCPU; cold operation sums overlap and are not a critical path. Commands/appendunit includes physical metadata transitions/retries and is not literal proposalwidth. Encodedbytes are not deviceI/O. Offered/window-limited cells do not establish maximum capacity, sustainedmemory/debt plateau or healthyheadroom. No defaultpromotion/threefilesystem/fullP9 acceptance.'}
save(job/'READOUT.json',readout);save(job/'SUMMARY.json',{'cells':cells,'group_cells':group_cells,'pairs':pairs,'regressions':readout['regressions'],'totals':dict(all_status)})
save(job/'READOUT_PROVENANCE.json',{'method':'All48 original ledgers independently checked for schedule/conservation/unique tickets/inflight/refusal validity/applied values/recovery and nearest-rank latency. All48 summaries and all replica storage/primitive deltas/maintenance/resources checked. No workload or external validator rerun.','inputs':inputs,'analyse_py_sha256':sha(__file__)})
print(json.dumps({'totals':dict(all_status),'maintenance':readout['maintenance_totals'],'regressions':readout['regressions'],'raw_cases':len(records)},indent=2))
