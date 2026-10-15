import hashlib,json,stat,math
from pathlib import Path
b=Path('target/p9/native487-capacity-bound');out=b/'review/independent-actual';sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();load=lambda p:json.loads(Path(p).read_text());inputs={}
def bound(p):
 p=Path(p);inputs[str(p)]={'sha256':sha(p),'bytes':p.stat().st_size};return load(p)
go=bound(b/'ROOT_QUIET_GO.json');end=bound(b/'ROOT_QUIET_END.json');host_results={}
for host,job in [('linux',b/'comparison/linux/campaign'),('mac',b/'comparison/mac/collected')]:
 r=bound(out/(host+'.json'));ref=bound(b/('review/actual487-'+host+'/READOUT.json'));config=bound(job/'CAMPAIGN_CONFIG.json');terminal=bound(job/'TERMINAL.json');before=bound(job/'before-host.json');after=bound(job/'after-host.json')
 assert terminal['exit']==0 and terminal['all_timing_ended']and terminal['cases_terminated']==48 and not terminal['received_signal']and terminal['no_retries_or_replacements']
 assert before['device_identity']==after['device_identity']==config['device_identity']and before['cpu_policy']==after['cpu_policy']==config['cpu_policy']
 losses=[];group_losses=[]
 for pair in r['pairs']:
  a=pair['baseline'];z=pair['candidate'];fr=next(p for p in ref['pairs']if p['pair']==pair['pair'])
  gl=[g for g in a['groups']if z['groups'][g]['intended_p99_ms']>a['groups'][g]['intended_p99_ms']]
  assert gl==[g for g in fr['groups']if fr['groups'][g]['p99_ratio']>1]
  for g in a['groups']:
   if g in gl:group_losses.append({'pair':pair['pair'],'group':g,'baseline_ms':a['groups'][g]['intended_p99_ms'],'candidate_ms':z['groups'][g]['intended_p99_ms']})
  if pair['losses']['rate']or pair['losses']['during']or pair['losses']['p99']or gl:losses.append(pair['pair'])
 assert set(losses)=={p['pair']for p in ref['regressions']}
 if host=='linux':
  for arm,record in config['candidates'].items():
   src=record['source'];assert sha(src['path'])==src['sha256']
   for name,digest in src['files'].items():
    p=Path(src['root'])/name;assert sha(p)==digest and oct(stat.S_IMODE(p.stat().st_mode))==src['modes'][name]
   for art in record['artifacts']:assert sha(art['original'])==sha(art['copy'])==art['sha256']
   for item in record['proofs']+record['compiler_tools']:assert sha(item['path'])==item['sha256']
 else:
  collected=bound(job/'COLLECTED.json');assert collected['original_handle']==end['mac']['original_handle']==67726 and collected['actual_exit']==0
  assert len(collected['files'])==1036
  for name,item in collected['files'].items():
   p=job/name;assert sha(p)==item['sha256']and p.stat().st_size==item['bytes']and oct(stat.S_IMODE(p.stat().st_mode))==item['mode']
 host_results[host]={'cases':48,'totals':r['totals'],'all_regression_pairs':losses,'group_p99_losses':group_losses,'loss_counts':{k:sum(p['losses'][k]for p in r['pairs'])for k in r['pairs'][0]['losses']},'cells':r['cells'],'source_config_go_end_bound':True,'device_cpu_policy_before_after_exact':True}
for name in ['recompute.py','linux.command.json','linux.stdout','linux.stderr','linux.exit','mac.command.json','mac.stdout','mac.stderr','mac.exit']:
 p=out/name;inputs[str(p)]={'sha256':sha(p),'bytes':p.stat().st_size}
for host in ['linux','mac']:
 r=load(out/(host+'.json'))
 for p in r['pairs']:
  for arm in ['baseline','candidate']:
   v=p[arm];assert math.isclose(v['average_busy_cores'],v['wholechild_cpu_s']/v['wholechild_wall_s'],rel_tol=1e-12)
receipt={'actual_exit':0,'cases':96,'hosts':host_results,'analysis_scope':'Independent standard-library raw CSV/summary/resource recomputation; no workload invoked and no frozen reader import. All96 native boundary counts/rates, nearest-rank applied intended/service and all-offer dispatch quantiles, groups/pending/backlog, recovery/retry/join, CPU/RSS and physical deltas agree. Every full/group-tail regression set exactly matches frozen reader.','recommendation':'Retain486 as a checked bounded Counter algorithm improvement: all12 paired loaded whole-child CPU/completed values improve acrosshosts, with useful median loaded rates and lower average busy cores. This is not broad latency/fairness/default/fullP9 acceptance: all raw singleton/moderate/skew/cold rate/tail/refusal losses remain, and cold/QUIC regressions are unresolved. Root owns final retention and user-requested archive/push/pause; no further experiment proposed.','retained_oracles':'Existing480 ready-admission oracle plus486 six independent legacy validator/iterator/state tests remain; no483 general/ingress API is present.','current_analysis_terminals':[{'host':'linux','original_handle':83430,'actual_exit':0,'terminal_chunk':'17c82c'},{'host':'mac','original_handle':55449,'actual_exit':0,'terminal_chunk':'d06f95'}],'current_analysis_failures':[],'limits':['CPU/completed has whole-child CPU numerator including startup/warmup/verification/recovery/cleanup and measurement-completed original denominator. Average busy cores is user+system child CPU divided by the matching wrapper child-wall elapsed, not native throughput times CPU/original. RSS is peak observation, not a debt plateau.','Raw summaries verify same fixed per-replica1WAL/1snapshot/1peer endpoint worker topology. No fixed CPU affinity, exclusive physical-core limit or historical eligible-core observation was imposed; do not claim that new constraint satisfied retroactively.','Finite offered-cap/window cases with3 pairs do not prove maximum sustainable capacity, causation of tails, full fairness, original framework floors, fullP9 or third-filesystem acceptance.','Native storage elapsed is inclusive/nested; append-unit histogram includes metadata and is not proposal width; encoded bytes/block counts are not physical device-I/O bytes.'],'inputs':inputs}
(out/'RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n');print('INDEPENDENT487_96_PASS',sha(out/'RECEIPT.json'));print('RECOMMEND_RETAIN486_SCOPED_CPU_WORK_REDUCTION_WITH_ALL_GUARD_LOSSES')
