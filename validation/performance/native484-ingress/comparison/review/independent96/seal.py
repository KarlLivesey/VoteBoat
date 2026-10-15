import hashlib,json,stat,math
from pathlib import Path
b=Path('target/p9/native484-ingress');out=b/'review/independent-actual';sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();load=lambda p:json.loads(Path(p).read_text());inputs={}
def bound(p):
 p=Path(p);inputs[str(p)]={'sha256':sha(p),'bytes':p.stat().st_size};return load(p)
go=bound(b/'ROOT_QUIET_GO.json');end=bound(b/'ROOT_QUIET_END.json');host_results={}
for host,job in [('linux',b/'comparison/linux/revision2/campaign'),('mac',b/'comparison/mac/collected')]:
 r=bound(out/(host+'.json'));ref=bound(b/('review/actual484-'+host+'/READOUT.json'));config=bound(job/'CAMPAIGN_CONFIG.json');terminal=bound(job/'TERMINAL.json');before=bound(job/'before-host.json');after=bound(job/'after-host.json')
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
  collected=bound(job/'COLLECTED.json');assert collected['original_handle']==end['mac']['original_handle']==2749 and collected['actual_exit']==0
  assert len(collected['files'])==1036
  for name,item in collected['files'].items():
   p=job/name;assert sha(p)==item['sha256']and p.stat().st_size==item['bytes']and oct(stat.S_IMODE(p.stat().st_mode))==item['mode']
 host_results[host]={'cases':48,'totals':r['totals'],'all_regression_pairs':losses,'group_p99_losses':group_losses,'loss_counts':{k:sum(p['losses'][k]for p in r['pairs'])for k in r['pairs'][0]['losses']},'cells':r['cells'],'source_config_go_end_bound':True,'device_cpu_policy_before_after_exact':True}
for name in ['recompute.py','recompute-initial.py','linux.stdout','linux.stderr','linux.exit','linux-attempt2.stdout','linux-attempt2.stderr','linux-attempt2.exit','mac.command.json','mac.stdout','mac.stderr','mac.exit']:
 p=out/name;inputs[str(p)]={'sha256':sha(p),'bytes':p.stat().st_size}
receipt={'actual_exit':0,'cases':96,'hosts':host_results,'analysis_scope':'Independent standard-library raw CSV/summary/resource recomputation; no workload invoked and no frozen reader import. All96 native boundary counts/rates, nearest-rank applied intended/service and all-offer dispatch quantiles, groups/pending/backlog, recovery/retry/join, CPU/RSS and physical deltas agree. Every full/group-tail regression set exactly matches frozen reader.','decision':'Reject483 ingress optimization and restore exact480 production/tests/contracts/native-offered source. Mac one-group loaded improves all6 pairs, but Linux17/24 rate losses, singleton/moderate regressions, and all6 Mac skew cold-tail losses fail a portable useful-throughput/fairness improvement.','retained_oracles':'Existing480 Counter ready-admission differential oracle remains. New483 general/ingress/Node/provider controls depend on the rejectedAPI and remain available in immutable483 commit and qualification evidence; no independent new retained API is required.','failures_preserved':[{'command':'Initial Linux recompute invocation in tool d2971d','actual_exit':1,'script':'recompute-initial.py','stdout':'linux.stdout','stderr':'linux.stderr','cause':'Analysis parser expected semicolon histogram separator; actual native raw uses |. Focused parser delimiter correction only; all original data untouched.'},{'corrected_linux_original_handle':48474,'actual_exit':0,'terminal_chunk':'0e8c2c'},{'mac_original_handle':90675,'actual_exit':0,'terminal_chunk':'f4db46'}],'limits':['CPU/RSS are whole child including startup/warmup/verification/recovery/cleanup, not native timer CPU or per-original causal evidence. RSS is peak process observation, not a memory/debt plateau.','Finite offered-cap/window cases with3 pairs do not prove maximum sustainable capacity, causation of tails, full fairness, original framework floors, fullP9 or third-filesystem acceptance.','Native storage elapsed is inclusive/nested; append-unit histogram includes metadata and is not proposal width; encoded bytes/block counts are not physical device-I/O bytes.'],'inputs':inputs}
(out/'RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n');print('INDEPENDENT96_PASS',sha(out/'RECEIPT.json'));print('RECOMMEND_REJECT483_RESTORE480')
