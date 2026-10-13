import hashlib,json,sys
from pathlib import Path
mode=sys.argv[1];p=Path(sys.argv[2])
if mode=='guards':
 v=json.loads(p.read_text());h=lambda q:hashlib.sha256(q.read_bytes()).hexdigest()
 for name,x in v['variants'].items():
  for q,d in x['source_sha256'].items():assert h(Path(x['source_root'])/q)==d,(name,q)
  for q,d in x['artifact_sha256'].items():assert h(Path(q))==d,(name,q)
 print('all frozen source/library/executable guards pass; same recorded compiler settings')
else:
 cases=[]
 for trial in [1,2,3]:
  for clients,batch in [(1,1),(4096,4)]:
   for variant in ['baseline','candidate','untouched']:
    label=f'{variant}-c{clients}-b{batch}-t{trial}'
    assert (p/f'{label}.exit').read_text().strip()=='0',label
    v=json.loads((p/f'{label}.json').read_text())
    if variant=='untouched':
     assert v['operations']==v['requested_operations']==131072 and v['clients']==clients and v['batch']==batch
     assert v['warmup_writes']==1024 and v['samples']==131072//batch and v['members']==[0,1,2]
     assert v['client_workers']==1 and v['server_workers']==16 and all(v[k]==0 for k in ['append_delay_us','flush_delay_us','network_delay_us','write_rate'])
    else:
     assert v['settings']['operations']==v['completed_originals']==131072 and v['completed_requests']==131072//batch
     assert v['settings']['clients']==clients and v['settings']['batch']==batch and v['settings']['warmup']==1024
     assert v['applied_frontiers']==[132097]*3 and v['durability']=='volatile_no_disk'
     assert v['completion']=='every_original_locally_applied_after_quorum_commit'
     assert v['settings']['maintenance']=='default_retention' and v['settings']['envelopes']=='upstream_default'
     assert [v[k] for k in ['api_entry_batch_capacity','append_entry_batch_capacity','replication_entry_capacity']]==[4096,4096,1024]
     leader=v['diagnostics'][0];assert leader['proposed_originals']==132096
     assert sum(x['original_entries']*x['batches'] for x in leader['proposal_widths'])==132096
    cases.append({'label':label,'variant':variant,'trial':trial,'clients':clients,'batch':batch,'json':v})
 assert len(cases)==18
 (p/'RESULTS.json').write_text(json.dumps({'scope':'Mac incremental matched414 versus frozen8a5840d/Core415; wrapper416 is not exercised; actual recorded OpenRaft minimal control, no published floor qualification','cases':cases,'source_artifact_guards':True,'every_original_guard':True},indent=2)+'\n')
 print('18 original completion/policy/identity controls pass')
