"""Finish only existing default-FileLogIo readback/physical evidence; no subprocess."""
from pathlib import Path
import hashlib,json
controls=Path(__file__).resolve().parent;job=controls.parent
load=lambda p:json.loads(Path(p).read_text());h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();write=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
assert not(controls/'COMPLETED_RESULTS.json').exists();original=load(controls/'TERMINAL.json');assert original['actual_exit']==1 and original['cases']==2 and original['source_exact']
name='tcp-default-atomic';root=controls/'wal'/name;publication='atomic';summary=dict(x.split('=',1)for x in(root/'summary.txt').read_text().split())
process=load(controls/(name+'.exit.json'));validator=load(controls/(name+'-check.exit.json'));extra=load(controls/(name+'-publication-check.exit.json'))
assert process['exit']==validator['exit']==0 and extra['exit']==1
assert '--publication'not in process['command']and'--ready-items'not in process['command']
assert not(root/'publication.csv').exists()and not any(k.startswith('publication_')for k in summary)
assert 'ENOENT'in(controls/(name+'-publication-check.stderr')).read_text()
assert summary['ready_visit_items_requested']==summary['ready_visit_items_effective']=='16'and summary['core_proposal_entries']=='64'
assert summary['ready_visit_bytes']=='1048576'and summary['ready_visit_ms']=='2'and summary['ready_batch_capable']=='true'
assert summary['leader_placement']=='1:1'and summary['warmup']=='64'and summary['groups']=='1'
assert summary['retry_verified']==summary['workers_joined']=='true'and summary['recovered_value']=='576'
assert summary['offers']==summary['admitted']==summary['applied']=='512'
assert all(summary[k]=='0'for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])
assert int(summary['checkpoints'])>0 and int(summary['reclaims'])>0
lines=(controls/(name+'.stderr')).read_text().splitlines();readbacks=[x for x in lines if x.startswith('capacity_readback ')];assert len(readbacks)==6 and not any(x.startswith('publication_readback ')for x in lines)
rows=[dict(token.split('=',1)for token in line.split()[1:])for line in readbacks]
assert {(r['mode'],r['replica'])for r in rows}=={(mode,str(replica))for mode in ['Create','Recover']for replica in [1,2,3]}
assert all(r['offered_capacity_effective']==r['client_group_requests']=='4096'and r['shard_group_items']=='4112'for r in rows)
def crc(data):
 value=0xffffffff
 for b in data:
  value^=b
  for _ in range(8):value=(value>>1)^(0x82f63b78 if value&1 else 0)
 return value^0xffffffff
physical=[]
for replica in range(1,4):
 directory=root/('replica'+str(replica));selector=directory/'CURRENT';current=selector.read_bytes()
 assert len(current)==20 and current[:8]==b'VBLCUR01'and crc(current[:16])==int.from_bytes(current[16:],'little')and not(directory/'WITNESS-CURRENT').exists()
 generation=int.from_bytes(current[8:16],'little');wal=directory/('log.wal'if generation==0 else f'log.{generation%2}.wal');metadata=directory/('MANIFEST'if generation==0 else f'MANIFEST.{generation%2}');ack=metadata.read_bytes()
 assert len(ack)==52 and ack[:8]==b'VBLSTR02'and crc(ack[:48])==int.from_bytes(ack[48:],'little')and int.from_bytes(ack[40:48],'little')==wal.stat().st_size
 physical.append({'replica':replica,'generation':generation,'selector':str(selector),'wal_bytes':wal.stat().st_size,'metadata_bytes':len(ack),'selector_sha256':h(selector),'wal_sha256':h(wal),'metadata_sha256':h(metadata)})
write(controls/(name+'-PHYSICAL-COMPLETION.json'),physical)
source=load(job/'SOURCE.json');publication_source=job/'source/examples/benchmark/publication.rs';text=publication_source.read_text();assert h(publication_source)==source['sha256']['examples/benchmark/publication.rs']
assert 'return Ok(Self::Legacy)'in text and 'Self::Legacy => BenchJournal::Legacy(ObservedIo {'in text and 'FileLogIo::create(directory)?'in text and 'FileLogIo::open(directory)?'in text and 'if self != Self::Legacy {'in text
files=load(controls/'BEFORE.json');assert files==load(controls/'AFTER.json')and all(h(p)==v['sha256']and oct(Path(p).stat().st_mode&0o777)==v['mode']for p,v in files.items())
artifacts=load(job/'ARTIFACTS.json');assert all(h(a['original'])==h(a['copy'])==a['sha256']and a['compiler_record']['fresh']is False for a in artifacts)
results=load(controls/'RESULTS.json');assert len(results)==2 and {r['protocol']for r in results}=={'tcp','quic'}and all(r['selection']=='witness'and r['ready_quantum']==64 for r in results)
results.append({'selection':'default-atomic','ready_quantum':16,'native_command':process['command'],'protocol':'tcp','publication':'atomic','diagnostic_publication_selected':False,'summary':summary,'capacity_readbacks':readbacks,'publication_readbacks':[],'physical':physical,'default_binding_source':{'path':str(publication_source),'sha256':h(publication_source)},'validation_scope':'Default Legacy selects FileLogIo without native publication diagnostic trace. Therefore no publication.csv/readback/settings expected; full native storage validator already passed and actual CURRENT/MANIFEST CRC/lengths verify default format.'})
write(controls/'COMPLETED_RESULTS.json',results)
write(controls/'COMPLETION.json',{'exit':0,'source_exact':True,'cases':3,'actual_native_processes_pass':3,'actual_native_storage_validators_pass':3,'witness_publication_validators_pass':2,'original_controller_exit':1,'original_extra_default_publication_checker_exit':1,'postterminal_completion_only':True,'no_workload_or_validator_rerun':True,'default_publication_trace_expected':False,'preserved_original_terminal_sha256':h(controls/'TERMINAL.json'),'completed_results_sha256':h(controls/'COMPLETED_RESULTS.json'),'scope':'Focused harness-reference correction from the same original raw default case. Original extra checker failure and original controller exit1 remain unchanged; it required an opt-in diagnostics file absent from the default contract.'})
print(json.dumps(load(controls/'COMPLETION.json'),indent=2))
