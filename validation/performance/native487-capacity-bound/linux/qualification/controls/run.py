from pathlib import Path
import hashlib,json,re,subprocess,time,datetime,os,signal,shutil,sys,traceback
job=Path(__file__).resolve().parent.parent;controls=job/'controls';h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();write=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
assert (job/'STATUS').read_text().strip()=='0' and (job/'STAGE').read_text().strip()=='DONE'
assert json.loads((job/'BUILD_TERMINAL.json').read_text())['actual_exit']==0
assert not (controls/'RUN.json').exists() and not (controls/'wal').exists()
node=str(Path(shutil.which('node')).resolve());python=str(Path(sys.executable).resolve());write(controls/'RUNTIME_TOOLS.json',{'python':{'path':python,'sha256':h(python)},'node':{'path':node,'sha256':h(node)}})
source=json.loads((job/'SOURCE.json').read_text());artifact=next(x for x in json.loads((job/'ARTIFACTS.json').read_text())if x['target']=='native_benchmark')
assert all(h(artifact[k])==artifact['sha256'] for k in ['original','copy'])
tools=job/'control-tools';assert not tools.exists();tools.mkdir();queue=['validation/check-native-benchmark.mjs'];copied={}
while queue:
 relative=queue.pop()
 if relative in copied:continue
 data=(job/'source'/relative).read_bytes();target=tools/Path(relative).name;target.write_bytes(data);copied[relative]=h(target)
 assert source['sha256'][relative]==h(target)
 queue.extend(str(Path(relative).parent/ref)for ref in re.findall(r"(?:from\s*|import\s*)['\"](\./[^'\"]+)['\"]",data.decode()))
write(tools/'SOURCE.json',{'commit':source['commit'],'sha256':copied});(controls/'wal').mkdir()
files={str(job/'source'/n):{'sha256':d,'mode':source['physical_modes'][n]}for n,d in source['sha256'].items()}
files.update({str(p):{'sha256':h(p),'mode':oct(p.stat().st_mode&0o777)}for p in [Path(__file__),*tools.iterdir()]})
write(controls/'BEFORE.json',files);write(controls/'ARTIFACT.json',artifact)
commands=[];results=[];code=1
write(controls/'RUN.json',{'scope':'Three native486 fresh release functional controls: explicit q64 Witness TCP/QUIC two-group31:1 plus omitted publication/ready flags default FileLogIo TCP one-group; no capacity/gain timing claim. Selectedwindow4096 is not achieved capacity/batchwidth.','start':datetime.datetime.now(datetime.timezone.utc).isoformat(),'selected':[['tcp','witness',64,2,'31,1'],['quic','witness',64,2,'31,1'],['tcp','default-atomic',16,1,None]]})
def crc(data):
 value=0xffffffff
 for b in data:
  value ^= b
  for _ in range(8):value=(value>>1)^(0x82f63b78 if value&1 else 0)
 return value^0xffffffff
active=None
received_signals=[]
def signal_owned(number):
 if active is not None:
  try:os.killpg(active.pid,number)
  except ProcessLookupError:pass
def interrupted(number,frame):
 received_signals.append(number);signal_owned(number)
for number in [signal.SIGINT,signal.SIGTERM]:signal.signal(number,interrupted)
def run(name,cmd):
 global active
 clock=time.monotonic();print('START '+name,flush=True);result=None;error=None
 try:
  with(controls/(name+'.stdout')).open('wb')as out,(controls/(name+'.stderr')).open('wb')as err:
   active=subprocess.Popen(cmd,stdout=out,stderr=err,start_new_session=True)
   write(controls/(name+'.OWNED.json'),{'pid':active.pid,'process_group':active.pid,'command':cmd})
   if received_signals:signal_owned(received_signals[-1])
   result=active.wait()
 except BaseException:error=traceback.format_exc()
 finally:
  if active is not None and active.poll()is None:
   signal_owned(signal.SIGTERM)
   try:result=active.wait(timeout=10)
   except subprocess.TimeoutExpired:signal_owned(signal.SIGKILL);result=active.wait()
  active=None
  receipt={'command':cmd,'exit':result,'elapsed_s':time.monotonic()-clock,'error':error,'received_signals':list(received_signals)}
  write(controls/(name+'.exit.json'),receipt);commands.append(receipt);print('END '+name+' '+str(result),flush=True)
 assert result==0 and error is None and not received_signals,receipt

try:
 for protocol,selection,quantum,group_count,weights in [('tcp','witness',64,2,'31,1'),('quic','witness',64,2,'31,1'),('tcp','default-atomic',16,1,None)]:
  publication='atomic'if selection=='default-atomic'else'witness'
  name=protocol+'-'+selection;root=controls/'wal'/name;assert not root.exists()
  command=[artifact['copy'],str(root),protocol,'512','4096',str(group_count),'--offered','128','--maintenance','2','--offered-capacity','4096']
  if selection=='witness':command.extend(['--ready-items','64','--publication','witness','--group-weights',weights])
  else:assert '--publication'not in command and '--ready-items'not in command
  run(name,command)
  run(name+'-check',[node,str(tools/'check-native-benchmark.mjs'),'--storage',str(root)])
  if selection=='witness':run(name+'-publication-check',[node,str(tools/'check-publication-timings.mjs'),str(root/'publication.csv'),publication])
  summary=dict(x.split('=',1)for x in(root/'summary.txt').read_text().split())
  stderr=(controls/(name+'.stderr')).read_text().splitlines()
  readbacks=[x for x in stderr if x.startswith('capacity_readback ')]
  publication_readbacks=[x for x in stderr if x.startswith('publication_readback ')]
  for records in [readbacks]+([publication_readbacks]if selection=='witness'else[]):
   rows=[dict(token.split('=',1)for token in line.split()[1:])for line in records]
   assert len(rows)==6 and {(r['mode'],r['replica'])for r in rows}=={(mode,str(replica))for mode in ['Create','Recover']for replica in [1,2,3]}
  assert all('offered_capacity_effective=4096 'in x and 'client_group_requests=4096 'in x and 'shard_group_items=4112 'in x for x in readbacks)
  if selection=='witness':
   assert all('publication_effective=witness 'in x and 'publication_timings=native 'in x for x in publication_readbacks)
   assert summary['publication_requested']==summary['publication_effective']=='witness'
   assert summary['publication_provider']=='WitnessLogIo'and summary['publication_timings']=='native'
  else:
   assert not publication_readbacks and not(root/'publication.csv').exists()
   assert not any(k.startswith('publication_')for k in summary)
   text=(job/'source/examples/benchmark/publication.rs').read_text()
   assert 'return Ok(Self::Legacy)'in text and 'Self::Legacy => BenchJournal::Legacy(ObservedIo {'in text
   assert 'FileLogIo::create(directory)?'in text and 'FileLogIo::open(directory)?'in text
  assert summary['ready_visit_items_requested']==summary['ready_visit_items_effective']==str(quantum)
  assert summary['core_proposal_entries']=='64'and summary['ready_visit_bytes']=='1048576'and summary['ready_visit_ms']=='2'and summary['ready_batch_capable']=='true'
  assert summary['leader_placement']==','.join(str(g)+':1'for g in range(1,group_count+1))and summary['warmup']=='64'and summary['groups']==str(group_count)
  if weights is not None:assert summary['workload_group_weights']==weights and summary['workload_group_phase']=='absolute_operation_minus_one'
  else:assert 'workload_group_weights'not in summary and '--group-weights'not in command
  assert summary['retry_verified']==summary['workers_joined']=='true' and int(summary['recovered_value'])==64+int(summary['applied'])
  assert int(summary['offers'])==int(summary['admitted'])==int(summary['applied'])==512
  assert all(int(summary[k])==0 for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])
  assert int(summary['checkpoints'])>0 and int(summary['reclaims'])>0
  assert summary['offered_capacity_requested']==summary['offered_capacity_effective']=='4096'
  physical=[]
  for replica in range(1,4):
   directory=root/('replica'+str(replica));selector=directory/('CURRENT'if publication=='atomic'else'WITNESS-CURRENT')
   current=selector.read_bytes();assert len(current)==20
   assert current[:8]==(b'VBLCUR01'if publication=='atomic'else b'VBWCUR01')
   assert not(directory/('CURRENT'if publication=='witness'else'WITNESS-CURRENT')).exists()
   assert crc(current[:16])==int.from_bytes(current[16:],'little');generation=int.from_bytes(current[8:16],'little')
   wal=directory/('log.wal'if generation==0 else f'log.{generation%2}.wal')
   metadata=directory/('MANIFEST'if generation==0 else f'MANIFEST.{generation%2}')
   ack=metadata.read_bytes()
   if publication=='witness':
    primary=wal.read_bytes()[:128];assert len(primary)==len(ack)==128
    for record,role in [(primary,1),(ack,2)]:
     assert record[:8]==b'VBWACK01' and int.from_bytes(record[8:12],'little')==1 and int.from_bytes(record[12:16],'little')==role
     assert int.from_bytes(record[16:24],'little')==generation and int.from_bytes(record[24:32],'little')>0
     assert all(int.from_bytes(record[start:end],'little')>0 for start,end in [(32,48),(48,56),(56,64)]) and record[72:124]==bytes(52)
     assert crc(record[:124])==int.from_bytes(record[124:],'little')
     assert int.from_bytes(record[64:72],'little')==wal.stat().st_size-128
    assert primary[16:124]==ack[16:124]
   else:
    assert len(ack)==52 and ack[:8]==b'VBLSTR02' and crc(ack[:48])==int.from_bytes(ack[48:],'little')
    assert int.from_bytes(ack[40:48],'little')==wal.stat().st_size
   physical.append({'replica':replica,'generation':generation,'selector':str(selector),'wal_bytes':wal.stat().st_size,'metadata_bytes':len(ack),'selector_sha256':h(selector),'wal_sha256':h(wal),'metadata_sha256':h(metadata)})
  write(controls/(name+'-PHYSICAL.json'),physical)
  results.append({'selection':selection,'ready_quantum':quantum,'groups':group_count,'weights':weights,'diagnostic_publication_selected':selection=='witness','native_command':command,'protocol':protocol,'publication':publication,'summary':summary,'capacity_readbacks':readbacks,'publication_readbacks':publication_readbacks,'physical':physical});write(controls/'RESULTS.json',results)
 code=0
finally:
 same=all(Path(p).exists() and h(p)==v['sha256'] and oct(Path(p).stat().st_mode&0o777)==v['mode']for p,v in files.items())
 write(controls/'AFTER.json',{p:{'sha256':h(p),'mode':oct(Path(p).stat().st_mode&0o777)}for p in files if Path(p).exists()})
 write(controls/'TERMINAL.json',{'actual_exit':code if same else 1,'source_exact':same,'commands':commands,'cases':len(results),'received_signals':received_signals,'all_owned_children_waited':active is None,'end':datetime.datetime.now(datetime.timezone.utc).isoformat()});print('CONTROLS_EXIT='+str(code),flush=True)
raise SystemExit(code if same else 1)
