"""Lossless Linux484 raw paired evidence; no workloads or validators rerun."""
from pathlib import Path
import datetime,gzip,hashlib,json,shutil
base=Path('target/p9/native484-ingress');cmp=base/'comparison';job=cmp/'linux/revision2/campaign';review=base/'review';out=Path('validation/performance/native484-ingress/comparison/linux')
load=lambda p:json.loads(Path(p).read_text())
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
assert not out.exists()
c=load(job/'CAMPAIGN_CONFIG.json');r=load(review/'actual484-linux/READOUT.json');end=load(base/'ROOT_QUIET_END.json')
assert r['valid_cases']==48 and not r['failures'] and len(r['pairs'])==24
assert r['actual_terminal']['exit']==0 and r['actual_terminal']['cases_terminated']==48
assert (job/'controller.exit').read_text().strip()=='0'
assert all(all(v==0 for v in x['actual_exits'].values())for x in r['records'])
assert sha(review/'raw_readout.py')=='02209e3306d1fc1c1685dec733f7633b146154834df2a414b768ada03baaa516'
proof={'scope':'Original Linux484 48-case paired480/483 finite offered native campaign; no workload, source, build, profiling or validator rerun during collection. Both actual host terminals preceded readout. Not sustained P9 acceptance.','original_handle':84364,'actual_terminal_exit':0,'terminal_tool_chunk':'52f183','readout_original_handle':19859,'readout_actual_exit':0,'readout_terminal_tool_chunk':'d4a3d9','exact_launch_command':['/usr/bin/python3.14',str(job.resolve()/'campaign.py'),'--authorized-quiet'],'source_commits':c['source_commits'],'config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),'readout_sha256':sha(review/'raw_readout.py'),'totals':r['totals'],'actual_processes':48,'actual_native_validators':48,'actual_physical_readbacks':48,'source_inputs':{a:len(v['source']['files'])for a,v in c['candidates'].items()},'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'limits':r['limits'],'exclusions':'No binaries, build trees, full production source, generated WAL/snapshot states, caches or bytecode. Source inventories and hashes remain.'}
save(review/'LINUX_ARCHIVE_PROOF.json',proof)
selected={}
def add(p,rel):
 p=Path(p);rel=str(rel);assert p.is_file() and not p.is_symlink(),str(p);assert rel not in selected,rel;selected[rel]=p
for p in job.rglob('*'):
 if p.is_file()and '__pycache__'not in p.parts:add(p,Path('campaign-job')/p.relative_to(job))
for case in c['cases']:
 for p in Path(case['wal_root']).iterdir():
  if p.is_file():assert p.suffix in ['.csv','.txt','.json'];add(p,Path('case-results')/case['name']/p.name)
# Preserve failed initial Linux preparation and the actual revision2 setup, excluding WAL trees.
for root in [cmp/'linux',cmp/'linux/revision2']:
 for p in root.iterdir():
  if p.is_file():add(p,Path('preparation/linux')/p.relative_to(cmp/'linux'))
for root in [cmp/'linux/campaign',cmp/'common',cmp/'final']:
 for p in root.rglob('*'):
  if p.is_file()and '__pycache__'not in p.parts:add(p,Path('preparation')/p.relative_to(cmp))
for p in cmp.iterdir():
 if p.is_file():add(p,Path('preparation')/p.name)
for name in ['ROOT_QUIET_GO.json','ROOT_QUIET_END.json','ROOT_PUBLISHED_GO.json']:
 add(base/name,Path('authority')/name)
for name in ['raw_readout.py','PREPARED_READOUT.json','REFERENCE_VALIDATION.json','REFERENCE481_CONTEXT.json','INDEPENDENT_CONFIG_REVIEW.json','config_review.py','config-review.stdout','config-review.stderr','config-review.exit','actual484-linux.stdout','actual484-linux.stderr','actual484-linux.exit','LINUX_ARCHIVE_PROOF.json','archive-linux.py']:
 add(review/name,Path('readout')/name)
for p in(review/'actual484-linux').iterdir():
 if p.is_file():add(p,Path('readout/actual484-linux')/p.name)
for arm,record in c['candidates'].items():
 for n,item in enumerate(record['proofs']):
  p=Path(item['path']);assert sha(p)==item['sha256'];add(p,Path('candidate-proof')/arm/(str(n).zfill(2)+'-'+p.name))
out.mkdir(parents=True);selection=[];packing=[]
for rel,p in sorted(selected.items()):
 b=p.read_bytes();assert not b.startswith((b'\x7fELF',b'!<arch>\n'))
 sensitive=p.suffix in ['.stdout','.stderr','.log']or('candidate-proof/'in rel and p.suffix=='.txt')
 packed=(len(b)>65536 or sensitive)and p.suffix!='.gz';saved=rel+('.gz'if packed else'');t=out/saved;t.parent.mkdir(parents=True,exist_ok=True)
 if packed:
  t.write_bytes(gzip.compress(b,compresslevel=9,mtime=0));assert gzip.decompress(t.read_bytes())==b
  packing.append({'original_path':rel,'archived_path':saved,'original_bytes':len(b),'original_sha256':sha(p),'container_bytes':t.stat().st_size,'container_sha256':sha(t)})
 else:shutil.copy2(p,t);assert t.read_bytes()==b
 selection.append({'source_path':str(p),'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(b),'source_mode':oct(p.stat().st_mode&0o7777)})
save(out/'SELECTION.json',selection);save(out/'PACKING.json',packing)
lines=['# Linux484 paired native ingress results','','Original controller 84364 actually terminated with exit 0 (tool chunk 52f183), with 48 END records, ALL_TIMING_END and CAMPAIGN_EXIT 0. Both host terminals preceded collection. All 48 native processes, frozen native validators and physical readback checks passed; no case was replaced or retried. Baseline480 f7b0ef1 and candidate483 efd383d use their original fresh native artifacts, explicit WitnessLogIo and three durable voters.','','| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline during/s | Candidate during/s | Baseline p99 ms | Candidate p99 ms | Baseline window refusals, trials | Candidate window refusals, trials |','| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |']
for cell in ['singleton','moderate','loaded','skew']:
 for proto in ['tcp','quic']:
  a=next(x for x in r['cells']if(x['cell'],x['protocol'],x['arm'])==(cell,proto,'baseline'));b=next(x for x in r['cells']if(x['cell'],x['protocol'],x['arm'])==(cell,proto,'candidate'))
  lines.append(f"| {cell} | {proto} | {a['rates']['completed_ops_s']['median']:.3f} | {b['rates']['completed_ops_s']['median']:.3f} | {a['rates']['during_ops_s']['median']:.3f} | {b['rates']['during_ops_s']['median']:.3f} | {a['quantiles']['intended']['p99_ms']['median']:.3f} | {b['quantiles']['intended']['p99_ms']['median']:.3f} | {','.join(map(str,a['statuses']['window_refused']['trials']))} | {','.join(map(str,b['statuses']['window_refused']['trials']))} |")
lines+=['','Three-trial medians, not a pooled score. All 391680 scheduled offers reconcile to 263904 applied originals and 127776 window refusals. Every other refusal class, unknown outcome and final pending count is zero. All cases recover 64+applied, verify original retry outcomes and join workers. There are 624 completed checkpoints, 1872 reclaims and zero maintenance skips. READOUT retains all 48 individual records, 24 adjacent pairs, median/min/max/trials, completed/during/drain rates, intended/service/dispatch nearest-rank p50/p95/p99/max, raw-ledger conservation and per-group values/backlogs. Intended/service quantify successful originals; dispatch includes all offers.','','Results are mixed with substantial regressions: 17/24 pairs lose completed throughput, 16/24 worsen intended p99, and 20/24 have either loss. Singleton TCP and both moderate protocols lose throughput in all three pairs. Loaded TCP paired ratios are 0.634/0.780/1.508; QUIC 1.029/0.998/0.925. These finite measurements do not establish a universal improvement. All prior481 regressions remain explicit reference context in REFERENCE481_CONTEXT.json; no prior score is spliced into this campaign.','','| Loaded protocol | Baseline CPU us/original | Candidate CPU us/original | Baseline leader barriers/original | Candidate leader barriers/original | Baseline mean barrier ms | Candidate mean barrier ms |','| --- | ---: | ---: | ---: | ---: | ---: | ---: |']
import statistics
for proto in ['tcp','quic']:
 vals=[]
 for arm in ['baseline','candidate']:
  q=[x for x in r['records']if(x['cell'],x['protocol'],x['arm'])==('loaded',proto,arm)]
  vals.append([statistics.median(x['wholechild_resources']['cpu_us_per_completed_original']for x in q)]+[statistics.median(x['replica_storage']['1'][k]for x in q)for k in ['barriers_per_completed_original','mean_barrier_ms']])
 a,b=vals;lines.append(f'| {proto} | {a[0]:.3f} | {b[0]:.3f} | {a[1]:.5f} | {b[1]:.5f} | {a[2]:.3f} | {b[2]:.3f} |')
lines+=['','Whole-child CPU includes warmup, maintenance, recovery, startup/cleanup and refused-offer work. CPU/original uses this wider boundary and is not stage attribution or measurement-only CPU. No allocator or causal inference is made. Whole-child RSS ranges from 15093760 to 36728832 bytes; Linux raw KiB is converted to bytes. This is not a continuous memory/debt plateau. Block counters are not byte counts. Host-poll maxima and per-original elapsed values describe fixture rounds, not individual visits.','','Physical work retains all three replicas, barrier/WAL-sync/publication deltas and primitive counters, plus cold initialization/reclaim/recovery stages. Nested elapsed scopes overlap; do not sum them as CPU time. Ordinary Witness publication has one WAL sync plus one witness-file sync, with no per-barrier file open/rename/directory sync. Metadata-inclusive commands/append-unit are not literal proposal width; codec bytes are not physical device I/O.','','| Skew cold group | Baseline p99 ms | Candidate p99 ms |','| --- | ---: | ---: |']
for proto in ['tcp','quic']:
 q=[next(x for x in r['group_cells']if(x['cell'],x['protocol'],x['arm'],x['group'])==('skew',proto,a,'2'))for a in ['baseline','candidate']]
 assert all(x['metrics']['completed']['trials']==[256]*3 and x['statuses']['window_refused']['trials']==[0]*3 for x in q)
 lines.append(f"| {proto} | {q[0]['quantiles']['intended']['p99_ms']['median']:.3f} | {q[1]['quantiles']['intended']['p99_ms']['median']:.3f} |")
lines+=['','All 256 cold offers eventually apply in every skew case, with zero cold refusals; TCP cold tails improve in the median while QUIC worsens. Hot/cold backlog, all per-group refusals and trial tails are preserved. Both groups lead on replica1 and the fixture limits each group to 2048 pending. This does not prove unrestricted cold fairness. All replicas share the same Btrfs volume/device, so aggregate skew throughput cannot replace a one-group floor or independent placement.','','Fixed cells remain singleton q16 256@8/window1, moderate q16 8192@128/window256, loaded q64 16000@2000/window4096, skew q64 8192@1024/window4096/groups2/31:1. There are 64 singleton warmups, active 2-second maintenance and the original 120-second drain bound. The existing retained-entry guard remains unchanged. This is a finite offered-load diagnostic, not sustained 20M-operation acceptance or maximum capacity.','','Before/after guards bind actual source bytes/modes, artifact original/copy identities, compiler/protoc/profile/runtime tools, fixture permissions and the complete validator closure. Linux stable1.98.1/LLVM22.1.8/protoc3.9.0 is unchanged within host; native opt3/debug0/assertions/overflow/default LTO and codegen remain unchanged. Source inventories contain 1495 baseline and 1503 candidate exported inputs, including supplemental schemas; candidate publication qualification covers 1502 inputs plus its exported schema. Fresh root/executable/buildscript freshness is established separately in the published480 and484 qualification archives. No build, profile, policy or filesystem changes occurred during timing.','','The first Linux preparation failed under sandbox-only device visibility before READY, without a workload. Its raw error and original prepared files are retained beside revision2 host-visible setup and the exact final config. Unpublished sentinel refusal, focused qualification guard attempts and every raw stderr/output/exit remain. Raw logs are losslessly gzip-packed, including whitespace-sensitive small stdout/stderr. SELECTION records original hashes, lengths and modes; PACKING records reversible containers; SHA256SUMS seals every payload. No binaries, source trees, build trees, generated WAL/snapshot states or caches are included.','','P9 remains active and incomplete: full sustained framework margins, real ext4/XFS acceptance, bounded debt/headroom, fairness, independent placement/shared physical barriers and P8 composition remain open. This diagnostic does not redefine the full goal.']
(out/'README.md').write_text('\n'.join(lines)+'\n')
for item in selection:
 p=out/item['archived_path'];b=gzip.decompress(p.read_bytes())if p.suffix=='.gz'and not item['source_path'].endswith('.gz')else p.read_bytes();assert len(b)==item['original_bytes']and hashlib.sha256(b).hexdigest()==item['original_sha256']
files=sorted(p for p in out.rglob('*')if p.is_file());(out/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in files))
receipt={**proof,'payloads':len(files),'originals':len(selection),'gzip':len(packing),'archive_bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'manifest_sha256':sha(out/'SHA256SUMS'),'all_original_SHA_length_gzip_roundtrip':'PASS','archive':str(out)};save(review/'LINUX_ARCHIVE_RECEIPT.json',receipt);print(json.dumps(receipt,indent=2))
