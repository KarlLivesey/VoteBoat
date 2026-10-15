#!/usr/bin/env python3
"""Finite completed native comparison collector; no subprocesses or workloads.
Requires explicit root terminal receipt: all_recorded_jobs_terminal=true,
revalidation_directory=<absolute path>, revalidation_exit=0, original_handle=33443,
original_exit=1. Original failed statuses remain immutable payloads.
"""
from pathlib import Path
import argparse,gzip,hashlib,json,shutil,stat
ROOT=Path(__file__).resolve().parents[3]
JOB=ROOT/'target/p9/comparison470-native/linux'
OUT=ROOT/'validation/performance/comparison470-native/linux'
def sha(data):return hashlib.sha256(data).hexdigest()
def load(p):return json.loads(Path(p).read_text())
def main():
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--terminal-scope',type=Path,required=True);scopefile=ap.parse_args().terminal_scope.resolve();scope=load(scopefile)
 assert scope['all_recorded_jobs_terminal'] is True and scope['original_handle']==33443 and scope['original_exit']==1 and scope['revalidation_exit']==0
 revalidation=Path(scope['revalidation_directory']);assert revalidation.is_dir()
 rv=load(revalidation/'TERMINAL.json');assert rv['exit']==0 and len(rv['commands'])==31 and all(c['exit']==0 for c in rv['commands'])
 assert (JOB/'CAMPAIGN_STAGE').read_text().strip()=='ALL_TIMING_END' and (JOB/'CAMPAIGN_STATUS').read_text().strip()=='1'
 config=load(JOB/'CAMPAIGN_CONFIG.json');campaign=load(JOB/'campaign/CAMPAIGN.json');assert len(campaign['cases'])==18 and all(c['exit']==0 for c in campaign['cases'])
 assert sum(any(code for code in c['validator_exits']) for c in campaign['cases'])==4
 assert not OUT.exists(),'No overwrite of sealed/partial archive'
 selected={}
 def add(p,relative):
  p=Path(p);relative=Path(relative);assert p.is_file() and not p.is_symlink(),p
  assert not relative.is_absolute() and '..' not in relative.parts
  if str(relative) in selected:assert sha(selected[str(relative)].read_bytes())==sha(p.read_bytes()),relative
  selected[str(relative)]=p
 def tree(directory,prefix):
  for p in sorted(Path(directory).rglob('*')):
   if p.is_file() and '__pycache__' not in p.parts:add(p,Path(prefix)/p.relative_to(directory))
 for p in JOB.iterdir():
  if p.is_file():add(p,p.name)
 for name in ['campaign','preparation-attempt1','campaign-metadata']:
  tree(JOB/name,name)
 for case in campaign['cases']:
  for p in Path(case['wal_root']).iterdir():
   if p.is_file() and p.suffix in {'.csv','.txt','.json','.stdout','.stderr'}:add(p,Path('case-output')/case['name']/p.name)
 tree(revalidation,'revalidation1')
 tools=Path(config['control_tools']['directory']);tree(tools,'control-tools')
 for variant in ['baseline','candidate']:
  native=Path(config[variant]['source_inventory']).parent
  for p in native.iterdir():
   if p.is_file() and (p.suffix in {'.json','.py','.sh','.txt','.sha256'} or p.name in {'STATUS','STAGE','PID'}):add(p,Path('identity')/variant/p.name)
  tree(native/'logs',Path('identity')/variant/'logs')
 for p in [ROOT/'target/p9/native470-linux/controls/ROOT_CONTROL_REVIEW.json',ROOT/'target/p9/native470-linux/controls/TERMINAL.json',ROOT/'target/p9/slice470/publication30/PUBLICATION.json']:
  add(p,Path('identity/actual-control-and-publication')/p.parent.name/p.name)
 for p in [JOB.parent/'SCHEMA.md',JOB.parent/'COPY_PLAN.md',Path(__file__).resolve(),JOB.parent/'readout.py']:add(p,Path('preparation')/p.name)
 add(scopefile,'TERMINAL_SCOPE.json')
 add(ROOT/'validation/performance/comparison470-native/GAUGE_FIX_SCHEMA.md','GAUGE_FIX_SCHEMA.md')
 OUT.mkdir(parents=True);packing=[];selection=[]
 for relative,source in sorted(selected.items()):
  raw=source.read_bytes();assert not raw.startswith(b'\x7fELF') and not raw.startswith(b'!<arch>\n'),source
  target=OUT/relative;target.parent.mkdir(parents=True,exist_ok=True);archived=relative
  if len(raw)>65536:
   archived=relative+'.gz';target=OUT/archived;target.write_bytes(gzip.compress(raw,compresslevel=9,mtime=0));target.chmod(stat.S_IMODE(source.stat().st_mode))
   packing.append({'original_path':relative,'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'container_bytes':target.stat().st_size,'container_sha256':sha(target.read_bytes())})
  else:shutil.copy2(source,target)
  selection.append({'source_path':str(source),'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'source_mode':oct(stat.S_IMODE(source.stat().st_mode))})
 (OUT/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n');(OUT/'SELECTION.json').write_text(json.dumps(selection,indent=2)+'\n')
 (OUT/'README.md').write_text('''# Native470 finite append-window comparison

Original33443 is terminal1: all18 workload processes exited0; four original raw
validators rejected a decreasing correlation-capacity snapshot gauge. All raw
failures remain in campaign/. Root revalidation1 fixes gauge classification and
rechecks EXISTING data; it does not rerun workloads or replace original statuses.
See TERMINAL_SCOPE.json for exact completed processing scope.

The primary comparison uses the SAME native470 ELF OFF(defaultNone/worker1)
versus ON(window4/64originals/1MiB). Immutable458 is older production455 and older
observer/workload code: historical context only. All cases are finite15000offers
at500/s, window64/ready16,64warmups,onegroup3voters,8B Counter, maintenance2s,
no pause. TCP/QUIC each rotate ABC/BCA/CAB. Runtime outcome ledgers distinguish
completed during horizon from final drain, refusals and all other outcomes.

OUTCOMES.json retains integer count/elapsed rational inputs, offer-ledger exact
p99 nanoseconds, all summary fields/resources, per-replica numeric storage
snapshots, leader and replica-inclusive counts, elapsed shares and gauges.
SUMMARY/PAIRED are derived; original CSV/summary/commands remain authoritative.
No consistent amortization benefit was established: TCP ON loses all3 rate
pairs; QUIC mixed. Median leader barriers/applied do not decrease in either
protocol. Positive multi-unit synchronization demonstrates activation only.
Same instrumentation code is used OFF/ON, but grouping-dependent ticket scan
and correlation work can differ; this measures instrumented end-to-end
usefulness, not observer-free algorithm cost. No causal allocation claim.

Successful client originals differ from suffix commands (replicas/retries) and
from physical publication calls. Missing/dropped/errors must remain explicit;
capacity_bytes and pending_tickets are snapshot gauges, not cumulative RSS.
Storage wall scopes overlap and are not summed as CPU. Linux maxRSS is rawKiB
and normalizedbytes for whole process/children including warmup/recovery/cleanup,
not engine ownership, a time-series plateau, or a memory/debt bound.

Preparation-attempt1 preserves the extra metadata argv bug caught BEFORE any
workload. The correction removes that argv only. No successful case was rerun.
Release activation controls original52319 are separate functional evidence,
not capacity. Three inherited owner-history failures remain separate; no P9
floor/sustained-headroom/fairness/acceptance claim follows from this matrix.

No binaries, full sources, build trees, WAL/checkpoint state, or perf data are
included. Files>64KiB use lossless gzip; PACKING records original/container SHA
and lengths. SELECTION records original paths/modes. Nested manifests remain
ordinary payloads. Outer SHA256SUMS covers every regular file recursively except
itself. Absolute paths in raw records retain their historical spelling.
''')
 payloads=sorted(p for p in OUT.rglob('*') if p.is_file());assert all(not p.is_symlink() for p in payloads)
 (OUT/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(OUT))+'\n' for p in payloads))
 expected={line.split('  ',1)[1]:line.split('  ',1)[0] for line in (OUT/'SHA256SUMS').read_text().splitlines()}
 assert set(expected)=={str(p.relative_to(OUT)) for p in OUT.rglob('*') if p.is_file() and p!=OUT/'SHA256SUMS'}
 for relative,digest in expected.items():assert sha((OUT/relative).read_bytes())==digest
 packed={r['archived_path']:r for r in packing}
 for row in selection:
  target=OUT/row['archived_path'];data=target.read_bytes();raw=gzip.decompress(data) if row['archived_path'] in packed else data
  assert len(raw)==row['original_bytes'] and sha(raw)==row['original_sha256']
  assert sha(selected[next(k for k,v in selected.items() if str(v)==row['source_path'])].read_bytes())==row['original_sha256']
 for row in packing:assert (OUT/row['archived_path']).stat().st_size==row['container_bytes'] and sha((OUT/row['archived_path']).read_bytes())==row['container_sha256']
 print(json.dumps({'archive':str(OUT),'payloads':len(payloads),'compressed':len(packing),'manifest_sha256':sha((OUT/'SHA256SUMS').read_bytes()),'exact_set_and_original_readback':'PASS'}))
if __name__=='__main__':main()
