#!/usr/bin/env python3
"""Collect only terminal fresh472 Linux artifact/control attempts. No subprocesses."""
from pathlib import Path
import gzip,hashlib,json,shutil,stat
ROOT=Path(__file__).resolve().parents[3]
JOB=ROOT/'target/p9/slice472'
OUT=ROOT/'validation/performance/pipeline472/fresh-linux'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def load(p):return json.loads(Path(p).read_text())
def main():
 native=ROOT/'target/p9/native472-linux';failed=ROOT/'target/p9/matched472-linux';matched=ROOT/'target/p9/matched472-linux-attempt2'
 assert load(native/'BUILD_TERMINAL.json')['actual_exit']==0
 assert load(native/'controls/TERMINAL.json')['exit']==0 and load(native/'controls/TERMINAL.json')['same_selected_inputs']
 assert load(failed/'BUILD_TERMINAL.json')['actual_exit']==101
 assert load(matched/'BUILD_TERMINAL.json')['actual_exit']==0
 assert load(matched/'CLI_TERMINAL.json')['actual_exit']==0
 check=load(matched/'manifest-qualification/TERMINAL.json');assert check['actual_exit']==0 and check['tests_passed']==70 and check['full_source_before_after_equal']
 assert load(matched/'manifest-qualification/SOURCE_BEFORE.json')==load(matched/'manifest-qualification/SOURCE_AFTER.json')
 assert len(load(native/'SOURCE.json')['sha256'])==1465 and len(load(matched/'SOURCE.json')['sha256'])==1467
 assert not OUT.exists(),'Refuse overwrite of sealed/partial fresh archive'
 selected={}
 def add(p,relative):
  p=Path(p);assert p.is_file() and not p.is_symlink(),p
  relative=str(relative);assert not Path(relative).is_absolute() and '..' not in Path(relative).parts
  assert relative not in selected or sha(selected[relative].read_bytes())==sha(p.read_bytes())
  selected[relative]=p
 def tree(directory,prefix):
  for p in Path(directory).rglob('*'):
   if p.is_file() and '__pycache__' not in p.parts:add(p,Path(prefix)/p.relative_to(directory))
 for label,job in [('native472',native),('framework-failed-attempt1',failed),('framework-attempt2',matched)]:
  for p in job.iterdir():
   if p.is_file() and p.name!='source.tar':add(p,Path(label)/p.name)
  tree(job/'logs',Path(label)/'logs')
 tree(native/'control-tools','native472/control-tools')
 controls=native/'controls'
 for p in controls.iterdir():
  if p.is_file():add(p,Path('native472/controls')/p.name)
 for case in ['tcp-startup','tcp-shared','quic-startup','quic-shared']:
  for p in (controls/case).iterdir():
   if p.is_file() and p.suffix in {'.txt','.csv','.json'}:add(p,Path('native472/controls')/case/p.name)
 tree(failed/'preparation-attempt1','framework-failed-attempt1/preserved-preparation')
 tree(matched/'manifest-qualification','framework-attempt2/manifest-qualification')
 add(JOB/'ALL_ARTIFACT_SETUP_END.json','ALL_ARTIFACT_SETUP_END.json')
 add(Path(__file__).resolve(),'collect-fresh.py')
 tree(JOB/'publication','published472-proof')
 OUT.mkdir(parents=True);packing=[];selection=[]
 for name,p in sorted(selected.items()):
  raw=p.read_bytes();assert not raw.startswith((b'\x7fELF',b'!<arch>\n')),p
  archived=name+'.gz' if len(raw)>65536 else name;target=OUT/archived;target.parent.mkdir(parents=True,exist_ok=True)
  if archived!=name:
   target.write_bytes(gzip.compress(raw,compresslevel=9,mtime=0));target.chmod(stat.S_IMODE(p.stat().st_mode));packing.append({'original_path':name,'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'container_bytes':target.stat().st_size,'container_sha256':sha(target.read_bytes())})
  else:shutil.copy2(p,target)
  selection.append({'source_path':str(p),'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'source_mode':oct(stat.S_IMODE(p.stat().st_mode))})
 (OUT/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n');(OUT/'SELECTION.json').write_text(json.dumps(selection,indent=2)+'\n')
 (OUT/'README.md').write_text('''# Fresh472 Linux artifact and functional control proof

All jobs were terminal before collection. Native export83325/build48082 and
four startup/shared TCP/QUIC controls87376 passed. Source1465=1453qualified+12
supplemental committed config/docs;7private fixtures retain mode0600. Actual
CargoJSON/vv root+example+buildscript fresh:false, opt3/debug0/assert+overflow,
native DEFAULT LTO/codegen and identical within-host compiler/protoc are bound by
PROFILE_PROOF/ARTIFACTS/SYMBOLS and raw logs. No DWARF/DWP claim for native debug0.
Native executable44358562…e79034 original/copy hashes match. All14functional
commands passed:128 originals,192 recovered, retries/joins and three-replica
create/recover depth4 readbacks. Each shared leader measured1 synchronized
multi-unit call with48known suffix originals; missing/drop/errors0. Activation
is distinct from measured release capacity/amortization.

The first framework export39552 passed its INCOMPLETE projection. Synchronous
build chunk56b6bd exited101 in0.303s BEFORE compilation: missing exported
benchmarks/matched/Cargo.toml (lock also omitted). The original1453 checked
scope captured matched Rust but omitted its manifest+lock; inherited export
extras wrongly assumed those were already qualified. Original source inventories,
scripts, actual101/logs and preserved preparation remain unchanged. No timing
or successful workload was rerun due to this failure. Native graph was complete
for its root/native build and its successful source was not mutated.

Corrected fresh framework-attempt2 adds only the two committed workspace files
as supplemental configuration, plus an essential guard before compilation.
Export50166/build23097 passed; SOURCE1467 and all physical modes are retained.
Actual root/executable/pinned OpenRaft+bench-minimal are fresh:false under
opt3/debug2packed/assert+overflow/thinLTO/CGU1; release rootbuildscript oracle,
original/copied ELF3318a42a…8c1419 and DWPc352b7dc…77ebd have separate evidence.
Actual3CLI OFF/ON/OpenRaft16K controls (synchronous chunka490e4 exit0) validate
frontiers17409x3/per-original outcomes and29phases/4scopes/15methods.

MANIFEST_LOCK_IDENTITY_SUPPLEMENT is RETROSPECTIVE: both files match published
parenta01, published472de5a7221 and frozen470. It does not invent original
before/after coverage. To close that specific omission, frozen-source standalone
matched70 original75425 ran once with a distinct debug target and full1467
SHA/mode before+after inventories:70passed0failed, exact equality. Original
qualification1453 scope remains unchanged. No broad root suites were repeated.

Raw commands/env/status/CargoJSON/vv/compiler/source/profile/oracle/symbol hashes
are preserved separately per attempt. Three inherited baseline-reproduced native
owner timeouts remain failures, not reexecuted or represented as passing here.
The finite24native campaign is archived separately by root; Mac and framework
paired timing evidence are separate. No performance retention or full P9 claim
is made by this artifact/functional archive. Same instrumentation can have
workload/grouping-dependent cost; physical suffix commands are not client success.

No binaries/sidecar bytes, full source, build trees, WAL/checkpoint storage or
source.tar are included. Selected case data are only top-level logs/CSV/readbacks.
Files>64KiB are losslessgzip with original/container SHA and length in PACKING.
SELECTION records original path/mode/hash. Nested manifests remain payloads.
SHA256SUMS covers exact recursive regular files except itself. Historical absolute
paths, failed statuses and old preparation text retain their original spelling.
''')
 payloads=sorted(p for p in OUT.rglob('*') if p.is_file());assert all(not p.is_symlink() for p in payloads)
 (OUT/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(OUT))+'\n' for p in payloads))
 expected={r:d for d,r in [line.split('  ',1) for line in (OUT/'SHA256SUMS').read_text().splitlines()]};assert set(expected)=={str(p.relative_to(OUT)) for p in OUT.rglob('*') if p.is_file() and p!=OUT/'SHA256SUMS'}
 for name,digest in expected.items():assert sha((OUT/name).read_bytes())==digest
 packed={r['archived_path']:r for r in packing}
 for row in selection:
  data=(OUT/row['archived_path']).read_bytes();raw=gzip.decompress(data) if row['archived_path'] in packed else data
  assert len(raw)==row['original_bytes'] and sha(raw)==row['original_sha256']==sha(Path(row['source_path']).read_bytes())
 for row in packing:assert (OUT/row['archived_path']).stat().st_size==row['container_bytes'] and sha((OUT/row['archived_path']).read_bytes())==row['container_sha256']
 print(json.dumps({'archive':str(OUT),'payloads':len(payloads),'gzip':len(packing),'manifest_sha256':sha((OUT/'SHA256SUMS').read_bytes()),'exact_set_original_container_readback':'PASS'}))
if __name__=='__main__':main()
