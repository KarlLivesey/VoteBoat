"""Lossless fresh480 Linux qualification seal; no builds/checks/workloads."""
from pathlib import Path
import gzip,hashlib,json,shutil
job=Path(__file__).resolve().parent
out=Path('validation/performance/native480-admission/linux');assert not out.exists()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
load=lambda p:json.loads(p.read_text())
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
ready=load(job/'READY.json');review=load(job/'INDEPENDENT_ROOT_REVIEW.json');completion=load(job/'controls/COMPLETION.json')
assert ready['source_commit']=='f7b0ef12709bb7e7aad4f0279851aa035b508e9e'and review['exit']==0
assert ready['actual_export_exit']==ready['actual_build_exit']==ready['actual_postterminal_completion_exit']==0
assert ready['original_controls_exit']==1 and completion['original_controller_exit']==1
assert ready['actual_native_processes_pass']==ready['actual_native_storage_validators_pass']==3
assert completion['exit']==0 and completion['no_workload_or_validator_rerun']
for name,digest in ready['proof_sha256'].items():assert sha(job/name)==digest,name
selection={}
for p in job.iterdir():
 if p.is_file()and p.name not in ['source.tar','source.sha256','ARCHIVE_RECEIPT.json','archive.stdout','archive.stderr']:
  selection['qualification/'+p.name]=p
for folder in ['logs','control-tools','controls']:
 for p in(job/folder).rglob('*'):
  if not p.is_file()or '__pycache__'in p.parts:continue
  rel=p.relative_to(job)
  if folder=='controls'and'wal'in rel.parts:
   # Preserve each run's CSV/text results, never physical WAL/snapshot trees.
   if p.parent.parent!=job/'controls/wal':continue
   assert p.suffix in ['.csv','.txt','.json'],p
  selection['qualification/'+rel.as_posix()]=p
out.mkdir(parents=True);selected=[];packing=[]
for relative,p in sorted(selection.items()):
 assert not p.is_symlink();data=p.read_bytes()
 assert not data.startswith((b'\x7fELF',b'!<arch>\n',b'\xcf\xfa\xed\xfe',b'\xfe\xed\xfa\xcf'))
 whitespace=any(line.endswith((b' ',b'\t',b'\r'))for line in data.split(b'\n'))or data.endswith(b'\n\n')
 packed=(len(data)>65536 or whitespace)and p.suffix!='.gz'
 saved=relative+('.gz'if packed else'');target=out/saved;target.parent.mkdir(parents=True,exist_ok=True)
 if packed:
  target.write_bytes(gzip.compress(data,compresslevel=9,mtime=0));target.chmod(p.stat().st_mode&0o777)
  assert gzip.decompress(target.read_bytes())==data
  packing.append({'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'container_sha256':sha(target),'container_bytes':target.stat().st_size,'reason':'large or Git-whitespace-sensitive original text'})
 else:shutil.copy2(p,target);assert target.read_bytes()==data
 selected.append({'source_path':str(p),'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'source_mode':oct(p.stat().st_mode&0o777),'archived_mode':oct(target.stat().st_mode&0o777)})
save(out/'SELECTION.json',selected);save(out/'PACKING.json',packing)
proof={'exit':0,'source_commit':ready['source_commit'],'source_files':1495,'qualified_projection_files':1494,'ready_sha256':sha(job/'READY.json'),'native_artifact_sha256':ready['artifact_sha256'],'root_artifact_sha256':ready['root_artifact_sha256'],'original_handles':ready['original_handles'],'actual_export_exit':0,'actual_build_exit':0,'original_controls_exit':1,'separate_postterminal_completion_exit':0,'original_native_processes_and_storage_validators':3,'witness_publication_validators':2,'same_raw_default_filelogio_completion':True,'scope':'Fresh native480 Linux functional qualification only. No speedup, capacity or fullP9 acceptance claim. Original failed extra default checker/controller exit1 retained; no workload or validator rerun.','excluded':'Binaries/root rlib, target-native-release buildtree, full source directory/source.tar, redundant source.sha256 inventory, generated WAL/snapshot trees and caches. SOURCE.json retains full source/archive identities; published main qualification referenced, not recopied.'}
save(out/'ARCHIVE_PROOF.json',proof)
(out/'README.md').write_text('''# 480 Linux fresh native qualification

Published source f7b0ef12709bb7e7aad4f0279851aa035b508e9e contains the checked
temporary ready-admission optimization. Original export28822 and build90090
terminate0. Exact1495 source file hashes/modes include the unchanged1494 checked
projection plus the480 schema. Fresh root library, executable and buildscript
Cargo records prove fresh:false; verbose rustc proves native opt3/debug0,
assertions/overflow and native defaultLTO/codegen. Compiler/tools/locks match the
immutable476 within-host reference. Debug0 symbol-table/buildID evidence is
retained; framework debug2 artifacts are not substituted.

Three original native controls each apply512, recover576, verify retries and
join workers with zero unknown/pending/refusals and active maintenance. Explicit
Witness TCP/TLS and QUIC select ready64 and expose six capacity plus six provider
Create/Recover readbacks. Default FileLogIo selects ready16 and exposes six
capacity readbacks, zero opt-in publication readbacks and no publication.csv.
Actual native process and full storage validator exits are0 for all three;
both explicitWitness publication validators exit0.

Original controller36585 remains exit1: an additional checker wrongly expected
the opt-in publication diagnostics file from the default path. Its failure and
original terminal are retained. Separate postterminal completion0 uses exactly
the same raw default case and verifies FileLogIo selectors/manifests/source
binding and completed outcomes. No native case or validator was repeated, and
the failed original is not relabeled success. COMPLETION and COMPLETED_RESULTS
bind the correction to exact original hashes; independent review preserves its
own initial default-readback assumption failure as provenance.

This is functional readiness, not a throughput result. Later paired481 timing
must use the frozen candidate/reference artifacts on a quiet host. Defaults
remain FileLogIo/ready16; fullP9 and realext4/XFS acceptance remain incomplete.

CargoJSON, verbose invocations, original commands/exits/stdout/stderr, compiler
identities, source/mode inventories, qualification links, source/build guards,
all control results/errors, selected physical hashes and raw CSV/text outcomes
are lossless. Large or whitespace-sensitive text is gzip-packed without
stripping; PACKING and SELECTION retain original hashes, lengths and modes.
SHA256SUMS seals all payloads except itself. Full source, build trees, binaries,
WAL/snapshot data and caches are excluded. Existing main qualification is linked
by exact path/hash rather than duplicating its complete inventory and logs.
''')
manifest=''.join(sha(p)+'  '+p.relative_to(out).as_posix()+'\n'for p in sorted(out.rglob('*'))if p.is_file())
(out/'SHA256SUMS').write_text(manifest)
for r in selected:
 p=out/r['archived_path'];data=p.read_bytes()
 if r['archived_path'].endswith('.gz')and not r['source_path'].endswith('.gz'):data=gzip.decompress(data)
 assert len(data)==r['original_bytes']and hashlib.sha256(data).hexdigest()==r['original_sha256']
 assert oct(p.stat().st_mode&0o777)==r['source_mode']==r['archived_mode']
receipt={**proof,'payloads':len(manifest.splitlines()),'selected_originals':len(selected),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'all_original_hashes_lengths_modes_roundtrip':True,'archive':str(out)}
save(job/'ARCHIVE_RECEIPT.json',receipt);print(json.dumps(receipt,indent=2))
