"""Lossless fresh483 Linux artifact/control evidence, adapted from480 archive."""
from pathlib import Path
import gzip,hashlib,json,shutil
job=Path(__file__).resolve().parent
out=Path('validation/performance/native484-ingress/linux');assert not out.exists()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
load=lambda p:json.loads(p.read_text())
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
ready=load(job/'READY.json');review=load(job/'INDEPENDENT_ROOT_REVIEW.json');source=load(job/'SOURCE.json');originals=load(job/'ORIGINALS.json');terminal=load(job/'controls/TERMINAL.json')
assert ready['source_commit']==source['commit']==review['commit']=='efd383dc36b4b9edf8f66021b90115824440b468'and review['exit']==0
assert ready['actual_export_exit']==ready['actual_build_exit']==ready['actual_network_controls_exit']==0
assert ready['actual_native_processes_pass']==ready['actual_native_storage_validators_pass']==3
assert ready['actual_instrumented_witness_publication_validators_pass']==2 and ready['default_legacy_filelogio_physical_verified']
assert terminal['actual_exit']==0 and terminal['source_exact']and terminal['cases']==3 and terminal['all_owned_children_waited']
assert source['inputs']==1503 and source['functional_projection_inputs']==1502 and source['added_artifact_configuration_inputs']==['validation/performance/native483-ingress/SCHEMA.md']
assert all(v['actual_exit']==0 for v in originals['originals'].values())
assert sha(job/'READY.json')==originals['ready_sha256']==review['ready_sha256']
for name,digest in ready['proof_sha256'].items():assert sha(job/name)==digest,name
selection={}
for p in job.iterdir():
 if p.is_file()and p.name not in ['source.tar','ARCHIVE_RECEIPT.json','archive.stdout','archive.stderr']:
  selection['qualification/'+p.name]=p
for folder in ['logs','control-tools','controls']:
 for p in(job/folder).rglob('*'):
  if not p.is_file()or '__pycache__'in p.parts:continue
  rel=p.relative_to(job)
  if folder=='controls'and'wal'in rel.parts:
   if p.parent.parent!=job/'controls/wal':continue
   assert p.suffix in ['.csv','.txt','.json'],p
  selection['qualification/'+rel.as_posix()]=p
# Copy direct and normalized qualification/publication receipt references. Keep
# their absolute original paths untouched; SELECTION supplies the archive map.
repo=job.parents[3]
def reference(item):
 p=Path(item['path']);assert sha(p)==item['sha256'],p
 relative=p.relative_to(repo)
 name='root-qualification/'+relative.as_posix()
 assert name not in selection or selection[name]==p
 selection[name]=p
 if p.suffix=='.json':
  value=load(p)
  for child in value.get('proofs',{}).values():reference(child)
q=load(job/'FUNCTIONAL_QUALIFICATION.json');reference(q['source_inventory'])
for item in q['proofs'].values():reference(item)
for pair in q['final_source_guards']:
 for item in pair.values():reference(item)
pub=load(job/'PUBLICATION.json');reference(pub['proof']);reference(pub['hook'])
selection['root-authority/ROOT_PUBLISHED_GO.json']=job.parent/'ROOT_PUBLISHED_GO.json'
out.mkdir(parents=True);selected=[];packing=[]
for relative,p in sorted(selection.items()):
 assert not p.is_symlink();data=p.read_bytes()
 assert not data.startswith((b'\x7fELF',b'!<arch>\n',b'\xcf\xfa\xed\xfe',b'\xfe\xed\xfa\xcf'))
 whitespace=any(line.endswith((b' ',b'\t',b'\r'))for line in data.split(b'\n'))or data.endswith(b'\n\n')
 packed=(len(data)>65536 or whitespace or p.suffix in ['.stdout','.stderr','.log','.txt'])and p.suffix!='.gz'
 saved=relative+('.gz'if packed else'');target=out/saved;target.parent.mkdir(parents=True,exist_ok=True)
 if packed:
  target.write_bytes(gzip.compress(data,compresslevel=9,mtime=0));target.chmod(p.stat().st_mode&0o777)
  assert gzip.decompress(target.read_bytes())==data
  packing.append({'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'container_sha256':sha(target),'container_bytes':target.stat().st_size,'reason':'large or Git-whitespace-sensitive original text'})
 else:shutil.copy2(p,target);assert target.read_bytes()==data
 selected.append({'source_path':str(p),'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'source_mode':oct(p.stat().st_mode&0o777),'archived_mode':oct(target.stat().st_mode&0o777)})
save(out/'SELECTION.json',selected);save(out/'PACKING.json',packing)
proof={'exit':0,'source_commit':ready['source_commit'],'source_files':source['inputs'],'qualified_projection_files':source['functional_projection_inputs'],'extra_committed_source':source['added_artifact_configuration_inputs'],'ready_sha256':sha(job/'READY.json'),'native_artifact_sha256':ready['artifact_sha256'],'root_artifact_sha256':ready['root_artifact_sha256'],'native_build_id':originals['build_id'],'original_handles':{k:v['handle']for k,v in originals['originals'].items()},'actual_export_exit':0,'actual_build_exit':0,'actual_controls_exit':0,'native_processes_and_storage_validators':3,'witness_publication_validators':2,'default_legacy_filelogio_verified':True,'independent_root_review_exit':review['exit'],'independent_root_review_sha256':sha(job/'INDEPENDENT_ROOT_REVIEW.json'),'scope':'Fresh native483 Linux artifact/control qualification only; no speedup, capacity or fullP9 acceptance claim. All original processes actually terminate0; expected unpublished guard refusal1 is preserved, not a workload failure.','excluded':'Binaries/root rlib, target-native-release buildtree, full source directory/source.tar, generated WAL/snapshot trees and caches. Source inventories retain exact hashes/gitblob IDs/physical modes/archive SHA. Copied main 483 publication/sourceguards/normalized proofs and referenced actual exit receipts make authority inspectable without duplicating full main source or suite logs.'}
save(out/'ARCHIVE_PROOF.json',proof)
(out/'README.md').write_text("""# 484 Linux fresh native artifact qualification

Published 483 source `efd383dc36b4b9edf8f66021b90115824440b468` was exported once
(original 76794, actual 0), built fresh once (85533, actual 0), then functionally
controlled once (41354, actual 0). ORIGINALS preserves actual terminal chunks and
commands. Export includes 1503 exact files/modes: the 1502 checked source inputs
plus the committed 483 schema. All seven private-key fixtures retain 0600 modes.

CargoJSON and verbose rustc prove the root library, executable and buildscript
compiled fresh:false. The actual native profile is opt3/debug0 with assertions
and overflow checks, native defaultLTO/codegen; inherited flags/wrappers are
unset. Compiler/protoc bytes match immutable 480. Original and copied executable
SHA is 0ff042c1ade83771217ed96af4a0f24912d79917eb4f95cb0d8230808780963d;
root rlib SHA 8643d5a310e63ff5a1de9d311054e005d4b5841237e8b647cd66710f2a86366b.
ELF buildID ee9d5a035f2a39596b453aca1993d1f2223abdcf and debug0 symbol evidence
are retained. The unchanged 480 executable 8d3530a6… remains the future baseline.

Three original functional controls each apply 512 originals, recover 576,
verify individual outcomes/retries, join workers, and finish one checkpoint
and three reclaims with zero refusals/unknown/pending. Explicit Witness TCP/TLS
and QUIC select ready64, two groups, 31:1 offers, 512@128/s/window4096; each has six
capacity and six provider Create/Recover readbacks. Both publication validators
and all three full native/storage validators exit 0. The omitted-publication/
omitted-ready default TCP control uses FileLogIo/ready16/one group: six capacity
readbacks, no opt-in publication fields/readbacks/CSV. Its real CURRENT 20B and
MANIFEST 52B CRC/acknowledged WAL lengths verify; no nonexistent diagnostics file
is required. These are functional controls, not capacity or speedup measurements.

Independent root raw review passed without repeating builds/workloads/checkers.
READY and every bound proof remain unchanged. PREPARATION and the expected
UNSET guard refusal preserve the earlier unpublished state; placeholder hashes
in that historical preparation refer to the prepublication placeholders, while
the archived PUBLICATION/FUNCTIONAL_QUALIFICATION are the actual later bindings.
No failed workload was hidden or replaced. Copied normalized main 483 proofs,
actual exit receipts, enabled-hook publication receipt and 1502 before/after
source guard inventories retain original references; SELECTION maps them to
lossless archive copies.

Full CargoJSON/vv output, compiler/tool identities, source/archive/hash/mode
inventories, guards, commands/exits/stdout/stderr, raw outcome/storage/publication/
maintenance CSVs and selected physical proofs are preserved. Text and large or
whitespace-sensitive files are gzip-packed upfront; original lengths/hashes/modes
and exact roundtrips are recorded. Full source, build trees, binaries and generated
WAL/snapshot states are excluded. SHA256SUMS seals every payload except itself.
No paired campaign has started. Full P9 and real ext4/XFS acceptance remain open.
""")
manifest=''.join(sha(p)+'  '+p.relative_to(out).as_posix()+'\n'for p in sorted(out.rglob('*'))if p.is_file())
(out/'SHA256SUMS').write_text(manifest)
for r in selected:
 p=out/r['archived_path'];data=p.read_bytes()
 if r['archived_path'].endswith('.gz')and not r['source_path'].endswith('.gz'):data=gzip.decompress(data)
 assert len(data)==r['original_bytes']and hashlib.sha256(data).hexdigest()==r['original_sha256']
 assert oct(p.stat().st_mode&0o777)==r['source_mode']==r['archived_mode']
receipt={**proof,'payloads':len(manifest.splitlines()),'selected_originals':len(selected),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'all_original_hashes_lengths_modes_roundtrip':True,'archive':str(out)}
save(job/'ARCHIVE_RECEIPT.json',receipt);print(json.dumps(receipt,indent=2))
