"""Lossless486 fresh Linux native qualification, adapting484 packing structure."""
from pathlib import Path
import gzip,hashlib,json,shutil
job=Path(__file__).resolve().parent;parent=job.parent;repo=job.parents[3]
out=Path('validation/performance/native487-capacity-bound/linux');assert not out.exists()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();load=lambda p:json.loads(p.read_text())
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
ready=load(job/'READY.json');review=load(job/'INDEPENDENT_ROOT_REVIEW.json');source=load(job/'SOURCE.json');originals=load(job/'ORIGINALS.json');terminal=load(job/'controls/TERMINAL.json');canonical=load(parent/'ROOT_FINAL_INPUTS.json')
assert ready['source_commit']==source['commit']==review['published_commit']==canonical['published_commit']=='dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69' and review['actual_exit']==0
assert ready['actual_export_exit']==ready['actual_build_exit']==ready['actual_network_controls_exit']==0
assert ready['actual_native_processes_pass']==ready['actual_native_storage_validators_pass']==3
assert ready['actual_instrumented_witness_publication_validators_pass']==2 and ready['default_legacy_filelogio_physical_verified']
assert terminal['actual_exit']==0 and terminal['source_exact']and terminal['cases']==3 and terminal['all_owned_children_waited']
assert source['inputs']==len(source['sha256']) and source['functional_projection_inputs']==len(canonical['files'])==1496
assert source['added_artifact_configuration_inputs']==['validation/performance/native486-capacity-bound/SCHEMA.md']
assert all(v['actual_exit']==0 for v in originals['originals'])
for name,digest in ready['proof_sha256'].items():assert sha(job/name)==digest,name
selection={}
for p in job.iterdir():
 if p.is_file()and p.name not in ['source.tar','ARCHIVE_RECEIPT.json','archive.stdout','archive.stderr','archive.exit']:
  selection['qualification/'+p.name]=p
for folder in ['logs','control-tools','controls','prepared-unpublished']:
 for p in(job/folder).rglob('*'):
  if not p.is_file()or '__pycache__'in p.parts:continue
  rel=p.relative_to(job)
  if folder=='controls'and'wal'in rel.parts:
   if p.parent.parent!=job/'controls/wal':continue
   assert p.suffix in ['.csv','.txt','.json'],p
  selection['qualification/'+rel.as_posix()]=p
def reference(item):
 p=Path(item['path']);assert sha(p)==item['sha256'],p
 relative=p.relative_to(repo);name='root-qualification/'+relative.as_posix()
 assert name not in selection or selection[name]==p;selection[name]=p
q=load(job/'FUNCTIONAL_QUALIFICATION.json');reference(q['source_inventory'])
for item in q['proofs'].values():reference(item)
for item in q['actual_exit_files'].values():reference(item)
for pair in q['final_source_guards']:
 for item in pair.values():reference(item)
for item in q['publication_source_guards']:reference(item)
pub=load(job/'PUBLICATION.json');reference(pub['proof']);reference(pub['hook'])
for item in pub['actual_exit_files'].values():reference(item)
for p in(parent/'publication').iterdir():
 if p.is_file():selection['root-publication/'+p.name]=p
for name in ['ROOT_FINAL_INPUTS.json','CHECKED.json','ORIGINALS.json','ROOT_ARCHIVE_VERIFIED.json','run.py','publish.py']:
 selection['root-qualification/'+name]=parent/name
out.mkdir(parents=True);selected=[];packing=[]
for relative,p in sorted(selection.items()):
 assert not p.is_symlink();data=p.read_bytes();assert not data.startswith((b'\x7fELF',b'!<arch>\n',b'\xcf\xfa\xed\xfe',b'\xfe\xed\xfa\xcf'))
 whitespace=any(line.endswith((b' ',b'\t',b'\r'))for line in data.split(b'\n'))or data.endswith(b'\n\n')
 packed=(len(data)>65536 or whitespace or p.suffix in ['.stdout','.stderr','.log','.txt','.patch'])and p.suffix!='.gz'
 saved=relative+('.gz'if packed else'');target=out/saved;target.parent.mkdir(parents=True,exist_ok=True)
 if packed:
  target.write_bytes(gzip.compress(data,compresslevel=9,mtime=0));target.chmod(p.stat().st_mode&0o777);assert gzip.decompress(target.read_bytes())==data
  packing.append({'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'container_sha256':sha(target),'container_bytes':target.stat().st_size,'reason':'large or Git-whitespace-sensitive original text'})
 else:shutil.copy2(p,target);assert target.read_bytes()==data
 selected.append({'source_path':str(p),'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'source_mode':oct(p.stat().st_mode&0o777),'archived_mode':oct(target.stat().st_mode&0o777)})
save(out/'SELECTION.json',selected);save(out/'PACKING.json',packing)
proof={'exit':0,'source_commit':ready['source_commit'],'source_files':source['inputs'],'qualified_projection_files':source['functional_projection_inputs'],'extra_committed_source':source['added_artifact_configuration_inputs'],'ready_sha256':sha(job/'READY.json'),'native_artifact_sha256':ready['artifact_sha256'],'root_artifact_sha256':ready['root_artifact_sha256'],'native_build_id':'b208bec6896bea85c3be1d2ba9891a1a64ea9a35','original_handles':{v['phase']:v['original_handle']for v in originals['originals']},'actual_export_exit':0,'actual_build_exit':0,'actual_controls_exit':0,'native_processes_and_storage_validators':3,'witness_publication_validators':2,'default_legacy_filelogio_verified':True,'independent_root_review_exit':review['actual_exit'],'independent_root_review_sha256':sha(job/'INDEPENDENT_ROOT_REVIEW.json'),'canonical_source_projection_sha256':sha(parent/'ROOT_FINAL_INPUTS.json'),'scope':'Fresh486 Linux native artifact/control qualification only. Expected UNSET guard refusal preserved; root independent reviewer initial overflow-flag and default-provider diagnostic assumptions corrected without rerunning workloads. No speedup/capacity/fullP9 claim.','excluded':'Binaries/root rlib/buildtree/full source/source.tar/generated WAL/snapshot states and caches. Exact source/gitblob/hash/mode/archive inventories and all actual release-control raw ledgers/commands/exits retained.'}
save(out/'ARCHIVE_PROOF.json',proof)
(out/'README.md').write_text("""# 487 Linux fresh486 native artifact qualification

Published486 source dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69 was exported once
(original12673, actual0/tool09ff25), built fresh once (87183, actual0/6428b0), then
functionally controlled once (83300, actual0/24bc6c). ORIGINALS retains actual
commands and terminal chunks. The export contains1497 files/modes:1496 checked
inputs plus the committed486 schema. All seven private-key fixtures retain0600.

Actual CargoJSON and verbose rustc prove root library, executable and buildscript
compiled fresh:false. Native opt3/debug0/assertions/overflow and defaultLTO/codegen
are preserved; inherited flags/wrappers are unset. Linux stable1.98.1, LLVM22.1.8
and protoc3.9.0 tool bytes match immutable480. Executable SHA is
f3c6e797c3ea1e4af255210b8cbb63fa23505f0d0af22735dba286a50bffd8e7;
root rlib SHA ea3678c2cbdf0d5f715bb60b2817b9edbba0c0aa66f397f1ae022878fb286cd2.
ELF buildID b208bec6896bea85c3be1d2ba9891a1a64ea9a35 and debug0 symbol evidence
are preserved. Baseline480 executable8d3530a6… remains unchanged.

Three actual functional controls each apply512 originals, recover576, verify
original outcomes/retries, join workers, complete one checkpoint/three reclaims
and report zero refusals/unknown/pending. Explicit Witness TCP/TLS and QUIC use
ready64/two groups/31:1,512@128/s/window4096, with six capacity and six publication
Create/Recover readbacks each. Both publication validators and all three full
native/storage validators pass. The omitted-publication/omitted-ready default
TCP control uses Legacy FileLogIo/ready16/one group, six capacity readbacks and no
publication fields/readbacks/CSV. Its CURRENT20B and MANIFEST52B CRC/acknowledged
selected-WAL length are verified. No nonexistent opt-in diagnostics are required.
These are functional controls, not throughput/capacity acceptance measurements.

Independent root raw review passed without repeating builds/workloads/checkers.
It verifies1111 CargoJSON records, all1497 source inputs, canonical1496 checked
inputs,22 READY-bound proofs and all actual controls. Two initial review-only
parser assumptions are preserved with their old scripts and failure receipts:
overflow checks need not appear as a separate rustc switch when debug assertions
already enable them, while actual CargoJSON records overflow_checks=true; default
uninstrumented FileLogIo has no publication_provider summary field. Correcting
these review expectations did not alter artifacts/data or rerun workloads.

PREPARATION and prepared-unpublished preserve the original UNSET guard refusal
and exact prior runner files; ACTIVATION binds actual published486/qualified
receipts. Qualification reads the real486 CHECKED/14 COMMANDS/raw INPUTS maps and
publication guard digest, without inventing older483 receipt fields. The canonical
ROOT_FINAL_INPUTS, raw source guards, original root qualification75531 and actual
enabled-hook publication97050 receipts are included. READY and its bound proofs
remain unchanged after completion.

Full CargoJSON/vv, source/archive/hash/gitblob/mode inventories, original/copied
artifact hashes, tool/profile evidence, commands/exits/raw stdout/stderr, physical
proofs and all outcome/storage/publication/maintenance CSVs are retained. Text,
patches, large and whitespace-sensitive files use reversible gzip. SELECTION
records exact original hashes/lengths/modes; PACKING records containers and SHA256SUMS
seals all payloads except itself. Binaries, full production source, source.tar,
build trees, generated WAL/snapshot states and caches are excluded. No paired
campaign has started; fullP9 and real ext4/XFS acceptance remain open.
""".replace('original12673','original 12673').replace('actual0','actual 0').replace('tool09','tool 09').replace('contains1497','contains 1497').replace('modes:1496','modes: 1496').replace('retain0600','retain 0600').replace('stable1','stable 1').replace('LLVM22','LLVM 22').replace('protoc3','protoc 3').replace('executable8d','executable 8d').replace('apply512','apply 512').replace('recover576','recover 576').replace('verifies1111','verifies 1111').replace('all1497','all 1497').replace('canonical1496','canonical 1496').replace('inputs,22','inputs, 22').replace('qualification75531','qualification 75531').replace('publication97050','publication 97050'))
manifest=''.join(sha(p)+'  '+p.relative_to(out).as_posix()+'\n'for p in sorted(out.rglob('*'))if p.is_file());(out/'SHA256SUMS').write_text(manifest)
for row in selected:
 p=out/row['archived_path'];data=p.read_bytes()
 if row['archived_path'].endswith('.gz')and not row['source_path'].endswith('.gz'):data=gzip.decompress(data)
 assert len(data)==row['original_bytes']and hashlib.sha256(data).hexdigest()==row['original_sha256']
 assert oct(p.stat().st_mode&0o777)==row['source_mode']==row['archived_mode']
receipt={**proof,'payloads':len(manifest.splitlines()),'selected_originals':len(selected),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'all_original_hashes_lengths_modes_roundtrip':True,'archive':str(out)};save(job/'ARCHIVE_RECEIPT.json',receipt);print(json.dumps(receipt,indent=2))
