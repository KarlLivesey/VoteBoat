"""Seal collected Mac480 qualification; never performs remote/build/test work."""
from pathlib import Path
import gzip,hashlib,json,shutil
job=Path(__file__).resolve().parent;raw=job/'collection/raw';out=Path('validation/performance/native480-admission/mac');assert not out.exists()
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
load=lambda p:json.loads(p.read_text())
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
final=load(raw/'FINAL_READINESS.json');old=load(raw/'READINESS.json');append=load(raw/'APPEND_ONLY_RUNTIME_QUALIFICATION.json');review=load(job/'INDEPENDENT_ROOT_REVIEW.json')
assert final['exit']==old['exit']==append['exit']==review['exit']==0
assert sha(raw/'READINESS.json')==final['original_readiness_sha256']and sha(raw/'APPEND_ONLY_RUNTIME_QUALIFICATION.json')==final['append_qualification_sha256']
for name,digest in append['proof_sha256'].items():assert sha(raw/name)==digest,name
assert append['actual_rust_passed_counts']=={'client_runtime_all':[28],'client_runtime_core':[28]}
assert old['original_initial_exit']==101 and old['actual_resume_exit']==0
selected={}
for p in raw.rglob('*'):
 if not p.is_file()or '__pycache__'in p.parts:continue
 assert not p.is_symlink();assert p.name!='source.tar'
 if p==raw/'source.sha256':continue  # exact same inventory is in SOURCE.json
 selected['qualification/'+p.relative_to(raw).as_posix()]=p
selected['INDEPENDENT_ROOT_REVIEW.json']=job/'INDEPENDENT_ROOT_REVIEW.json'
selected['archive.py']=Path(__file__).resolve()
for p in job.iterdir():
 if p.is_file()and p.suffix=='.json'and ('TRANSFER'in p.name or 'COLLECTION'in p.name):selected['collection-proof/'+p.name]=p
out.mkdir(parents=True);selection=[];packing=[]
for relative,p in sorted(selected.items()):
 data=p.read_bytes();assert not data.startswith((b'\x7fELF',b'!<arch>\n',b'\xcf\xfa\xed\xfe',b'\xfe\xed\xfa\xcf'))
 sensitive=any(line.endswith((b' ',b'\t',b'\r'))for line in data.split(b'\n'))or data.endswith(b'\n\n')
 packed=(len(data)>65536 or sensitive)and p.suffix!='.gz';saved=relative+('.gz'if packed else'');target=out/saved;target.parent.mkdir(parents=True,exist_ok=True)
 if packed:
  target.write_bytes(gzip.compress(data,compresslevel=9,mtime=0));target.chmod(p.stat().st_mode&0o777);assert gzip.decompress(target.read_bytes())==data
  packing.append({'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'container_sha256':sha(target),'container_bytes':target.stat().st_size})
 else:shutil.copy2(p,target);assert target.read_bytes()==data
 selection.append({'source_path':str(p),'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'source_mode':oct(p.stat().st_mode&0o777),'archived_mode':oct(target.stat().st_mode&0o777)})
save(out/'SELECTION.json',selection);save(out/'PACKING.json',packing)
proof={'exit':0,'source_commit':final['source_commit'],'source_files':1494,'source_directories':325,'protected_private_fixture_keys_0600':7,'artifacts_sha256':review['artifact_sha256'],'original_initial_handle':59191,'initial_actual_exit':101,'original_resume_handle':27421,'resume_actual_exit':0,'append_only_missing_runtime_handle':36268,'append_only_actual_exit':0,'client_runtime_counts':append['actual_rust_passed_counts'],'original_four_native_controls':4,'final_readiness_sha256':sha(raw/'FINAL_READINESS.json'),'append_receipt_sha256':sha(raw/'APPEND_ONLY_RUNTIME_QUALIFICATION.json'),'scope':'Functional fresh artifact/readiness qualification only, not a measured gain or fullP9 acceptance. Original failed helper and missing-scope correction remain distinct; no performance case rerun or source change.'}
save(out/'ARCHIVE_PROOF.json',proof)
(out/'README.md').write_text('''# 480 Mac fresh native qualification

Published source f7b0ef12709bb7e7aad4f0279851aa035b508e9e is qualified with1494
exact source inputs/325directory modes and seven private fixture keys at0600.
The root library, native executable and buildscript compiled fresh:false under
native opt3/debug0/assertions/overflow/defaultLTO+codegen. CargoJSON, verbose
rustc, Mach-O UUID/symbol-table evidence, original/copy hashes and compiler/tool
identity checks are retained. Within-host476 compiler identities match; Mac
stable1.98.0 remains separate from Linux1.98.1.

Original59191 exits101 because the helper requested a nonexistent test target
before Rust compilation. Its runner, stderr, exit and original inputs remain.
The helper-only resume27421 actually exits0. Original selected outbound
admission8/core1, application17/core17, provider42/core27, library426, actual
TCP/TLS+QUIC service2, matched70, metadata and actual42Node tests pass. Formatting,
four strict production configurations and standalone format/strict checks pass.

That original selection omitted effect_owner/client_admission runtime tests;
outbound admission is a different scope. The separate append-only36268 run
actually exits0 with28all-feature and28core client-admission tests, including the
independent temporary-context histories. Source/artifact guards match before
and after. APPEND_ONLY_RUNTIME_QUALIFICATION and FINAL_READINESS bind the extra
scope without rewriting any old log, READINESS, STATUS, STAGE or original exit.
Only the missing filters ran; no broad unchanged suite, native build or original
control workload was repeated.

Four original explicit atomic/Witness TCP/TLS+QUIC controls select ready64,
groups2/31:1/window4096, apply512, recover576 and verify exact retries and worker
joins with zero refusals/unknown/pending. Each records six capacity and six
provider Create/Recover readbacks; active checkpoints/reclaims and storage
validators pass. These are functional controls, not throughput results.

The archive preserves original commands/exits/stdout/stderr, both helper errors
and corrections, source/mode/transfer inventories, actual CargoJSON/vv/profile
proofs, all four control histories/physical counters and append-only runtime
receipts. Large or whitespace-sensitive text uses deterministic lossless gzip;
PACKING/SELECTION retain original hashes, lengths and modes. SHA256SUMS covers
all payloads except itself. Remote source, binaries/build trees/WAL/snapshot data
and caches were not transferred. The redundant source.sha256 text is excluded
because SOURCE.json already retains those exact file hashes and mode facts.

Independent review checks the transferred original proofs; remote excluded
artifact/source bytes remain bound by original remote guards and exact transfer
receipts. No extra remote work, build, test or workload was run by the reviewer.
Fresh qualification enables later paired481 diagnostics, not a speedup/default
promotion. FullP9 and the user's realext4/XFS acceptance remain incomplete.
''')
manifest=''.join(sha(p)+'  '+p.relative_to(out).as_posix()+'\n'for p in sorted(out.rglob('*'))if p.is_file());(out/'SHA256SUMS').write_text(manifest)
for item in selection:
 p=out/item['archived_path'];data=p.read_bytes()
 if item['archived_path'].endswith('.gz')and not item['source_path'].endswith('.gz'):data=gzip.decompress(data)
 assert len(data)==item['original_bytes']and hashlib.sha256(data).hexdigest()==item['original_sha256']
 assert oct(p.stat().st_mode&0o777)==item['source_mode']==item['archived_mode']
receipt={**proof,'payloads':len(manifest.splitlines()),'selected_originals':len(selection),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'all_original_hashes_lengths_modes_roundtrip':True,'archive':str(out)}
save(job/'ARCHIVE_RECEIPT.json',receipt);print(json.dumps(receipt,indent=2))
