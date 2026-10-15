"""Losslessly seal original Mac486 qualification; no remote/build/test work."""
from pathlib import Path
import gzip,hashlib,json,shutil
job=Path(__file__).resolve().parent
raw=job/'collection/raw'
out=Path('validation/performance/native487-capacity-bound/mac')
assert not out.exists()
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
ready=load(raw/'READINESS.json');review=load(job/'QUALIFICATION_REVIEW.json');transfer=load(job/'LOCAL_TRANSFER_PROOF.json')
assert ready['exit']==review['exit']==transfer['exit']==0
assert ready['original_terminal']==load(raw/'ORIGINAL_TERMINAL.json')==load(job/'ORIGINAL_TERMINAL.json')
assert sha(raw/'READINESS.json')==review['readiness_sha256']==transfer['readiness_sha256']
collected=load(raw/'COLLECTED.json')
for name,r in collected['files'].items():
    p=raw/name
    assert sha(p)==r['sha256']and p.stat().st_size==r['bytes']and oct(p.stat().st_mode&0o777)==r['mode'],name
selected={}
for p in raw.rglob('*'):
    if not p.is_file()or '__pycache__'in p.parts:continue
    assert not p.is_symlink()
    if p==raw/'source.sha256':continue
    selected['qualification/'+p.relative_to(raw).as_posix()]=p
for p in job.iterdir():
    if not p.is_file()or p.name in ['source.tar','helpers.tar','COLLECTED.tar.gz','SOURCE.json','source.sha256','TRANSFER.json','ROOT_FINAL_INPUTS.json','REQUIRED_COMMIT','run.sh','verify-source.py','prepare-source.py','collect-artifacts.py','controls.py','collect-evidence.py','ORIGINAL_TERMINAL.json','ARCHIVE_RECEIPT.json','archive.stdout','archive.stderr','archive.exit']:continue
    if p.suffix in ['.json','.stdout','.stderr','.exit']or p.name=='REMOTE_JOB':selected['setup/'+p.name]=p
selected['archive.py']=Path(__file__).resolve()
for name in ['INDEPENDENT_ROOT_REVIEW.json','root-review.py']:
    assert(job/name).is_file(),name
    selected['independent-review/'+name]=job/name
out.mkdir(parents=True)
selection=[];packing=[]
for relative,p in sorted(selected.items()):
    data=p.read_bytes()
    assert not data.startswith((b'\x7fELF',b'!<arch>\n',b'\xcf\xfa\xed\xfe',b'\xfe\xed\xfa\xcf'))
    sensitive=any(line.endswith((b' ',b'\t',b'\r'))for line in data.split(b'\n'))or data.endswith(b'\n\n')
    packed=(len(data)>65536 or sensitive)and p.suffix!='.gz'
    saved=relative+('.gz'if packed else'')
    target=out/saved;target.parent.mkdir(parents=True,exist_ok=True)
    if packed:
        target.write_bytes(gzip.compress(data,compresslevel=9,mtime=0));target.chmod(p.stat().st_mode&0o777)
        assert gzip.decompress(target.read_bytes())==data
        packing.append({'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'container_sha256':sha(target),'container_bytes':target.stat().st_size})
    else:
        shutil.copy2(p,target);assert target.read_bytes()==data
    selection.append({'source_path':str(p),'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'source_mode':oct(p.stat().st_mode&0o777),'archived_mode':oct(target.stat().st_mode&0o777)})
save(out/'SELECTION.json',selection)
save(out/'PACKING.json',packing)
proof={'exit':0,'source_commit':ready['source_commit'],'source_files':ready['inputs'],'source_directories':ready['directories'],'protected_private_fixture_keys_0600':7,'native_artifact_sha256':ready['artifact_sha256'],'native_uuid':ready['native_uuid'],'actual_original_handle':60577,'actual_original_exit':0,'terminal_tool_chunk':'ee680f','readiness_sha256':sha(raw/'READINESS.json'),'original_terminal_sha256':sha(raw/'ORIGINAL_TERMINAL.json'),'original_remote_payloads':len(collected['files']),'selected_remote_original_payloads':len(collected['files'])-1,'selected_remote_files_including_collection_manifest':sum(n.startswith('qualification/')for n in selected),'redundant_remote_file_omitted':'source.sha256; exact hashes/modes already inSOURCE.json','qualification_review_sha256':sha(job/'QUALIFICATION_REVIEW.json'),'independent_root_review_sha256':sha(job/'INDEPENDENT_ROOT_REVIEW.json'),'scope':'Fresh published486 native artifact/functionality qualification only. All four controls explicitly ready64; default names mean default FileLogIo provider, not omitted defaultready16. No throughput/sustained/fullP9/default-promotion claim.'}
save(out/'ARCHIVE_PROOF.json',proof)
(out/'README.md').write_text('''# 487 Mac fresh486 qualification

Published486 source dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69 has1496 exact
source files/327directory modes and seven private fixture keys at0600.
Original60577 actually ends0 (toolee680f). No qualification attempt or control
was replaced or retried. The connection explicitly uses Karl's existing
configured agent; no system/SSH configuration or kernel policy changed.

Actual all/core application23, unit application39, client_admission28,
provider42/27, TCP/TLS+QUIC service2 and matched70 pass. Unchanged broad
library/Node suites are intentionally not rerun: root production diff is one
Counter validator method; existing broader480 evidence is retained separately.
Explicit TAP reports42tests/42pass/0fail/0skip. Formatting, all four strict
production configurations and standalone format/strict pass.

Native root library/executable/buildscript compile fresh:false in a new target,
with opt3/debug0/assertions/overflow and native defaultLTO/codegen. Full CargoJSON,
verbose actual rustc invocations, source/lock/tool identities, original/copy
hashes, release oracle exclusion and new Mach-O UUID/symbol evidence remain.
Compiler/cargo/protoc bytes match Mac480 (stable1.98.0,protoc3.21.12,LLVM22.1.8);
Linux stable1.98.1 is a separate within-host environment.

Four defaultFileLogIo/Witness TCP/QUIC functional histories all explicitly use
ready64,2groups31:1,512offers@128/window4096,64warm and active2smaintenance.
Each applies512, recovers576, verifies retries/worker joins, and has zero
refusals/unknown/pending. Default means omitted publication selector only:
six capacity/zero publication readbacks, no publication.csv, and actual
CURRENT/MANIFEST CRC/length proof. Witness has six capacity/six publication
Create/Recover readbacks and full native storage/publication validation.
These controls do not prove omitted defaultready16 or a performance gain.

The sealed archive preserves original commands/exits/raw stdout/stderr,
source/mode/tool/build/profile proofs, all four original control ledgers and
physical counters. Large or whitespace-sensitive files use deterministic
lossless gzip; PACKING/SELECTION retain original hashes, bytes and modes.
SHA256SUMS covers every payload except itself. Source/buildtrees/binaries/WAL
were excluded from transfer; redundant source.sha256 is omitted because
SOURCE.json retains the same exact inventory. Readiness/terminal originals
are immutable. FullP9 and the pending ext4/XFS environment remain incomplete.
''')
manifest=''.join(sha(p)+'  '+p.relative_to(out).as_posix()+'\n'for p in sorted(out.rglob('*'))if p.is_file())
(out/'SHA256SUMS').write_text(manifest)
for r in selection:
    p=out/r['archived_path'];data=p.read_bytes()
    if r['archived_path'].endswith('.gz')and not r['source_path'].endswith('.gz'):data=gzip.decompress(data)
    assert len(data)==r['original_bytes']and hashlib.sha256(data).hexdigest()==r['original_sha256']
    assert oct(p.stat().st_mode&0o777)==r['source_mode']==r['archived_mode']
receipt={**proof,'payloads':len(manifest.splitlines()),'selected_originals':len(selection),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'all_original_hashes_lengths_modes_roundtrip':True,'archive':str(out)}
save(job/'ARCHIVE_RECEIPT.json',receipt)
print(json.dumps(receipt,indent=2))
