"""Seal existing480 qualification only; no build/test/benchmark or source copy."""
from pathlib import Path
import gzip,hashlib,json,shutil

source=Path('target/p9/native480-admission').resolve()
archive=Path('validation/performance/native480-admission/main')
assert not archive.exists(), 'Never overwrite qualification evidence'
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
def save(path,value):path.write_text(json.dumps(value,indent=2)+'\n')

checked=load(source/'final/CHECKED.json')
assert checked['exit']==checked['actual_terminal_exit']==0 and checked['source_exact'] and checked['inputs']==1494
assert checked['original_final_handle']==31691
assert load(source/'checked4/TERMINAL.json')=={'exit':0,'same_selected_inputs':True,'selected_inputs':1494,'all_selected_commands_completed':True}
assert sha(source/'checked4/BEFORE.json')==sha(source/'checked4/AFTER.json')
assert load(source/'final/INPUTS.json')['files']==load(source/'checked4/BEFORE.json')['files']
assert len(load(source/'final/INPUTS.json')['files'])==1494
def references(value):
 if isinstance(value,dict):
  if set(value)>={'path','sha256'}:
   path=Path(value['path']);assert path.is_file()and sha(path)==value['sha256'],path
  for item in value.values():references(item)
 elif isinstance(value,list):
  for item in value:references(item)
for name in ['CHECKED','functional','strict','metadata']:
 value=load(source/'final'/(name+'.json'));assert value['exit']==0 and value['source_exact'];references(value)
functional=load(source/'final/functional.json')
for entry in functional['actual_commands'].values():assert load(entry['path'])['exit']==0
assert load(load(source/'final/strict.json')['actual_receipt']['path'])['exit']==0
for entry in load(source/'final/metadata.json')['actual_receipts'].values():assert load(entry['path'])['exit']==0
assert load(source/'LEGACY_ORACLE_PROOF.json')['legacy_counter_validate_proposal_body_unchanged']
assert load(source/'metadata-tap/EXIT.json')['exit']==0
tap=(source/'metadata-tap/stdout').read_text()
assert '# tests 42' in tap and '# pass 42' in tap and '# fail 0' in tap

selected={}
for directory in ['checks','checked','checked2','checked3','checked4','final','metadata-tap']:
 for path in (source/directory).rglob('*'):
  if not path.is_file():continue
  assert not path.is_symlink()and '__pycache__'not in path.parts
  selected['qualification/'+path.relative_to(source).as_posix()]=path
selected['qualification/LEGACY_ORACLE_PROOF.json']=source/'LEGACY_ORACLE_PROOF.json'
selected['archive-main.py']=Path(__file__).resolve()
for name in ['archive-main-attempt1.py','archive-main.stdout','archive-main.stderr']:
 selected['archiver-initial-attempt/'+name]=source/name
archive.mkdir(parents=True)
selection=[];packing=[]
for relative,path in sorted(selected.items()):
 data=path.read_bytes();assert not data.startswith((b'\x7fELF',b'!<arch>\n',b'\xcf\xfa\xed\xfe',b'\xfe\xed\xfa\xcf'))
 assert path.suffix in ['.py','.rs','.json','.stdout','.stderr']or path.name in ['stdout','stderr'],path
 packed=len(data)>65536
 saved=relative+('.gz'if packed else'');target=archive/saved;target.parent.mkdir(parents=True,exist_ok=True)
 if packed:
  target.write_bytes(gzip.compress(data,compresslevel=9,mtime=0));target.chmod(path.stat().st_mode&0o777)
  assert gzip.decompress(target.read_bytes())==data
  packing.append({'original_path':relative,'archived_path':saved,'original_sha256':sha(path),'original_bytes':len(data),'container_sha256':sha(target),'container_bytes':target.stat().st_size})
 else:shutil.copy2(path,target);assert target.read_bytes()==data
 selection.append({'source_path':str(path),'original_path':relative,'archived_path':saved,'original_sha256':sha(path),'original_bytes':len(data),'source_mode':oct(path.stat().st_mode&0o777),'archived_mode':oct(target.stat().st_mode&0o777)})
save(archive/'PACKING.json',packing);save(archive/'SELECTION.json',selection)
review={'exit':0,'reviewer':'mac475_collect','final_original_handle':31691,'actual_terminal_exit':0,'inputs':1494,
 'reviewed':'Public optional default-None callback lifetime/owned input, exact legacy oracle, temporary Counter identity reservation/error order, lazy router prefix reset/error cleanup/drop-before-application, wrapper receipt bound, independent downstream runtime tests and partial contract metadata.',
 'checks':'Final normalized proof references and all final command exits rechecked; actual tests/formatter/strict hook were executed by root, not rerun by reviewer.',
 'scope':'Selected finite tests and source review, not a complete provider/Raft proof, fresh performance artifact, measured speedup or full P9 acceptance.',
 'source_reuse':'checked2 successful unchanged application/provider/library/service/matched scopes retained; checked4 reruns affected client tests after a test-only named type alias plus metadata and complete enabled hook. Exact reuse/input records and every failed attempt retained.',
 'metadata_reporting':'44 contracts/415 operations in positive metadata receipt. Original final negative-command one-file summary is retained. The separate explicit canonical-Node TAP diagnostic executes42tests/42pass/0fail with identity/environment and original command retained.',
 'proofs':{name:{'sha256':sha(source/'final'/(name+'.json')),'archive_path':'qualification/final/'+name+'.json'}for name in ['CHECKED','functional','strict','metadata']}}
save(archive/'REVIEW.json',review)
readme='''# 480 local qualification: exact temporary ready admission

Original final runner31691 actually terminates0. All1494 selected input hashes
and modes match before/after. This archive qualifies the checked implementation;
it contains no fresh candidate performance artifact or measured speedup.

The optional contract1 capability borrows immutable application state and owns
temporary reservation IDs. Counter prepares once for an unchanged ready prefix;
the first candidate and providers returningNone retain per-command validation.
Every original keeps its context/receipt checks, position and exact outcome.
State resets before another prefix and after errors, and drops before application.
Counter uses one box; the service receipt wrapper adds one constant forwarding box.

Successful existing command receipts cover all/core application17, all/core
client-admission28, all/core provider conformance, library426, actual TCP/TLS and
QUIC service histories2, and matched70. Counter differential controls compare
148750 bounded histories to the unchanged full legacy validator. Independent
downstream contexts test singleton/fallback, lazy preparation/all pending work,
group separation, capacity/receipt partial refusal and cleanup. Metadata records
partial44contracts/415operations; the negative command exits0 and preserves its
original one-file reporter summary. A separate explicit canonical-Node TAP
diagnostic establishes42tests/42pass/0fail, with command, identity and environment.
The enabled pre-push hook's formatter, four strict production profiles and
standalone formatting/strict checks all exit0. No threshold or hook was bypassed.

The qualification chain preserves rather than replaces its failures:

- `qualification/checks/`: first E0373 borrowed-group iterator compile error,
  exact failed ready.rs source and failed-source inventory; fixed with owned
  move capture of the copied group identity.
- `qualification/checked/`: pre-launch Python else1 syntax error provenance and
  original runner source (zero commands executed), then module-loader E0583.
  The loader received an explicit application/ready_admission.rs path.
- `qualification/checked2/`: successful unchanged Rust/service/matched scopes,
  followed by stale metadata expectations43/411. Exact count update and three
  ready-context omission controls are reflected in subsequent metadata checks.
- `qualification/checked3/`: metadata passes, then strict Clippy type_complexity
  rejects the independent test's nested pending-observation type. A named alias
  fixes its representation without changing behavior, thresholds or lint levels.
- `qualification/checked4/`: affected client tests, metadata and complete enabled
  hook pass. SOURCE_REUSE and final proofs explain precisely which checked2
  scopes remain valid; broad unchanged suites were not repeated.

Every command, actual exit, stdout/stderr, runner, failure, source inventory,
reuse record and final normalized proof is retained losslessly. Large files use
deterministic gzip; PACKING and SELECTION retain original hashes, lengths and
modes. SHA256SUMS covers every payload except itself. No executable, build tree,
full source tree, dependency cache or generated WAL is included. The tiny failed
ready.rs is intentional evidence, not a complete source snapshot.
The archive helper's initial whole-object comparison error is also preserved;
final INPUTS has extra scope/qualification fields, while its1494 file records
match BEFORE exactly. Only this guard was focused to the actual files projection.

Available-host measurements, fresh published candidate artifacts and Mac
qualification follow publication. Default FileLogIo and ready16 stay unchanged.
Real ext4/XFS native acceptance, full sustained framework margins, physical
memory/debt, fairness/headroom/placement and P8 composition remain open.
'''
(archive/'README.md').write_text(readme)
manifest=''.join(sha(path)+'  '+path.relative_to(archive).as_posix()+'\n'for path in sorted(archive.rglob('*'))if path.is_file())
(archive/'SHA256SUMS').write_text(manifest)
for item in selection:
 path=archive/item['archived_path'];data=path.read_bytes()
 if item['archived_path'].endswith('.gz'):data=gzip.decompress(data)
 assert len(data)==item['original_bytes']and hashlib.sha256(data).hexdigest()==item['original_sha256']
 assert oct(path.stat().st_mode&0o777)==item['archived_mode']==item['source_mode']
receipt={'exit':0,'archive':str(archive),'payloads':len(manifest.splitlines()),'selected_originals':len(selection),'gzip':len(packing),'manifest_sha256':sha(archive/'SHA256SUMS'),'bytes':sum(path.stat().st_size for path in archive.rglob('*')if path.is_file()),'all_original_hashes_lengths_modes_roundtrip':True,'final_checked_sha256':sha(source/'final/CHECKED.json'),'no_tests_or_workloads_launched':True}
save(source/'ARCHIVE_MAIN_RECEIPT.json',receipt)
print(json.dumps(receipt,indent=2))
