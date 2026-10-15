"""Pack already-terminal476 Linux native setup; no builds, tests or workloads."""
from pathlib import Path
import datetime,gzip,hashlib,json,re,shutil,subprocess

job=Path('target/p9/native476-linux').resolve();out=Path(__file__).resolve().parent
load=lambda p:json.loads(Path(p).read_text());sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
assert {p.name for p in out.iterdir()}=={'archive.py'}, 'refuse archive overwrite'
commit='7e9068296ce00dde5bf3e03bf4d852ed2833d185';ready=load(job/'READY.json');assert ready['source_commit']==commit
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==commit
assert subprocess.check_output(['git','rev-parse','origin/main'],text=True).strip()==commit
guards=[]
for stage in ['qualification','source']:
 command=['python3',str(job/'guard.py'),str(job),stage];r=subprocess.run(command,capture_output=True)
 for stream,b in [('stdout',r.stdout),('stderr',r.stderr)]: (out/f'collection-{stage}.{stream}').write_bytes(b)
 receipt={'command':command,'exit':r.returncode};(out/f'collection-{stage}.exit.json').write_text(json.dumps(receipt,indent=2)+'\n');guards.append(receipt);assert r.returncode==0
assert (job/'source-export.exit').read_text().strip()==(job/'STATUS').read_text().strip()==(job/'SETUP_STATUS').read_text().strip()=='0'
assert load(job/'BUILD_TERMINAL.json')['actual_exit']==0
source=load(job/'SOURCE.json');assert source['inputs']==1492 and source['functional_projection_inputs']==1491 and source['commit']==commit
assert all(sha(Path(source['source_root'])/p)==h for p,h in source['sha256'].items())
for relative,mode in source['physical_modes'].items():assert oct((Path(source['source_root'])/relative).stat().st_mode&0o777)==mode
for relative,mode in source['physical_directory_modes'].items():assert oct((Path(source['source_root'])/relative).stat().st_mode&0o777)==mode
artifacts=load(job/'ARTIFACTS.json')
for a in artifacts:assert a['compiler_record']['fresh']is False and sha(a['original'])==sha(a['copy'])==a['sha256']
symbols=load(job/'SYMBOLS.json');assert symbols['root_buildscript_fresh_record']['fresh']is False
controls=load(job/'controls/TERMINAL.json');assert controls['actual_exit']==0 and controls['source_exact']and controls['cases']==4
assert load(job/'controls/BEFORE.json')==load(job/'controls/AFTER.json')
failed=load(job/'controls-sandbox-denied/TERMINAL.json');assert failed['actual_exit']==1 and failed['cases']==0 and failed['source_exact']
assert (job/'controls-sandbox-denied/tcp-atomic.stderr').read_text()=='Error: "no local TCP/UDP endpoint"\n'
assert load(job/'controls-sandbox-denied/tcp-atomic.exit.json')['exit']==1
rows=load(job/'controls/RESULTS.json');assert len(rows)==4
for r in rows:
 s=r['summary'];assert int(s['applied'])==int(s['admitted'])==512 and int(s['recovered_value'])==576
 assert s['retry_verified']==s['workers_joined']=='true'and all(int(s[k])==0 for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])
 assert len(r['capacity_readbacks'])==len(r['publication_readbacks'])==6 and len(r['physical'])==3
 assert int(s['checkpoints'])==1 and int(s['reclaims'])==3
 for suffix in ['', '-check','-publication-check']:assert load(job/'controls'/(r['protocol']+'-'+r['publication']+suffix+'.exit.json'))['exit']==0
selected={}
def add(path,relative):
 path=Path(path);relative=str(relative);assert path.is_file()and not path.is_symlink();assert relative not in selected;selected[relative]=path
for p in job.iterdir():
 if p.is_file()and p.name!='source.tar':add(p,p.name)
for name in ['logs','control-tools','control-tools-sandbox-denied']:
 for p in(job/name).rglob('*'):
  if p.is_file():add(p,p.relative_to(job))
for name in ['controls','controls-sandbox-denied']:
 for p in(job/name).iterdir():
  if p.is_file():add(p,p.relative_to(job))
 for case in(job/name/'wal').iterdir():
  assert case.is_dir()
  for p in case.iterdir():
   if p.is_file():
    assert p.suffix in ['.csv','.txt','.json'];add(p,Path('case-results')/name/case.name/p.name)
q=load(job/'FUNCTIONAL_QUALIFICATION.json');publication=load(job/'PUBLICATION.json')
for name,r in {'source-inventory':q['source_inventory'],**q['proofs']}.items():
 assert sha(r['path'])==r['sha256'];add(r['path'],Path('qualification-proofs')/(name+'.json'))
for i,pair in enumerate(q['final_source_guards']):
 for name,r in pair.items():assert sha(r['path'])==r['sha256'];add(r['path'],Path('qualification-proofs')/f'guard{i}-{name}.json')
for name,r in [('publication-receipt',publication['proof']),('pre-push-hook',publication['hook'])]:
 assert sha(r['path'])==r['sha256'];add(r['path'],Path('publication-proofs')/name)
publication_dir=Path(publication['proof']['path']).parent
for p in publication_dir.iterdir():
 if p.is_file()and p.name!='RECEIPT.json':add(p,Path('publication-proofs')/p.name)
selection=[];packing=[]
for relative,p in sorted(selected.items()):
 b=p.read_bytes();assert not b.startswith((b'\x7fELF',b'!<arch>\n'));name=relative+('.gz'if len(b)>65536 else'');destination=out/name;assert not destination.exists();destination.parent.mkdir(parents=True,exist_ok=True)
 if name!=relative:
  destination.write_bytes(gzip.compress(b,compresslevel=9,mtime=0));packing.append({'original_path':relative,'archived_path':name,'original_bytes':len(b),'original_sha256':sha(p),'container_bytes':destination.stat().st_size,'container_sha256':sha(destination)})
 else:shutil.copy2(p,destination)
 selection.append({'source_path':str(p),'archived_path':name,'original_sha256':sha(p),'original_bytes':len(b),'source_mode':oct(p.stat().st_mode&0o7777)})
for name,value in [('SELECTION.json',selection),('PACKING.json',packing)]: (out/name).write_text(json.dumps(value,indent=2)+'\n')
note=(job/'logs/elf-notes.log').read_text();build_id=re.search(r'Build ID: ([0-9a-f]+)',note).group(1)
proof={'collection_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source_commit':commit,'current_HEAD_origin_main_equal':True,'source_files':1492,'qualified_inputs':1491,'directory_modes':len(source['physical_directory_modes']),'source_files_directory_modes_exact':True,'original_copy_root_example_SHA_exact':True,'root_library_example_buildscript_fresh_false':True,'actual_source_export_handle':97792,'actual_build_handle':26237,'actual_release_controls_handle':30285,'all_actual_successful_phase_exits':0,'actual_socket_denied_phase_exit':1,'failed_case_completed':0,'successful_release_cases':4,'successful_native_and_publication_validators':8,'physical_replica_records':12,'capacity_readbacks':24,'publication_readbacks':24,'elf_build_id':build_id,'collection_guards':guards,'performance_campaign_started':False,'scope':'Original terminal proof re-read and packed without rebuilding, rerunning tests/controls or generating WAL. Source/locks/compiler/native profile and actual failures retained; no sustained throughput/gain/cross-filesystem acceptance claim.'}
(out/'COLLECTION.json').write_text(json.dumps(proof,indent=2)+'\n')
lines=['#476 fresh Linux native artifact and release controls','','Published source `7e9068296ce00dde5bf3e03bf4d852ed2833d185`; source export97792, fresh build26237 and actual release controls30285 all terminate0. Collection verifies local HEAD/origin-main equal that published commit and all1491 qualified inputs within1492 exported files. File modes and326 observed directory modes match the source freeze. Original/copy root-library and executable hashes match. Cargo JSON/verbose rustc prove root library, example and root build-script fresh:false.','','Linux stable rustc1.98.1 (LLVM22.1.8), x86_64-unknown-linux-gnu, protoc3.9.0; compiler/tool hashes match the475 within-host reference. Native opt3/debug0/assertions+overflow/defaultLTO/defaultcodegen is separate from the framework profile. The release checkpoint debug oracle is absent.','','Executable SHA256: `0bf40f6b4547c446f9db2a86a4bffe6d9ab223640d470154ec266a0d7cc2d064`','', 'Root library SHA256: `a72d441edb01d4b080f51ff12b57558556cf08a7fe92a437543d50ea53130f96`','',f'ELF build ID: `{build_id}`','','The same executable runs both explicit atomic/FileLogIo and witness/WitnessLogIo arms with identical native diagnostics. Each TCP/TLS and QUIC case has one group/three voters/8B Counter/64 warmups/512 offers/4096 window/128 offers per second/ready16/maintenance2s.','','| Transport | Publication | Applied originals | Recovered incl. warmup | Observed max inflight | Checkpoints/reclaims |','| --- | --- | ---: | ---: | ---: | --- |']
for r in rows:
 s=r['summary'];lines.append(f"| {r['protocol']} | {r['publication']} | {s['applied']} | {s['recovered_value']} | {s['max_inflight']} | {s['checkpoints']}/{s['reclaims']} |")
lines+=['','All2048 offered originals apply; all refusal classes, unknown and pending are zero. Every case verifies retries and joins workers. All four native/storage and four publication timing validators pass. Each case records six actual capacity and six publication readbacks across three Create and three Recover assemblies. Twelve post-join physical records check selected generation, selector/metadata CRC and exact WAL boundary; witness arms check both fixed128B records and their matching metadata. These are functional checks, not proof of4096-wide batching or a capacity/gain comparison.','','The initial restricted-sandbox control failed before network setup with `no local TCP/UDP endpoint` (exit1,0 cases completed). Its actual run.txt/offers.csv, stdout/stderr/command/exit, before/after inventories, terminal and original setup failure are retained under controls-sandbox-denied and case-results; no nonexistent summary or successful outcome is invented. Only functional controls were retried with authorized localhost networking, using the same source/artifact. No source export or build repeated. The earlier unpublished-sentinel refusal also remains raw.','','All original setup/guard/build commands, Cargo JSON/vv compiler diagnostics, symbol/build-ID records, source inventories, publication/qualification receipts, both successful and failed control outputs and validators, maintenance/bases/storage/publication/offer tables and source modes are retained. Large originals use lossless gzip with original/container hashes and lengths in PACKING; SELECTION records original paths/bytes/hashes/modes. SHA256SUMS seals the exact recursive payload set. Generated WAL states, executables/libraries, source archive/full source and build trees remain local under target and are excluded.','','No timed performance campaign has started. This does not establish a throughput gain, sustained20M framework floors, Btrfs/APFS/pending real ext4/XFS acceptance, independent physical placement, fair/skewed load, bounded long-run memory/debt or P8 composition performance. FileLogIo remains the default; witness publication stays explicitly selected. Physical file inspection and finite crash controls do not establish real device power-loss behavior.']
(out/'README.md').write_text('\n'.join(lines)+'\n')
files=sorted(p for p in out.rglob('*')if p.is_file());(out/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in files))
manifest={line[66:]:line[:64]for line in(out/'SHA256SUMS').read_text().splitlines()};assert set(manifest)=={str(p.relative_to(out))for p in out.rglob('*')if p.is_file()and p.name!='SHA256SUMS'}
assert all(sha(out/p)==h for p,h in manifest.items())
for r in packing:
 b=gzip.decompress((out/r['archived_path']).read_bytes());assert len(b)==r['original_bytes']and hashlib.sha256(b).hexdigest()==r['original_sha256']
print(json.dumps({'payloads':len(manifest),'gzip':len(packing),'manifest_sha256':sha(out/'SHA256SUMS'),'archive':str(out),'all_exact_SHA_and_lossless_gzip':'PASS'}))
