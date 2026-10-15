from pathlib import Path
import json,gzip,hashlib,shutil
job=Path(__file__).resolve().parent;out=Path('validation/performance/native475-capacity/main');assert not out.exists();out.mkdir();sha=lambda b:hashlib.sha256(b).hexdigest();selected={}
def add(p,rel):
 p=Path(p);assert p.is_file()and not p.is_symlink();selected[str(rel)]=p
for p in job.iterdir():
 if p.is_file():add(p,p.name)
for folder in ['before','new-source','root','root2','network','network2','strict','strict2']:
 for p in(job/folder).rglob('*'):
  if not p.is_file()or 'tmp'in p.relative_to(job/folder).parts:continue
  rel=p.relative_to(job/folder)
  if rel.parts[0]=='wal':
   if len(rel.parts)!=3 or p.suffix not in ['.csv','.txt','.json']:continue
  add(p,p.relative_to(job))
packing=[]
for rel,p in sorted(selected.items()):
 b=p.read_bytes();assert not b.startswith((b'\x7fELF',b'!<arch>\n'));target=out/(rel+('.gz'if len(b)>65536 else''));target.parent.mkdir(parents=True,exist_ok=True)
 if len(b)>65536:
  target.write_bytes(gzip.compress(b,mtime=0));packing.append({'original_path':rel,'archived_path':str(target.relative_to(out)),'original_sha256':sha(b),'original_bytes':len(b),'container_sha256':sha(target.read_bytes()),'container_bytes':target.stat().st_size})
 else:shutil.copy2(p,target)
(out/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n')
a=json.loads((job/'root/BEFORE.json').read_text())['files'];b=json.loads((job/'root2/BEFORE.json').read_text())['files'];assert b==json.loads((job/'ROOT_FINAL_INPUTS.json').read_text())['files']
for name in ['validation/check-offered.mjs','validation/check-offered.test.mjs']:assert a[name]==b[name]
for name,row in b.items():assert sha(Path(name).read_bytes())==row['sha256']and oct(Path(name).stat().st_mode&0o7777)==row['mode']
for folder in ['root2','network2','strict2']:
 r=json.loads((job/folder/'TERMINAL.json').read_text());assert r['exit']==0 and r['source_exact']is True
(out/'REUSED_JS_INPUTS.json').write_text(json.dumps({'old_before':'root/BEFORE.json','final_before':'root2/BEFORE.json','files':{n:b[n]for n in ['validation/check-offered.mjs','validation/check-offered.test.mjs']},'actual11tests_pass':'root/offered-js.exit.json','scope':'Exact independent JS input hashes unchanged by subsequent Rust-only lint extractions; reuse11 actual controls, not Node file-level wrapper count.'},indent=2)+'\n')
(out/'README.md').write_text('''#475 explicit offered capacity profile: checked Linux fixture

No production/API/provider/default/format change. Private --offered-capacity4096
permits wider offered windows with commonclient4096/group4096 and shard4112items
including original16protected controlslots. Global/byte/control/background/ready/
Core/worker limits stay unchanged. Actual limits checked before worker spawn in
all shared Create/Recover paths; legacy/startup/lanes remain unchanged. run.txt,
stderr and summary identify the profile, including failed-run setup metadata.
Independent validator accepts >64 only with complete exact disclosed profile.

Finalroot2 original17312 exits0: nativeexample51 pass230.32s and debugbuild0,
1469guarded source/mode inputs exact. Finalnetwork2 original73521 exits0:
11 invalid CLI cases reject before root creation; actual TCP/QUIC w4096 offered
controls pass original512offer histories, physicalstorage/maintenance checks,
recovery64+applied/retries/joins,3Create+3Recover readbacks perprotocol. Original
six64/256/4096 controls from82546 allpass before pure lint extractions; finalwide
controls plus finalfull51 cover the extracted assembly/validation paths. Small
debug cases are functional only; achievedoccupancy is not4096-wide batching.

Finalstrict2 original37703 exits0 through enabled formatter/default/all/core/
native/standalone strict checks, zero diagnostics, unchanged1469source/modes.
JS11 controls reuse exact final module/test SHA from originalroot0. Earlier
node --test reported file-level1 only; raw direct11actual assertions remain.
Meaningful Rust controls admit4096 originals into actualTimedShard, refuseextra,
preserve16protected controls versus legacy240+16, reject limits/ready/group drift,
and check parser/mode/count bounds. No concurrency/consensus execution claim is
inferred from the held queue test.

Keep all attempts: initialfocused86100 passes3with obsoletewrapperwarning;
wrapperremoved,83038 then readbackfixdb2203 pass3/48filtered warningfree. Initial
full51 passes225.52s before lint failure49154/101 (main102/shared109lines).
Two private named helper extractions and countcontrols fix causes, no thresholds
changed. Finalfull51 repeated after these actualsource changes; broad unchanged
production/P8/other integration scopes are reused. Original functions/patches/
logs/commands/exits/modes and both network control raws stay. Native baseline
threeowner timeout limitations remain; no new power-loss or fullP9 claim.

Archive excludes binaries/fullsource/buildtrees/syntheticWAL duplicates; actual
WAL states remain under target. PACKING losslessly preserves originals;
SHA256SUMS covers exact recursive payloads. Checkedpublication precedes fresh
asserted native-only artifacts on Linux/Mac. Native releaseopt3/debug0/assertions/
overflow/defaultLTO+CGU stays distinct from frameworkdebug2/thin profile. Btrfs
and APFS proceed; user's realext4/XFSscratch is pending. Each future candidate
must gain against samevolume/device baseline on allthree environments; no raw
cross-drive CoW attribution. All fullP9 framework floors/sustainedmemorydebt/
durableheadroom/fairness/placement/P8 obligations remain open.
''')
files=sorted(p for p in out.rglob('*')if p.is_file());(out/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(out))+'\n'for p in files));rows={l[66:]:l[:64]for l in(out/'SHA256SUMS').read_text().splitlines()};assert set(rows)=={str(p.relative_to(out))for p in out.rglob('*')if p.is_file()and p!=out/'SHA256SUMS'}
for name,h in rows.items():assert sha((out/name).read_bytes())==h
for r in packing:
 raw=gzip.decompress((out/r['archived_path']).read_bytes());assert sha(raw)==r['original_sha256']and len(raw)==r['original_bytes']
proof={'payloads':len(rows),'gzip':len(packing),'manifest_sha256':sha((out/'SHA256SUMS').read_bytes()),'exact_set_payload_hashes_lossless_original_lengths':'PASS','guarded_final_inputs':len(b),'native_example51_debugbuild':'PASS','finalTCP_QUIC_wide_recovery_readbacks':'PASS','invalidCLI_before_root':11,'full_strict_hook':'PASS','JS11_exact_input_reuse':'PASS','native_performance_claim':False};Path('validation/performance/native475-capacity/ROOT_ARCHIVE_REVIEW.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))
