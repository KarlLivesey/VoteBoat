#!/usr/bin/env python3
"""Collect finite completed472 qualification only after explicit root release.
--terminal-scope JSON must assert all_recorded_jobs_terminal=true and record
original_runs (handles/actual exits). This script never tests/builds workloads.
"""
from pathlib import Path
import argparse,gzip,hashlib,json,re,shutil,stat,subprocess
ROOT=Path(__file__).resolve().parents[3]
JOB=ROOT/'target/p9/slice472'
OUT=ROOT/'validation/performance/pipeline472/main'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def load(p):return json.loads(Path(p).read_text())
def files(value):
 value=value.get('files',value)
 return {str(Path(p).relative_to(ROOT)) if Path(p).is_absolute() else p:r for p,r in value.items() if isinstance(r,dict) and 'sha256' in r}
def main():
 ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--terminal-scope',type=Path,required=True);receipt=ap.parse_args().terminal_scope.resolve();scope=load(receipt)
 assert scope['all_recorded_jobs_terminal'] is True and scope['original_runs']
 terminal={'attempt1':('EXIT.json',101),'attempt2':('EXIT.json',101),'attempt3':('TERMINAL.json',0),'validation4':('TERMINAL.json',0),'selected5':('TERMINAL.json',0),'profiles6':('TERMINAL.json',0),'strict7':('TERMINAL.json',0)}
 for name,(record,code) in terminal.items():assert load(JOB/name/record)['exit']==code,(name,record)
 assert not OUT.exists(),'No overwrite of partial/sealed archive'
 final={}
 for name in ['validation4','profiles6','strict7']:
  final.update(files(load(JOB/name/'AFTER.json')))
 for name,row in final.items():
  p=ROOT/name;assert p.is_file() and sha(p.read_bytes())==row['sha256'],('Final source differs',name)
  mode=row.get('mode',row.get('physical_mode_at_capture'));assert oct(stat.S_IMODE(p.stat().st_mode))==mode,(name,mode)
 selected={}
 def add(p,relative):
  p=Path(p);assert p.is_file() and not p.is_symlink(),p
  relative=str(relative);assert not Path(relative).is_absolute() and '..' not in Path(relative).parts
  assert relative not in selected or sha(selected[relative].read_bytes())==sha(p.read_bytes())
  selected[relative]=p
 for p in JOB.iterdir():
  if p.is_file():add(p,p.name)
 for directory in terminal:
  for p in (JOB/directory).rglob('*'):
   if not p.is_file():continue
   rel=p.relative_to(JOB/directory)
   if directory=='selected5' and len(rel.parts)>1:
    if len(rel.parts)!=2 or rel.parts[0] not in ['tcp-startup','tcp-shared','quic-startup','quic-shared']:continue
    if p.suffix not in ['.csv','.txt','.json']:continue
   if any(part in {'target','test-tmp','__pycache__'} for part in rel.parts):continue
   add(p,Path(directory)/rel)
 for dirname in ['historical-source','test-source-before']:
  for p in (JOB/dirname).rglob('*'):
   if p.is_file():add(p,Path(dirname)/p.relative_to(JOB/dirname))
 # Source versions include changed/new qualified files, not a whole source tree.
 base=load(JOB/'BEFORE.json')['commit']
 command=['git','diff','--binary',base,'--','src','tests','examples','benchmarks/matched']
 result=subprocess.run(command,cwd=ROOT,capture_output=True,check=True)
 (JOB/'CURRENT_SOURCE.patch').write_bytes(result.stdout)
 (JOB/'CURRENT_SOURCE_PATCH_COMMAND.json').write_text(json.dumps({'command':command,'exit':result.returncode,'base':base},indent=2)+'\n')
 for p in [JOB/'CURRENT_SOURCE.patch',JOB/'CURRENT_SOURCE_PATCH_COMMAND.json']:add(p,p.name)
 changed=subprocess.run(['git','diff','--name-only',base,'--','src','tests','examples','benchmarks/matched'],cwd=ROOT,capture_output=True,text=True,check=True).stdout.splitlines()
 initial=files(load(JOB/'BEFORE.json'));changed=set(changed)|{name for name in final if name not in initial and name.startswith(('src/','tests/','examples/','benchmarks/matched/'))}
 for name in sorted(changed):
  assert name in final,('Changed source outside final recorded scope',name)
  add(ROOT/name,Path('source-final')/name)
  old=subprocess.run(['git','show',base+':'+name],cwd=ROOT,capture_output=True)
  if old.returncode==0:
   oldpath=JOB/'base-source'/name;oldpath.parent.mkdir(parents=True,exist_ok=True);oldpath.write_bytes(old.stdout);add(oldpath,Path('source-base')/name)
 schema=ROOT/'validation/performance/pipeline472/SCHEMA.md';add(schema,'SCHEMA.md');add(receipt,'TERMINAL_SCOPE.json')
 # Freeze the current committed validator closure used by selected5 commands.
 queue=[ROOT/'validation/check-native-benchmark.mjs',ROOT/'validation/check-journal-timings.mjs'];visited=set()
 while queue:
  p=queue.pop()
  if p in visited:continue
  visited.add(p);add(p,Path('selected-validator-tools')/p.name)
  queue.extend((p.parent/ref).resolve() for ref in re.findall(r"(?:from\s*|import\s*)['\"](\./[^'\"]+)['\"]",p.read_text()))
 OUT.mkdir(parents=True);packing=[];selection=[]
 for name,p in sorted(selected.items()):
  raw=p.read_bytes();assert not raw.startswith((b'\x7fELF',b'!<arch>\n')),p
  archived=name+'.gz' if len(raw)>65536 else name;target=OUT/archived;target.parent.mkdir(parents=True,exist_ok=True)
  if archived!=name:
   target.write_bytes(gzip.compress(raw,compresslevel=9,mtime=0));target.chmod(stat.S_IMODE(p.stat().st_mode));packing.append({'original_path':name,'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'container_bytes':target.stat().st_size,'container_sha256':sha(target.read_bytes())})
  else:shutil.copy2(p,target)
  selection.append({'source_path':str(p),'archived_path':archived,'original_sha256':sha(raw),'original_bytes':len(raw),'source_mode':oct(stat.S_IMODE(p.stat().st_mode))})
 pattern=re.compile(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out')
 results={name:[dict(zip(['result','passed','failed','ignored','measured','filtered'],row)) for row in pattern.findall(p.read_text(errors='replace'))] for name,p in selected.items() if p.name.endswith(('.stdout','.stderr','.log')) or p.name in ['stdout','stderr']}
 results={name:rows for name,rows in results.items() if rows}
 for name,value in [('PACKING.json',packing),('SELECTION.json',selection),('ACTUAL_TEST_COUNTS.json',results),('FINAL_SOURCE_SCOPE.json',{'base':base,'current_input_count':len(final),'files':final,'changed_new':sorted(changed),'scope':'Union of captured final validation/core/strict inputs; actual per-attempt before/after inventories remain authoritative.'})]: (OUT/name).write_text(json.dumps(value,indent=2)+'\n')
 (OUT/'README.md').write_text('''#472 bounded driver pump qualification

The root terminal receipt records actual original handles and command exits.
Attempts1/2 remain compile failures101 (wrong private budget argument, then
worker fixture batch_cost namespace/String). Attempt3 tests8actual-driver and
2hidden budgets passed. Historical initial pump/test bytes were reconstructed
ONLY by exact known inverse edits and their original frozen SHA was verified;
raw compiler failures and all source scopes remain preserved.

Validation4 passed rootfmt/all-feature strict, library446, driver15, application12,
provider46/runtime55/worker11, actual TCP/QUIC Counter services2 and debug native
example build. Profiles6 passed corelib363/owner203/provider31/runtime38/worker3,
matched70 and metadata0. Strict7 is the actual enabled pre-push hook scope; see
raw command/environment/stdout/stderr/status, not an inferred clean subset.
Counts remain per recorded scope: no full all-feature owner suite claim.

Selected5 uses the checked current debug ELF only for four startup/shared
TCP/QUIC functional histories128 originals+64warmups,192 recovered, retries and
joins, actual create/recover depth4 selection. All14commands passed. Each shared
leader measured one physical same-group multi-unit synchronized call containing
48known suffix originals; missing/drop/errors0. This proves activation, not
release throughput, useful amortization or P9 performance acceptance. Historical
runner470 markers are preserved; actual argv/ELF/source inventories bind472.

The3previously baseline-reproduced native progress timeouts remain failures and
were not reexecuted here; see pipeline470 final-controls/baseline24/27/28. No
claim of complete owner-history acceptance or a measured472 speedup follows.
The bounded pump shares original steps/attempts/effects/retries/events across
passes, preserves per-attempt timers, default depth1, control/cold-group order,
exact unit IDs and pre-Durable client ownership. These functional checks do not
replace later fresh asserted release/framework/native/Mac measured retention.

No binaries/buildtrees/fullsource/WAL/checkpoint state are included. Only changed
source versions, inventories, raw controls and scripts are copied. Source scopes
are qualified at captured hashes; metadata review counts are references only.
Raw files>64KiB are losslessgzip; PACKING records original/container hashes and
lengths. Every nested inventory/manifest is an ordinary payload. SHA256SUMS
covers all regular files recursively except itself. Absolute command paths retain
original spelling. Collector progress is not a qualification exit receipt.
''')
 payloads=sorted(p for p in OUT.rglob('*') if p.is_file());(OUT/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(OUT))+'\n' for p in payloads))
 expected={r:d for d,r in [line.split('  ',1) for line in (OUT/'SHA256SUMS').read_text().splitlines()]};assert set(expected)=={str(p.relative_to(OUT)) for p in OUT.rglob('*') if p.is_file() and p!=OUT/'SHA256SUMS'}
 for name,digest in expected.items():assert sha((OUT/name).read_bytes())==digest
 packed={r['archived_path']:r for r in packing}
 for row in selection:
  raw=(OUT/row['archived_path']).read_bytes()
  if row['archived_path'] in packed:raw=gzip.decompress(raw)
  assert len(raw)==row['original_bytes'] and sha(raw)==row['original_sha256']==sha(Path(row['source_path']).read_bytes())
 for row in packing:assert (OUT/row['archived_path']).stat().st_size==row['container_bytes'] and sha((OUT/row['archived_path']).read_bytes())==row['container_sha256']
 print(json.dumps({'archive':str(OUT),'payloads':len(payloads),'gzip':len(packing),'manifest_sha256':sha((OUT/'SHA256SUMS').read_bytes()),'exact_set_original_container_readback':'PASS'}))
if __name__=='__main__':main()
