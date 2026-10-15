#!/usr/bin/env python3
"""Archive the completed24-case native472 campaign; never executes workloads."""
from pathlib import Path
import gzip,hashlib,json,shutil,stat
ROOT=Path(__file__).resolve().parents[3]
JOB=ROOT/'target/p9/comparison472-native/linux'
OUT=ROOT/'validation/performance/comparison472-native/linux'
def sha(data):return hashlib.sha256(data).hexdigest()
def load(p):return json.loads(Path(p).read_text())
def main():
 terminal=load(JOB/'TERMINAL.json');assert terminal['original_handle']==33407 and terminal['actual_exit']==0 and terminal['workloads']==24 and terminal['validator_commands']==48 and terminal['workload_failures']==terminal['validator_failures']==0
 config=load(JOB/'CAMPAIGN_CONFIG.json');campaign=load(JOB/'campaign/CAMPAIGN.json');assert len(campaign['cases'])==24 and all(c['exit']==0 and c['validator_exits']==[0,0] for c in campaign['cases'])
 assert (JOB/'CAMPAIGN_STAGE').read_text().strip()=='ALL_TIMING_END'
 fresh=ROOT/'validation/performance/pipeline472/fresh-linux/SHA256SUMS';assert sha(fresh.read_bytes())=='af2b60150a81a0e9757a4ae10ac2c451058b6bf25384cfc5f6b5a318fa973cb7'
 assert not OUT.exists(),'No overwrite of sealed/partial archive'
 selected={}
 def add(p,relative):
  p=Path(p);assert p.is_file() and not p.is_symlink(),p
  relative=str(relative);assert not Path(relative).is_absolute() and '..' not in Path(relative).parts
  assert relative not in selected or sha(selected[relative].read_bytes())==sha(p.read_bytes())
  selected[relative]=p
 def tree(directory,prefix):
  for p in Path(directory).rglob('*'):
   if p.is_file() and '__pycache__' not in p.parts:add(p,Path(prefix)/p.relative_to(directory))
 for p in JOB.iterdir():
  if p.is_file():add(p,p.name)
 for name in ['campaign','campaign-metadata','analysis-attempt1','control-tools']:tree(JOB/name,name)
 for case in campaign['cases']:
  for p in Path(case['wal_root']).iterdir():
   if p.is_file() and p.suffix in {'.csv','.txt','.json'}:add(p,Path('case-output')/case['name']/p.name)
 for variant in ['baseline','candidate']:
  native=Path(config[variant]['source_inventory']).parent
  for name in ['SOURCE.json','ARTIFACTS.json','PROFILE_PROOF.json','SYMBOLS.json','FUNCTIONAL_QUALIFICATION.json','PUBLICATION.json','QUALIFIED_SOURCE.json','BUILD_RUN.json','BUILD_TERMINAL.json','SOURCE_EXPORT_TERMINAL.json','REQUIRED_COMMIT.txt','STATUS','STAGE']:
   if (native/name).is_file():add(native/name,Path('identity')/variant/name)
  for name in ['rustc-path.txt','cargo-path.txt','protoc-path.txt','rustc.txt','cargo.txt','protoc.txt','tool-binaries.sha256','baseline-tool-preflight.log','native-release-profile.txt','native-root-rustc-lines.txt']:
   if (native/'logs'/name).is_file():add(native/'logs'/name,Path('identity')/variant/'compiler'/name)
 for p in JOB.parent.glob('*.py'):add(p,Path('scripts')/p.name)
 add(ROOT/'validation/performance/comparison472-native/SCHEMA.md','SCHEMA.md')
 OUT.mkdir(parents=True);packing=[];selection=[]
 for name,p in sorted(selected.items()):
  raw=p.read_bytes();assert not raw.startswith((b'\x7fELF',b'!<arch>\n')),p
  archived=name+'.gz' if len(raw)>65536 else name;target=OUT/archived;target.parent.mkdir(parents=True,exist_ok=True)
  if archived!=name:
   target.write_bytes(gzip.compress(raw,compresslevel=9,mtime=0));target.chmod(stat.S_IMODE(p.stat().st_mode));packing.append({'original_path':name,'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'container_bytes':target.stat().st_size,'container_sha256':sha(target.read_bytes())})
  else:shutil.copy2(p,target)
  selection.append({'source_path':str(p),'archived_path':archived,'original_bytes':len(raw),'original_sha256':sha(raw),'source_mode':oct(stat.S_IMODE(p.stat().st_mode))})
 for name,value in [('PACKING.json',packing),('SELECTION.json',selection),('FRESH_ARTIFACT_REFERENCE.json',{'path':str(fresh.parent),'manifest_sha256':sha(fresh.read_bytes()),'scope':'Sealed357-payload fresh Linux archive includes full candidate CargoJSON/vv/build/control/manifest-omission chronology. Compact immutable baseline470/candidate472 identities are duplicated here; large build logs/binaries are not.'})]: (OUT/name).write_text(json.dumps(value,indent=2)+'\n')
 (OUT/'README.md').write_text('''# Native472 finite paired ordering comparison

Original33407 actually terminated0/ALL_TIMING_END:24 workload processes and48
post-campaign raw+selection validators all0. All cases, refusal ledgers, tails,
readbacks, resources and raw statuses are preserved without replacement/rerun.
Old470 versus new472 each default1/selected4, TCP/QUIC, three paired trials:
ABCD/BADC/CDAB (not a fully balanced four-trial Latin square). Every case uses
15000offers at500/s,30s horizon,64warmup,window64/ready16,onegroup3voters,8B
Counter, active2smaintenance, no pause. Both use nativeopt3/debug0/assert+overflow
/defaultLTO+CGU and identical within-host tools/configuration. Primary selected
comparison is470ON→472ON; default arms expose common-path regressions.

Actual completed originals/second medians (total including final drain):
TCP default159.952699→148.767926, selected148.735150→134.986591.
QUIC default148.802908→154.403297, selected122.190528→150.639131.
Selected rate ratios TCP1.052302/.853259/1.126598; QUIC1.040483/1.327552/1.266712.
Selected intended-completion p99 medians TCP2159.825357→1552.317153ms,
QUIC2498.553566→1276.462611ms. TCP selected trial2 loses rate and has2.231092x
p99; QUIC selected trial1 p99 regresses1.360566x. Default TCPtrial2 rate/tail
regresses; default QUIC has one rate loss and all3p99 regressions. Every other
pair/tail, during-horizon completion rate and exact count/elapsed numerator is in
PAIRED/SUMMARY/OUTCOMES plus authoritative raw CSV. Median alone is insufficient.

All24 recover64+actualapplied and verify retries/worker joins; high window
refusals remain actual outcomes, not successful client work. Physical sharing
is rare: selected leader synchronized multi-unit calls old/new TCP14/8/2 versus
12/7/4, QUIC6/12/10 versus8/8/6. Those contain known suffix originals, including
replica/retry observations, not unique client successes. Leader barriers/applied
selected medians TCP.138998→.140683 and QUIC.141319→.139857 do not demonstrate
consistent both-protocol amortization. No source-cause or performance-win claim.
Same instrumentation code can perform grouping-dependent correlation work;
results measure instrumented end-to-end usefulness, not observer-free pump cost.

Initial readout0 incorrectly subtracted append/barrier lifetime maxima under a
counter-delta label. Initial derived files/script remain analysis-attempt1;
ANALYSIS_LABEL_CORRECTION documents the correction. Current readout retains
lifetime maxima as raw snapshots separately; SUMMARY+PAIRED bytes are unchanged.
All raw outputs are unchanged; p99 derives from original offer rows. Counter
deltas, snapshot gauges, lifetime maxima and interval latency are distinct.
Nested wall scopes overlap and are not additive CPU. Whole-process LinuxmaxRSS
(rawKiB/bytes) includes warmup/recovery/cleanup; not engine ownership or a
continuous RSS/debt plateau. Overflow histogram bins are not exact widths.

Source/artifact/compiler/profile/control/publication identity is bound in config
and compact identity records. FRESH_ARTIFACT_REFERENCE pins the sealed fresh472
archive, including failed first framework export101 and corrected supplemental
manifest qualification; this native campaign did not rebuild those artifacts.
Framework bulk/singleton and Mac data are separate. Three inherited owner
history timeouts remain failures. This finite refused-load diagnostic is not
20Msustained, seven-cell healthy minimum, fairness, memory/headroom or fullP9
acceptance. No retention decision is asserted by this archive.

No binaries/sidecar bytes, fullsource, buildtrees, WAL/checkpoint/nested replica
state or large duplicated Cargo logs are included. Only top-level benchmark
CSV/text/readbacks are selected. Files>64KiB use losslessgzip with original and
container SHA/length in PACKING. SELECTION records historical original paths and
modes. Nested inventories/manifests are ordinary payloads. SHA256SUMS covers the
exact recursive regular-file set except itself. No Mac copies or new checks.
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
