"""Lossless current487 Mac wrap-up evidence; no workload or source changes."""
from pathlib import Path
import gzip,hashlib,json,stat
j=Path(__file__).resolve().parent;b=j.parent.parent;raw=j/'collected';review=b/'review'
out=Path('validation/performance/native487-capacity-bound/comparison/mac')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
r=load(review/'actual487-mac/READOUT.json');c=load(j/'COMPACT_READOUT.json');transfer=load(j/'LOCAL_TRANSFER_PROOF.json')
assert r['valid_cases']==r['planned_cases']==48 and not r['failures'] and r['actual_terminal']['exit']==0
assert transfer['all_hashes_lengths_modes_exact'] and transfer['collected_originals']==1036
assert load(b/'ROOT_QUIET_END.json')['mac']['actual_terminal_exit']==0
assert (review/'actual487-mac.exit').read_text().strip()=='0' and (j/'compact.exit').read_text().strip()=='0'
reader=b/'comparison/readout/raw_readout.py'
assert sha(reader)==load(review/'actual487-mac/PROVENANCE.json')['readout_script_sha256']==load(review/'actual487-linux/PROVENANCE.json')['readout_script_sha256']
assert not out.exists(),'Never replace sealed evidence'
selected={}
def add(prefix,folder):
 for p in sorted(folder.rglob('*')):
  if p.is_file()and'__pycache__'not in p.parts:selected[prefix+str(p.relative_to(folder))]=p
add('raw/',raw);add('original-owner/',b/'mac-campaign-owner');add('readout/actual487-mac/',review/'actual487-mac')
for name in ['actual487-mac.command.json','actual487-mac.stdout','actual487-mac.stderr','actual487-mac.exit','INDEPENDENT_BOTH_CONFIG_REVIEW.json','review-mac-config.py','mac-config.stdout','mac-config.stderr','mac-config.exit']:
 selected['readout/'+name]=review/name
selected['readout/raw_readout.py']=reader
for name in ['ROOT_QUIET_GO.json','ROOT_QUIET_END.json']:selected['authority/'+name]=b/name
selected['authority/SCHEMA.md']=b/'comparison/SCHEMA.md'
for p in sorted(j.iterdir()):
 if p.is_file()and p.name not in ['COLLECTED.tar.gz','transfer.tar','prepared.tar','ARCHIVE_RECEIPT.json','ARCHIVE_REVIEW.json','archive.stdout','archive.stderr','archive.exit']:
  selected['collection/'+p.name]=p
out.mkdir(parents=True)
selection=[];packing=[]
for relative,p in sorted(selected.items()):
 data=p.read_bytes();assert not data.startswith((b'\x7fELF',b'!<arch>\n',b'\xcf\xfa\xed\xfe',b'\xfe\xed\xfa\xcf'))
 packed=len(data)>65536 or any(line.endswith((b' ',b'\t',b'\r'))for line in data.split(b'\n'))or data.endswith(b'\n\n')
 saved=relative+('.gz'if packed else'');q=out/saved;q.parent.mkdir(parents=True,exist_ok=True);q.write_bytes(gzip.compress(data,mtime=0)if packed else data);q.chmod(stat.S_IMODE(p.stat().st_mode))
 assert(gzip.decompress(q.read_bytes())if packed else q.read_bytes())==data
 e={'source_path':str(p),'original_path':relative,'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'original_mode':oct(stat.S_IMODE(p.stat().st_mode)),'archived_sha256':sha(q)};selection.append(e)
 if packed:packing.append(e)
lines=['# 487 Mac APFS Counter capacity-bound comparison','','Original67726 actually terminated0 (tool148477) after48 fixed processes, validators, readbacks and post-guards. No case was retried or replaced. Collection began after both original hosts terminated. Baseline is published480 f7b0ef12709bb7e7aad4f0279851aa035b508e9e; candidate is published486 dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69. Both use WitnessLogIo, identical within-host compiler/tools and separately fresh native opt3/debug0/assertions/overflow/default-LTO-codegen artifacts. Configured worker counts are unchanged. No CPU affinity restriction or single-core execution is claimed.','','| Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline p99 ms | Candidate p99 ms | Baseline CPU us/original | Candidate CPU us/original | Baseline average cores | Candidate average cores |','|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for cell in c['cells']:
 a,z=cell['baseline'],cell['candidate'];lines.append(f"| {cell['cell']} | {cell['protocol']} | {a['completed_s']:.3f} | {z['completed_s']:.3f} | {a['p99_ms']:.3f} | {z['p99_ms']:.3f} | {a['cpu_us_per_completed']:.3f} | {z['cpu_us_per_completed']:.3f} | {a['average_busy_cores']:.4f} | {z['average_busy_cores']:.4f} |")
lines+=['','These are three-trial arm medians. Every individual pair, intended/service/dispatch distribution, refusal, recovery, retry, join, physical counter and maintenance record remains in READOUT.json/raw ledgers. All391680 offers reconcile to323608 applied plus68072 window refusals; all other status classes are zero. All1036 original transferred payloads have exact SHA256/length/mode proof.','','All six loaded pairs improve completed throughput, p99 and whole-child CPU per completed original. Loaded TCP CPU ratios are0.536/0.458/0.479; QUIC0.515/0.568/0.491. Average busy cores also decrease, using child user+system CPU seconds divided by that same resource-helper child wall boundary. This avoids attributing gains to additional configured workers or using native-timer elapsed as the denominator. Whole-child CPU includes startup,64warmups, drain, verification, recovery and cleanup; CPU/original divides that broader numerator by measured applied originals and is not timer-only attribution. RSS is raw process bytes, not a continuous plateau.','','Regressions remain: singleton CPU/original worsens in all six pairs. Moderate TCP p99 worsens in one pair (ratio2.439); moderate QUIC in two (1.045/2.304), including15 candidate refusals in trial2. Skew TCP p99 worsens in all three pairs and rate loses two; skew QUIC p99 and rate each lose two. Skew cold TCP p99 trials456.493/460.326/887.317ms become863.912/809.464/425.099; cold QUIC387.448/877.607/480.639 become884.601/458.588/1079.287. All cold cases still apply256/256 with zero refusals. No universal gain or fairness claim follows.','','Singleton256@8/window1/ready16; moderate8192@128/window256/ready16; loaded16000@2000/window4096/ready64; skew8192@1024/window4096/ready64/groups2/31:1 with2048 pending/group. All have64warmups, active2s maintenance and120s drain. Finite offered horizons do not establish maximum/sustained useful capacity, healthy-margin framework floors, continuous bounded debt or full P9 acceptance. Per-group rates share whole-case drain elapsed and cannot replace one-group floors.','','All voters/groups share one APFS physical device on the M2 Max. Source/artifact/compiler/device/CPU guards before/after are retained. Snapshot installs are aggregate replica3 counters and cannot identify individual delayed originals. No missed-timer counter exists; late offer decisions are not timer misses. Inclusive/nested storage/publication/cold elapsed scopes overlap and cannot be summed as CPU. Ordinary provider counters exclude cold selectors/repair/reclaim; append units include metadata and are not literal proposal widths. Encoded bytes are not drive I/O. No ext4/XFS or independent-placement acceptance is claimed.','','Original command/stdout/stderr/exit records and collection provenance remain. A read-only initial path query error is retained; it did not modify or rerun the frozen reader. PACKING/SELECTION bind deterministic lossless gzip, original bytes/hashes/modes and archived representations. SHA256SUMS seals every payload recursively except itself. No source/buildtree/binary/WAL was transferred or archived. This archive finishes current487 evidence only; user requested commit/push and pause afterward.']
(out/'README.md').write_text('\n'.join(lines)+'\n');(out/'SELECTION.json').write_text(json.dumps(selection,indent=2)+'\n');(out/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n')
manifest=''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in sorted(out.rglob('*'))if p.is_file());(out/'SHA256SUMS').write_text(manifest)
receipt={'exit':0,'archive':str(out),'payloads':len(manifest.splitlines()),'packed_originals':len(packing),'selected_originals':len(selection),'raw_originals':1036,'manifest_sha256':sha(out/'SHA256SUMS'),'archived_bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'original_handle':67726,'actual_original_exit':0,'readout_sha256':sha(review/'actual487-mac/READOUT.json'),'same_child_wall_average_cores':True,'no_new_workloads':True}
(j/'ARCHIVE_RECEIPT.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
