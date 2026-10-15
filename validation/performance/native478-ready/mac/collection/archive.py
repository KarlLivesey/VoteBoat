"""Seal original Mac478 evidence after both-host terminal; never replace an archive."""
from pathlib import Path
import gzip,hashlib,json

job=Path(__file__).resolve().parent;raw=job/'raw';preparation=job.parent
destination=Path('validation/performance/native478-ready/mac')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
analysis=load(job/'ANALYSIS.json');config=load(raw/'CAMPAIGN_CONFIG.json')
assert analysis['cases']==analysis['validators']==analysis['readback_checks']==48 and analysis['actual_exit']==0
assert load(preparation/'mac-live/EXIT.json')['exit']==0
assert load(preparation/'ROOT_QUIET_END.json')['mac']['actual_terminal_exit']==0
assert not destination.exists(),'Refuse replacing sealed evidence'
payloads={str(p.relative_to(raw)):p for p in raw.rglob('*')if p.is_file()}
for p in job.iterdir():
 if p.is_file()and p.name not in ['COLLECTED.tar.gz','ARCHIVE_REVIEW.json']:
  payloads['collection/'+p.name]=p
for p in (preparation/'mac-live').rglob('*'):
 if p.is_file():payloads['original-controller/'+str(p.relative_to(preparation/'mac-live'))]=p
for p in preparation.iterdir():
 if p.is_file()and p.suffix!='.tar'and not p.name.startswith('independent-'):
  payloads['preparation/'+p.name]=p
for folder in ['mac-prepared','mac-transfer','common']:
 for p in (preparation/folder).rglob('*'):
  if p.is_file()and '__pycache__'not in p.parts:
   payloads['preparation/'+folder+'/'+str(p.relative_to(preparation/folder))]=p
independent=load(preparation/'independent-receipt.json')
assert independent['exit']==0
for p in preparation.glob('independent-*'):
 if p.is_file():payloads['independent-review/'+p.name]=p
proof={'scope':'48 original Mac478 ready16/64 paired processes, frozen validators and readback checks all actually ended0. Original14036; no retries/replacements. Not fullP9, sustained20M, default promotion or three-filesystem acceptance.',
 'source_commit':analysis['source_commit'],'schema_evidence_commit':'7c3795e5f155479ab703b1a4ed12a1a3772f399e',
 'native_executable_sha256':analysis['artifact_sha256'],'native_profile':'Same fresh476 native opt3/debug0/assertions+overflow/defaultLTO+codegen executable for both arms. No rebuild.',
 'common_source_files':len(config['candidate']['source']['files']),'source_device_cpu_guards_before_after_exact':True,
 'raw_transfer_members':len(load(raw/'COLLECTED.json')['files']),'totals':analysis['totals'],'maintenance_status_counts':analysis['maintenance_status_counts'],
 'timer_metric_limit':'No explicit missed-timer count is exposed. Dispatch distributions and late offer decisions are retained; late decisions are not timer misses.',
 'latency_convention':'Nearest rank ceil(n*p/100)-1; intended/service successful originals, dispatch all offered rows. Per-group completed rates share the original whole-case elapsed boundary; during rates use the fixed offered horizon.',
 'counter_scope':'Ordinary provider primitives exclude cold repair, initial selectors and reclamation. Metadata-inclusive command/unit mean is not a proposal-width maximum; codec bytes are not drive I/O; nested wall durations are not additive CPU.',
 'resource_scope':'Whole child process including warmup/recovery/cleanup; Mac RSS raw bytes. No continuous memory/debt plateau claim.',
 'host':'M2 Max; macOS/APFS Data volume/container/internal SSD shared by all three voters and both skew groups; stable1.98.0/LLVM22.1.8/protoc3.21.12. Linux1.98.1 is a separate within-host comparison.',
 'transfer_scope':'Post-terminal text/CSV/JSON only; no binaries, source/buildtrees or generated WAL transferred. Original stores remain on Mac.',
 'observation_failures':'Three absent-path status probes returned1; actual available tool-output text and commands/provenance retained. These were observations only; all48 native processes/validators/readbacks ended0 with no replacement.'}
(job/'ARCHIVE_PROOF.json').write_text(json.dumps(proof,indent=2)+'\n');payloads['collection/ARCHIVE_PROOF.json']=job/'ARCHIVE_PROOF.json'
lines=['#478 Mac APFS ready16/64 paired results','',
 'Original SSH handle 14036 actually terminated with exit 0 after ALL_TIMING_END. All 48 native cases, 48 frozen validators and 48 readback checks passed; no case was retried or replaced. Both hosts terminated before collection. Both arms explicitly select WitnessLogIo and reuse source 7e9068296ce00dde5bf3e03bf4d852ed2833d185 and the same fresh native476 executable. No source/build/profile/default change.', '',
 '| Cell | Protocol | q16 completed/s | q64 completed/s | q16 during/s | q64 during/s | q16 p99 ms | q64 p99 ms | q16 median window refusals | q64 median window refusals |',
 '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for cell in analysis['cells']:
 a=cell['arms']['q16'];b=cell['arms']['q64']
 lines.append(f"| {cell['cell']} | {cell['protocol']} | {a['completed_originals_s']['median']:.3f} | {b['completed_originals_s']['median']:.3f} | {a['during_originals_s']['median']:.3f} | {b['during_originals_s']['median']:.3f} | {a['intended_latency']['p99_ms']['median']:.3f} | {b['intended_latency']['p99_ms']['median']:.3f} | {a['refusals']['window_refused']['median']:.0f} | {b['refusals']['window_refused']['median']:.0f} |")
lines+=['',
 'Three-trial arm medians. ANALYSIS.json retains every raw-recomputed trial and all 24 paired comparisons, intended/service/dispatch p50/p95/p99/max, during/drain rates, every refusal class, per-group backlog, resources, physical deltas and cold maintenance. Singleton offers 8/s with window 1; moderate 128/s with window 256; bulk/skew 1024/s with total window 4096. Skew uses two groups and weights 31:1, with the fixture enforcing 2048 pending per group. These finite offered caps are not maximum capacity or sustained 20M acceptance.', '',
 'The 297984 offers reconcile to 265041 applied plus 32943 window refusals. Other refusal/unknown/pending classes are zero. Every case recovers 64+applied, verifies original-operation retries and joins workers. Active maintenance completes 624 checkpoints and 1872 reclaims. All actual groups have leader replica 1; the full leader placements are preserved in each pair. All voters and both skew groups share the same physical APFS device.', '',
 'Bulk gains occur in every pair: TCP 491.279 to 1015.083 completed originals/s and QUIC 492.916 to 1011.578/s. q64 bulk refuses no offers, while q16 TCP refuses 207/560/389 and QUIC 127/238/347. Leader barriers per completed original fall roughly 0.125 to 0.060 while mean barriers remain around 14 ms. Metadata-inclusive command/unit means increase roughly 8 to 16–17; these are not literal command proposal widths. Bulk remains capped near its 1024/s offer rate, so this does not establish maximum capacity or spare headroom.', '',
 'Regressions remain visible. Singleton TCP loses about 1.66% completed rate in two pairs and median p99 increases 158.825 to 160.021 ms; singleton QUIC median p99 increases 158.018 to 162.445 ms. Moderate TCP p99 rises 762.654 to 792.174 ms and moderate QUIC 776.264 to 1230.672 ms, with two slower-tail trials each. Moderate refusals remain zero; successful-throughput equality does not erase the tail losses.', '',
 '| Skew group | Protocol | q16 completed/s | q64 completed/s | q16 during/s | q64 during/s | q16 p99 ms | q64 p99 ms | q16 median refused | q64 median refused |',
 '|---|---|---:|---:|---:|---:|---:|---:|---:|---:|']
for cell in analysis['cells']:
 if cell['cell']!='skew':continue
 for group in ['1','2']:
  a=cell['arms']['q16']['groups'][group];b=cell['arms']['q64']['groups'][group]
  lines.append(f"| {'Hot 7936 offers'if group=='1'else'Cold 256 offers'} | {cell['protocol']} | {a['completed_originals_s']['median']:.3f} | {b['completed_originals_s']['median']:.3f} | {a['during_originals_s']['median']:.3f} | {b['during_originals_s']['median']:.3f} | {a['intended_latency']['p99_ms']['median']:.3f} | {b['intended_latency']['p99_ms']['median']:.3f} | {a['refusals']['window_refused']['median']:.0f} | {b['refusals']['window_refused']['median']:.0f} |")
lines+=['',
 'All 256 cold offers apply in every skew case with no cold refusals; cold during rates remain 31.0–31.5/s. Per-group completed rates use the same whole-case elapsed denominator and therefore include hot drain time. q64 hot refusals remain TCP 2687/1556/1146 and QUIC 1410/1216/1809. Hot backlog still approaches the 2048 per-group fixture ceiling. Cold TCP trial 1 p99 worsens 441.054 to 1695.135 ms; cold QUIC trial 3 worsens 879.274 to 1155.945 ms. TCP median cold p99 worsens 453.489 to 841.038 ms. Higher total throughput does not demonstrate uniformly preserved cold latency.', '',
 'The remaining measured stage is still persistence: q64 bulk/skew leader barriers account for roughly 85–89% of native elapsed wall time. This is inclusive wall attribution, not CPU time or proof of a cause inside that scope. Whole-child skew CPU usage is roughly 0.34–0.45 core equivalent over its separate warmup/recovery-inclusive boundary. Primitive ordinary namespace counters exclude cold selectors/reclaim/repair. All per-replica counters and append-unit/group histograms remain available; no proposal-width maximum or drive-byte claim is made.', '',
 'Five late offer decisions are retained, with all dispatch distributions; no explicit missed-timer counter exists. Mac whole-process RSS is in bytes and includes warmup/recovery/cleanup, not a continuous memory/debt plateau. Three observation-only missing-path probes are retained with honest tool-transcript provenance; they did not alter or replace the campaign. Original child/controller commands, streams, resource receipts and all setup outputs remain available.', '',
 'Large originals are losslessly packed with original hashes/lengths in PACKING.json; SHA256SUMS seals every payload. No source trees, binaries, buildtrees or generated WAL are included. FileLogIo and ready16 remain defaults. Further real ext4/XFS, sustained acceptance, independent placement and full P9 qualification remain open.']
(destination/'README.md').parent.mkdir(parents=True)
(destination/'README.md').write_text('\n'.join(lines)+'\n')
packing=[]
for relative,original in sorted(payloads.items()):
 content=original.read_bytes();saved=relative+'.gz'if len(content)>65536 else relative
 target=destination/saved;target.parent.mkdir(parents=True,exist_ok=True)
 target.write_bytes(gzip.compress(content,mtime=0)if saved!=relative else content)
 assert (gzip.decompress(target.read_bytes())if saved!=relative else target.read_bytes())==content
 if saved!=relative:packing.append({'original_path':relative,'archived_path':saved,'original_bytes':len(content),'original_sha256':sha(original)})
(destination/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n')
manifest=''.join(f'{sha(p)}  {p.relative_to(destination)}\n'for p in sorted(destination.rglob('*'))if p.is_file())
(destination/'SHA256SUMS').write_text(manifest)
print(json.dumps({'archive':str(destination),'payloads':len(manifest.splitlines()),'packed':len(packing),'manifest_sha256':sha(destination/'SHA256SUMS')},indent=2))
