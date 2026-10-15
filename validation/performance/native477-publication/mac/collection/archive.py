"""Seal original Mac477 paired evidence, preserving large original byte hashes."""
from pathlib import Path
import gzip,hashlib,json

job=Path(__file__).resolve().parent;raw=job/'raw';preparation=job.parent
destination=Path('validation/performance/native477-publication/mac')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
analysis=json.loads((job/'ANALYSIS.json').read_text());config=json.loads((raw/'CAMPAIGN_CONFIG.json').read_text())
assert analysis['cases']==analysis['validators']==analysis['readback_checks']==48 and analysis['actual_exit']==0
assert json.loads((preparation/'mac-live/EXIT.json').read_text())['exit']==0
assert json.loads((preparation/'ROOT_QUIET_END.json').read_text())['mac']['actual_terminal_exit']==0
assert not destination.exists(), 'refuse replacing sealed evidence'
payloads={str(p.relative_to(raw)):p for p in raw.rglob('*') if p.is_file()}
for name in ['ANALYSIS.json','ANALYSIS_EXECUTION.json','LOCAL_TRANSFER_PROOF.json','collect.py','analyse.py','archive.py']:
 payloads['collection/'+name]=job/name
for p in job.glob('*'):
 if p.is_file() and (p.name.endswith('.command.json') or p.name.endswith('.stdout') or p.name.endswith('.stderr')):
  payloads['collection/'+p.name]=p
for p in (preparation/'mac-live').iterdir():
 if p.is_file():payloads['original-controller/'+p.name]=p
for p in preparation.iterdir():
 if p.is_file() and p.suffix!='.tar' and p.name!='INDEPENDENT_READOUT.json':
  payloads['preparation/'+p.name]=p
for folder in ['mac-prepared','mac-transfer','mac-transfer2']:
 for p in (preparation/folder).rglob('*'):
  if p.is_file() and '__pycache__' not in p.parts:
   payloads['preparation/'+folder+'/'+str(p.relative_to(preparation/folder))]=p
for folder in sorted(preparation.glob('resource-*')):
 if folder.is_dir():
  for p in folder.rglob('*'):
   if p.is_file():payloads['preparation/'+folder.name+'/'+str(p.relative_to(folder))]=p
for name in ['independent-review.py','INDEPENDENT_READOUT.json','independent-review.stdout','independent-review.stderr','independent-cold-review.py','INDEPENDENT_COLD_WORK.json','independent-cold-review.stdout','independent-cold-review.stderr','NEXT_STAGE_REVIEW.md','INDEPENDENT_REVIEW_RECEIPT.json']:
 payloads['independent-review/'+name]=preparation/name
proof={'scope':'48 original Mac477 paired case processes, frozen validators and readback checks all actually ended0. Original94749; no retries/replacements. Not fullP9, sustained20M, default promotion or three-filesystem acceptance.',
 'source_commit':analysis['source_commit'],'proof_document_commit':'c3aeed5c800fcb47e70a8b0df1cbd5f655d9dfe2',
 'native_executable_sha256':analysis['artifact_sha256'],'native_profile':'Same fresh476 native opt3/debug0/assertions+overflow/defaultLTO+codegen executable for both arms.',
 'common_source_files':len(config['candidate']['source']['files']),'source_device_cpu_guards_before_after_exact':True,
 'raw_transfer_members':len(json.loads((raw/'COLLECTED.json').read_text())['files']),
 'totals':analysis['totals'],'maintenance_status_counts':analysis['maintenance_status_counts'],
 'timer_metric_limit':'No explicit missed-timer count is exposed. All raw logs, dispatch distributions and late decisions are retained; late offer decisions are not timer misses.',
 'latency_convention':'Nearest rank ceil(n*p/100)-1; intended/service successful originals, dispatch all offered rows.',
 'counter_scope':'Ordinary provider primitives exclude cold repair, initial selectors and reclamation. Metadata-inclusive command/unit mean is not a proposal-width maximum; codec bytes are not drive I/O; nested wall durations are not additive CPU.',
 'resource_scope':'Whole child process including warmup/recovery/cleanup; Mac RSS raw bytes. No continuous memory/debt plateau claim.',
 'host':'M2 Max; macOS/APFS Data volume/container/internal SSD shared by all three voters; stable1.98.0/LLVM22.1.8/protoc3.21.12. Linux1.98.1 is a separate within-host comparison.',
 'transfer_scope':'Post-terminal text/CSV/JSON only; no binaries, source/buildtrees or generated WAL transferred. Original stores remain on Mac.',
 'preparation_scope':'Superseded successful configurations and deliberate negative helper/guard probes retained. Initial Linux sandbox discovery failure is included as shared setup context, not a Mac benchmark failure.'}
(job/'ARCHIVE_PROOF.json').write_text(json.dumps(proof,indent=2)+'\n');payloads['collection/ARCHIVE_PROOF.json']=job/'ARCHIVE_PROOF.json'
destination.mkdir(parents=True);packing=[]
for relative,original in sorted(payloads.items()):
 content=original.read_bytes();saved=relative+'.gz' if len(content)>65536 else relative
 target=destination/saved;target.parent.mkdir(parents=True,exist_ok=True)
 target.write_bytes(gzip.compress(content,mtime=0) if saved!=relative else content)
 assert (gzip.decompress(target.read_bytes()) if saved!=relative else target.read_bytes())==content
 if saved!=relative:packing.append({'original_path':relative,'archived_path':saved,'original_bytes':len(content),'original_sha256':sha(original)})
(destination/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n')
lines=['#477 Mac APFS paired publication results','',
 'Original SSH94749 actually terminated0 after ALL_TIMING_END; all48 case processes,48 frozen validators and48 readback checks passed. No case was retried or replaced. Both hosts terminated before collection. Source7e9068296ce00dde5bf3e03bf4d852ed2833d185 and the same qualified native476 executable serve both explicit atomic and witness arms;476 qualification is archived separately. This is real durable three-voter Counter over TCP/TLS or QUIC, not the volatile framework.', '',
 '| Cell | Protocol | Atomic completed/s | Witness completed/s | Atomic p99 ms | Witness p99 ms | Atomic window refusals | Witness window refusals |',
 '|---|---|---:|---:|---:|---:|---:|---:|']
for cell in analysis['cells']:
 a=cell['arms']['atomic'];w=cell['arms']['witness']
 lines.append(f"| {cell['cell']} | {cell['protocol']} | {a['completed_originals_s']['median']:.3f} | {w['completed_originals_s']['median']:.3f} | {a['intended_latency']['p99_ms']['median']:.3f} | {w['intended_latency']['p99_ms']['median']:.3f} | {a['refusals']['window_refused']['median']:.0f} | {w['refusals']['window_refused']['median']:.0f} |")
lines+=['', 'Three-trial arm medians; ANALYSIS.json retains every trial, paired ratios/ranges, regressions, p50/p95/p99/max, intended/service/dispatch latency, during/drain rates, all refusal classes, backlog, physical deltas, maintenance and resources. Singleton offers8/s with window1; moderate128/s with window256; bulk512/s and overload1024/s with window4096. Offered caps are not maximum-capacity measurements.', '',
 'All297984 offers reconcile to285221 applied plus12763 window refusals; other refusal/unknown/pending classes are zero. All cases recover64+applied, preserve original-operation retries and join workers. Active maintenance completes672 checkpoints and2016 reclaims. Readbacks cover all three Create and three Recover providers/capacity profiles. Source/artifact, CPU-policy and device guards match before/after.', '',
 'Bulk rate and p99 improve in all six pairs: TCP357.263 to481.317 completed originals/s, p996927.865 to1275.512ms; QUIC353.521 to470.403/s, p997178.674 to1472.977ms. Witness bulk still has539–745 queued originals at horizon across trials: zero bulk refusals is buffering, not refusal-free headroom. Overload remains queue-limited with refusals and seconds of tail latency.', '',
 'The moderate QUIC regression is retained: p99 median897.705 to1891.831ms, worse in two trials; witness refusals43/0/3 versus atomic0/0/0. Singleton TCP still refuses19–20 of256 with witness; QUIC refuses15–19. This slice does not establish uniformly better latency.', '',
 'Mean leader bulk TCP barrier22.174 to14.007ms and ordinary publication16.310 to7.243ms; QUIC22.379 to14.292ms and16.543 to7.470ms. Command/unit means fall roughly9.05 to7.71 TCP and9.10 to7.69 QUIC. The benefit is not a larger observed batch. Metadata-inclusive command/unit means are not literal proposal widths; encoded codec bytes are not device I/O. Primitive wall scopes nest and are not additive CPU. Ordinary witness namespace counters exclude cold selector/reclaim/repair and do not imply zero whole-process namespace work.', '',
 'Raw ledgers and full controller/child logs are preserved. The fixture exposes no explicit missed-timer counter;15 late offer decisions and dispatch tails are retained without calling them timer misses. Whole-process RSS is in Mac bytes and includes warmup/recovery/cleanup, not a sustained memory/debt plateau. Three voters share one APFS internal SSD; this is not independent placement or filesystem-only causation.', '',
 'Preparation negatives, superseded configurations and the shared initial Linux sandbox discovery failure remain available. No Mac case failed or was replaced. Large originals are losslessly packed with original hashes/lengths in PACKING.json; SHA256SUMS seals every payload. Source trees, binaries, buildtrees and generated WAL remain on their original hosts. FileLogIo remains default; real ext4/XFS and full P9 acceptance remain open.']
# Keep numerical values literal while making the evidence narrative readable.
for index,line in enumerate(lines):
 for old,new in [('SSH94749','SSH handle 94749'),('terminated0','terminated with exit 0'),('all48','all 48'),('48 frozen','48 frozen'),('Source7e','Source 7e'),('native476','native476'),('offers8/s','offers 8/s'),('window1','window 1'),('moderate128/s','moderate 128/s'),('window256','window 256'),('bulk512/s','bulk 512/s'),('overload1024/s','overload 1024/s'),('window4096','window 4096'),('All297984','All 297984'),('to285221','to 285221'),('plus12763','plus 12763'),('recover64','recover 64'),('completes672','completes 672'),('and2016','and 2016'),('TCP357.263','TCP 357.263'),('to481.317','to 481.317'),('p996927.865','p99 6927.865'),('to1275.512ms','to 1275.512 ms'),('QUIC353.521','QUIC 353.521'),('to470.403','to 470.403'),('p997178.674','p99 7178.674'),('to1472.977ms','to 1472.977 ms'),('has539','has 539'),('median897.705','median 897.705'),('to1891.831ms','to 1891.831 ms'),('refusals43','refusals 43'),('atomic0/0/0','atomic 0/0/0'),('refuses19','refuses 19'),('of256','of 256'),('refuses15','refuses 15'),('barrier22.174','barrier 22.174'),('to14.007ms','to 14.007 ms'),('publication16.310','publication 16.310'),('to7.243ms','to 7.243 ms'),('QUIC22.379','QUIC 22.379'),('to14.292ms','to 14.292 ms'),('and16.543','and 16.543'),('to7.470ms','to 7.470 ms'),('roughly9.05','roughly 9.05'),('to7.71','to 7.71'),('and9.10','and 9.10'),('to7.69','to 7.69'),('counter;15','counter; 15')]:line=line.replace(old,new)
 lines[index]=line
(destination/'README.md').write_text('\n'.join(lines)+'\n')
manifest=''.join(f'{sha(p)}  {p.relative_to(destination)}\n' for p in sorted(destination.rglob('*')) if p.is_file())
(destination/'SHA256SUMS').write_text(manifest)
print(json.dumps({'archive':str(destination),'payloads':len(manifest.splitlines()),'packed':len(packing),'manifest_sha256':sha(destination/'SHA256SUMS')},indent=2))
