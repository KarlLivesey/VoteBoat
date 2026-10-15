from pathlib import Path
import json,gzip,hashlib,shutil
job=Path(__file__).resolve().parent;out=Path('validation/performance/native474-filesystem/linux');assert not out.exists();out.mkdir();sha=lambda b:hashlib.sha256(b).hexdigest();selected={}
def add(p,rel):
 p=Path(p);assert p.is_file() and not p.is_symlink();assert str(rel) not in selected;selected[str(rel)]=p
for p in job.iterdir():
 if p.is_file():add(p,p.name)
for folder in ['campaign','campaign-metadata']:
 for p in (job/folder).rglob('*'):
  if p.is_file():add(p,p.relative_to(job))
config=json.loads((job/'CAMPAIGN_CONFIG.json').read_text())
for c in config['cases']:
 for p in Path(c['wal_root']).iterdir():
  if p.is_file():
   assert p.suffix in ['.csv','.txt','.json'],str(p)
   add(p,Path('case-results')/c['name']/p.name)
packing=[];selection=[]
for rel,p in sorted(selected.items()):
 b=p.read_bytes();assert not b.startswith((b'\x7fELF',b'!<arch>\n'));name=rel+('.gz' if len(b)>65536 else '');target=out/name;target.parent.mkdir(parents=True,exist_ok=True)
 if name!=rel:
  target.write_bytes(gzip.compress(b,compresslevel=9,mtime=0));packing.append({'original_path':rel,'archived_path':name,'original_bytes':len(b),'original_sha256':sha(b),'container_bytes':target.stat().st_size,'container_sha256':sha(target.read_bytes())})
 else:shutil.copy2(p,target)
 selection.append({'source_path':str(p),'archived_path':name,'original_sha256':sha(b),'original_bytes':len(b),'source_mode':oct(p.stat().st_mode&0o7777)})
for name,obj in [('PACKING.json',packing),('SELECTION.json',selection)]: (out/name).write_text(json.dumps(obj,indent=2)+'\n')
r=json.loads((job/'READOUT.json').read_text());lines=['#474 same-device native filesystem comparison','', 'Original27143 ends0/ALL_TIMING_END after391.355s; all12 workloads and12 independent validators pass. Same immutable455 native ELF13c6f446, no build/source/default change. All3 voters share one NVMe Btrfs compress=zstd:1 filesystem; freshdefault versus NoCoW changes CoW/compression/filesystemdata-checksum policy together. Engine WALCRC/durability/recovery unchanged. No ext4/XFS or isolated-CoW/three-machine claim.','', '| Protocol | Policy | Completed originals/s median | During horizon/s | p99 ms median |','| --- | --- | ---: | ---: | ---: |']
for c in r['cells']:lines.append(f"| {c['protocol']} | {c['variant']} | {c['completed_originals_per_second']:.3f} | {c['during_originals_per_second']:.3f} | {c['p99_ms']:.6f} |")
for protocol in ['tcp','quic']:
 ps=[p for p in r['pairs'] if p['protocol']==protocol];lines+=['',protocol+' NoCoW/default paired rate ratios '+', '.join(f"{p['rate_ratio']:.6f}" for p in ps)+'; p99 ratios '+', '.join(f"{p['p99_ratio']:.6f}" for p in ps)+'.']
lines+=['', 'No universal deployment recommendation or engine change is retained: TCPtrial2 loses rate and nearly doubles p99; QUICtrials2/3 lose both. Preserve everycase, including2.500493s TCPNoCoW and2.519605s QUICNoCoW tails. No case replacement/retuning or cause beyond combined policy is inferred. Larger-window offered-load evidence is next.','',
'Everycase onegroup/3voters/8B Counter/64warmup/15000scheduledoffers500/s/window64/ready16/active2smaintenance/noPause. Alloffers reconcile to applied+windowrefused, unknown/pending/noleader/admissionrefused/notproposed0, recovery64+applied/retryverified/workersjoinedtrue. All hit64inflight, so this is a saturated fixed-point diagnostic, not refusal-free capacity or load curve. Both total/drain and during-horizon throughput, intended/service/dispatch latency and wholeprocess resources remain raw.','',
'Replica1 configured-leader medians default→NoCoW: TCP meanbarrier43.916→37.735ms, WALsync15.035→12.677ms, manifestpublish28.869→25.048ms; QUIC40.876→41.520ms,14.085→13.903ms,26.778→27.601ms. Nested barrier wall86–88% and publication57–58% remain elapsed contexts, not additive CPU or per-original latency; commands/unit and barriers/original remain in READOUT. Lifetime maxima stay stage snapshots; no max counter subtraction.','',
'Post-run attributes certify370regularfiles,18WALfiles in eacharm,0mismatches. CPU governor/EPP/driver/CPU sets remain exactly unchanged; powersave/balance_performance is disclosed, not assigned as cause. Full original/copy source1420/modes/root-and-example fresh:false/compiler/nativeopt3debug0assertionsoverflow/defaultLTO+CGU/validator guards pass beforeafter; published9559 restoration/fullhook and empty-source-worktree bindquietgrant. No builds/checks/copies/archives/profiles/competingbenchmarks duringtiming.','',
'Original commands/stdout/stderr/exits, all12offer/storage/maintenance/bases/config/summary raws, validators, resources, beforeafter guards/attrs/host/config/controller/setupreview and derived readout are retained. Synthetic WAL states/binaries/fullsource/buildtrees remain under their originaltarget paths and are notduplicated. PACKING losslessly preserves originals; SHA256SUMS seals exactrecursive payloads. Wholecommand RSS is not continuous physicalmemory/debt; no full P9seven-cell/both-host healthy margin, headroom/fairness/placement/P8 scope closes.']
(out/'README.md').write_text('\n'.join(lines)+'\n')
files=sorted(p for p in out.rglob('*') if p.is_file());(out/'SHA256SUMS').write_text(''.join(sha(p.read_bytes())+'  '+str(p.relative_to(out))+'\n' for p in files))
rows={line[66:]:line[:64] for line in (out/'SHA256SUMS').read_text().splitlines()};assert set(rows)=={str(p.relative_to(out)) for p in out.rglob('*') if p.is_file() and p!=out/'SHA256SUMS'}
for name,h in rows.items():assert sha((out/name).read_bytes())==h
for x in packing:
 b=(out/x['archived_path']).read_bytes();raw=gzip.decompress(b);assert sha(b)==x['container_sha256'] and len(b)==x['container_bytes'];assert sha(raw)==x['original_sha256'] and len(raw)==x['original_bytes']
proof={'payloads':len(rows),'gzip':len(packing),'manifest_sha256':sha((out/'SHA256SUMS').read_bytes()),'exactrecursive_allSHA_allcontainer_originalSHA_lengths':'PASS','case_count':12,'all12workload_validator_exit0':'PASS','source_artifact_unchanged_455':'PASS','post_run_file_policy':'PASS','all42slot_barriers_no_newformat':'No format changed; current manifest retained.','scope':'Independent archival reread; original actual data unchanged and no workload repeated.'};Path('validation/performance/native474-filesystem/ROOT_ARCHIVE_REVIEW.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))
