"""Adapt484 central archiver to current487 numerical wrap-up; leave host seals untouched."""
from pathlib import Path
import gzip,hashlib,json,shutil,stat
base=Path('target/p9/native487-capacity-bound');review=base/'review';out=Path('validation/performance/native487-capacity-bound/comparison/review');assert not out.exists()
load=lambda p:json.loads(Path(p).read_text());sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
end=load(base/'ROOT_QUIET_END.json');assert all(end[h]['actual_terminal_exit']==0 for h in ['linux','mac'])
sealed=load(review/'independent-actual/FILES.json')
for name,item in sealed.items():
 p=review/'independent-actual'/name;assert sha(p)==item['sha256']and p.stat().st_size==item['bytes']
assert load(review/'independent-actual/RECEIPT.json')['actual_exit']==0
assert load(review/'independent-actual/CORE_DECOMPOSITION.json')['loaded_checks']['average_busy_cores_falls']==12
selected={}
def add(p,rel):
 p=Path(p);rel=str(rel);assert p.is_file()and not p.is_symlink();assert rel not in selected;selected[rel]=p
for n in ['ROOT_QUIET_GO.json','ROOT_QUIET_END.json']:add(base/n,Path('authority')/n)
for host,job in [('linux',base/'comparison/linux/campaign'),('mac',base/'comparison/mac/collected')]:
 frozen=review/('actual487-'+host);r=load(frozen/'READOUT.json');assert r['valid_cases']==48 and not r['failures']
 for p in frozen.iterdir():
  if p.is_file():add(p,Path('frozen-readout')/host/p.name)
 for p in review.glob('actual487-'+host+'.*'):add(p,Path('frozen-readout')/host/p.name)
 for n in ['STARTED.json','TERMINAL.json','CAMPAIGN_STATUS','CAMPAIGN_STAGE','READY.json','CAMPAIGN_CONFIG.json']:add(job/n,Path('original-campaign')/host/n)
 add(job/'campaign/CAMPAIGN.json',Path('original-campaign')/host/'CAMPAIGN.json')
 if host=='linux':
  for n in ['controller.stdout','controller.stderr','controller.exit']:add(job/n,Path('original-campaign')/host/n)
 else:add(job/'COLLECTED.json',Path('original-campaign')/host/'COLLECTED.json')
 verification=base/'root'/(host.upper()+'_PAIRED_ARCHIVE_VERIFIED.json');assert load(verification)['exit']==0;add(verification,Path('campaign-archive-verification')/(host+'.json'));add(base/'root'/('verify-'+host+'-paired.py'),Path('campaign-archive-verification')/('verify-'+host+'-paired.py'));add(out.parent/host/'SHA256SUMS',Path('campaign-archive-verification')/(host+'-SHA256SUMS'))
for p in(review/'independent-actual').iterdir():
 if p.is_file():add(p,Path('independent96')/p.name)
for p in(base/'mac-campaign-owner').iterdir():
 if p.is_file():add(p,Path('original-campaign/mac-outer')/p.name)
for p in(base/'publication').iterdir():
 if p.is_file():add(p,Path('premeasurement-publication')/p.name)
for p in review.iterdir():
 if p.is_file()and(p.name.startswith(('CPU_','cpu-summary-'))or p.name in ['FINAL_RETENTION_DECISION.json','archive-central.py','archive-central.command.json']):add(p,Path('review-provenance')/p.name)
for n in ['COMPACT_READOUT.json','compact.py','compact.stdout','compact.stderr','compact.exit','LOCAL_TRANSFER_PROOF.json','collect.command.json','collect.stdout','collect.stderr','collect.exit','transfer-raw.command.json','transfer-raw.stderr','transfer-raw.exit']:add(base/'comparison/mac'/n,Path('mac-owner-provenance')/n)
add(base/'comparison/readout/raw_readout.py','frozen-readout/raw_readout.py')
out.mkdir(parents=True);selection=[];packing=[]
for rel,p in sorted(selected.items()):
 data=p.read_bytes();assert not data.startswith((b'\x7fELF',b'!<arch>\n'))
 packed=len(data)>65536 or p.suffix in ['.stdout','.stderr','.log']or data.endswith(b'\n\n')or any(line.rstrip(b'\r\n').endswith((b' ',b'\t'))for line in data.splitlines(keepends=True));saved=rel+('.gz'if packed else'');dest=out/saved;dest.parent.mkdir(parents=True,exist_ok=True)
 if packed:dest.write_bytes(gzip.compress(data,mtime=0));packing.append({'original_path':rel,'archived_path':saved,'original_bytes':len(data),'original_sha256':sha(p),'container_sha256':sha(dest)})
 else:shutil.copy2(p,dest)
 dest.chmod(stat.S_IMODE(p.stat().st_mode));selection.append({'source_path':str(p),'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(data),'source_mode':oct(stat.S_IMODE(p.stat().st_mode)),'gzip':packed})
save(out/'SELECTION.json',selection);save(out/'PACKING.json',packing)
for item in selection:
 p=out/item['archived_path'];data=gzip.decompress(p.read_bytes())if item['gzip']else p.read_bytes();assert len(data)==item['original_bytes']and hashlib.sha256(data).hexdigest()==item['original_sha256'];assert oct(stat.S_IMODE(p.stat().st_mode))==item['source_mode']
checks={'exit':0,'scope':'Central487 current numerical wrap-up only. Both immutable host archive seals unchanged. No workload/build/profile/validator rerun; frozen reader and independent raw recomputation retained.','independent96_sealed_payloads':len(sealed),'independent96_receipt_sha256':sha(review/'independent-actual/RECEIPT.json'),'cores_supplement_sha256':sha(review/'independent-actual/CORE_DECOMPOSITION.json'),'originals':len(selection),'gzip':len(packing),'all_original_sha_length_mode_and_gzip_roundtrips':'PASS','host_archive_verifications':{h:load(base/'root'/(h.upper()+'_PAIRED_ARCHIVE_VERIFIED.json'))for h in ['linux','mac']},'decision':load(review/'FINAL_RETENTION_DECISION.json')};save(out/'ORIGINAL_CHECKS.json',checks)
(out/'README.md').write_text('# 487 current numerical wrap-up\n\nBoth original48-case parents actually ended0 before collection (Linux44616/8d63f2, Mac67726/148477). Frozen reader and independent96-case raw recomputation agree on original outcomes, timer boundaries/rates, nearest-rank latencies, refusals/backlog/groups, recovery/retries/joins, CPU/RSS and physical work. Every paired and per-group tail loss is retained.\n\nRetain486 only as a checked measured loaded CPU/work reduction: all12 loaded CPU/completed and matching-child-wall average-core pairs improve;11/12 loaded native and same-childwall rates improve. All11 native-rate-gain pairs using more CPU/original or more average cores are explicitly flagged and excluded from CPU-efficiency wins. Singleton/moderate/skew/cold-tail losses remain unresolved; no universal throughput, latency, fairness, default or fullP9 acceptance is claimed.\n\nCPU/completed has a wider whole-child numerator. Average busy cores uses matching child-wrapper wall, with exact same-boundary decomposition in CORE_DECOMPOSITION. Native throughput has a different timer; no causal core-scaling claim follows. Configured workers match, while fixed affinity/exclusive physical cores were not imposed. Peak RSS is not a debt plateau; physical elapsed overlaps and is not CPU; append units include metadata.\n\nSELECTION/PACKING preserve exact original paths/hashes/bytes/modes and reversible gzip. Both host seals remain unchanged and root verification receipts are included. No production source, binaries/build trees or generated WALs included. Current results only; user requested archive/commit/push then stop. Full P9 remains incomplete and paused at user request.\n')
files=sorted(p for p in out.rglob('*')if p.is_file());(out/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in files));receipt={**checks,'archive':str(out),'payloads':len(files),'manifest_sha256':sha(out/'SHA256SUMS')};save(review/'CENTRAL_ARCHIVE_RECEIPT.json',receipt);print(json.dumps({k:receipt[k]for k in ['exit','originals','gzip','payloads','manifest_sha256']},indent=2))
