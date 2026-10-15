"""Separate484 numerical-review evidence, preserving both campaign archive seals."""
from pathlib import Path
import datetime,gzip,hashlib,json,shutil,sys
base=Path('target/p9/native484-ingress');review=base/'review';out=Path('validation/performance/native484-ingress/comparison/review')
load=lambda p:json.loads(Path(p).read_text());sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
assert len(sys.argv)==2,'Supply actual Mac root archive-verification receipt'
mac_verification=Path(sys.argv[1]);assert mac_verification.is_file();assert not out.exists()
linux_verification=review/'LINUX_PAIRED_ROOT_ARCHIVE_VERIFIED.json'
for p in [linux_verification,mac_verification]:assert load(p)['exit']==0
end=load(base/'ROOT_QUIET_END.json');assert all(end[h]['actual_terminal_exit']==0 for h in ['linux','mac'])
sealed=load(review/'independent-actual/FILES.json');assert len(sealed)==19
for name,item in sealed.items():
 p=review/'independent-actual'/name;assert sha(p)==item['sha256']and p.stat().st_size==item['bytes']
ind=load(review/'independent-actual/RECEIPT.json');assert ind['actual_exit']==0 and ind['cases']==96
assert sha(review/'raw_readout.py')=='02209e3306d1fc1c1685dec733f7633b146154834df2a414b768ada03baaa516'
selected={}
def add(p,rel):
 p=Path(p);rel=str(rel);assert p.is_file()and not p.is_symlink();assert rel not in selected;selected[rel]=p
for name in ['ROOT_QUIET_GO.json','ROOT_QUIET_END.json','ROOT_PUBLISHED_GO.json']:
 add(base/name,Path('authority')/name)
for host,job in [('linux',base/'comparison/linux/revision2/campaign'),('mac',base/'comparison/mac/collected')]:
 r=load(review/('actual484-'+host)/'READOUT.json');assert r['valid_cases']==48 and not r['failures']
 for p in(review/('actual484-'+host)).iterdir():
  if p.is_file():add(p,Path('frozen-readout')/host/p.name)
 for suffix in ['stdout','stderr','exit']:
  add(review/('actual484-'+host+'.'+suffix),Path('frozen-readout')/host/('readout.'+suffix))
 for name in ['STARTED.json','TERMINAL.json','CAMPAIGN_STATUS','CAMPAIGN_STAGE','READY.json','CAMPAIGN_CONFIG.json']:
  add(job/name,Path('original-campaign')/host/name)
 add(job/'campaign/CAMPAIGN.json',Path('original-campaign')/host/'CAMPAIGN.json')
 if host=='linux':
  for name in ['controller.stdout','controller.stderr','controller.exit']:
   add(job/name,Path('original-campaign')/host/name)
 else:
  add(job/'COLLECTED.json',Path('original-campaign')/host/'COLLECTED.json')
for p in(review/'independent-actual').iterdir():
 if p.is_file():add(p,Path('independent96')/p.name)
for name in ['raw_readout.py','PREPARED_READOUT.json','REFERENCE_VALIDATION.json','REFERENCE481_CONTEXT.json','actual484-mac.command.json','LINUX_ARCHIVE_RECEIPT.json','LINUX_ARCHIVE_PROOF.json','archive-linux.py','archive-linux.stdout','archive-linux.stderr','archive-linux.exit','verify-linux-paired-root.py','verify-mac-paired-root.py','archive-central.py','archive-central.command.json']:
 add(review/name,Path('review-provenance')/name)
add(linux_verification,'campaign-archive-verification/linux.json');add(mac_verification,'campaign-archive-verification/mac.json')
for p in(base/'mac-campaign-owner').iterdir():
 if p.is_file():add(p,Path('original-campaign/mac-outer')/p.name)
add(base/'comparison/mac/ARCHIVE_RECEIPT.json','campaign-archive-verification/mac-owner-archive-receipt.json')
for name in ['compact-attempt1-error.json','COMPACT_READOUT.json','LOCAL_TRANSFER_PROOF.json','collect.command.json','collect.stdout','collect.stderr','collect.exit','transfer-raw.command.json','transfer-raw.stderr','transfer-raw.exit']:
 add(base/'comparison/mac'/name,Path('mac-owner-provenance')/name)
# The host manifests are references; do not modify either sealed host archive.
for host in ['linux','mac']:
 add(out.parent/host/'SHA256SUMS',Path('campaign-archive-verification')/(host+'-SHA256SUMS'))
 for p in(base/('publication-'+host)).iterdir():
  if p.is_file():add(p,Path('checked-publication')/host/p.name)
out.mkdir(parents=True);selection=[];packing=[]
for rel,p in sorted(selected.items()):
 b=p.read_bytes();assert not b.startswith((b'\x7fELF',b'!<arch>\n'))
 packed=(len(b)>65536 or p.suffix in ['.stdout','.stderr','.log'])and p.suffix!='.gz';saved=rel+('.gz'if packed else'');t=out/saved;t.parent.mkdir(parents=True,exist_ok=True)
 if packed:
  t.write_bytes(gzip.compress(b,compresslevel=9,mtime=0));assert gzip.decompress(t.read_bytes())==b;packing.append({'original_path':rel,'archived_path':saved,'original_bytes':len(b),'original_sha256':sha(p),'container_bytes':t.stat().st_size,'container_sha256':sha(t)})
 else:shutil.copy2(p,t);assert t.read_bytes()==b
 selection.append({'source_path':str(p),'archived_path':saved,'original_sha256':sha(p),'original_bytes':len(b),'source_mode':oct(p.stat().st_mode&0o7777)})
save(out/'SELECTION.json',selection);save(out/'PACKING.json',packing)
for item in selection:
 p=out/item['archived_path'];b=gzip.decompress(p.read_bytes())if p.suffix=='.gz'and not item['source_path'].endswith('.gz')else p.read_bytes();assert len(b)==item['original_bytes']and hashlib.sha256(b).hexdigest()==item['original_sha256']
checks={'scope':'Distinct central484 numerical reviews only; original Linux/Mac campaign seals unchanged. No workload/build/profile/validator rerun.','original_campaigns':end,'independent96_sealed_payloads':19,'independent96_files_manifest_sha256':sha(review/'independent-actual/FILES.json'),'independent96_receipt_sha256':sha(review/'independent-actual/RECEIPT.json'),'all_original_SHA_length_gzip_roundtrip':'PASS','source_modes_recorded':True,'campaign_archive_verifications':{h:load(p)for h,p in [('linux',linux_verification),('mac',mac_verification)]},'host_manifest_sha256':{h:sha(out.parent/h/'SHA256SUMS')for h in ['linux','mac']},'originals':len(selection),'gzip':len(packing),'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
save(out/'ORIGINAL_CHECKS.json',checks)
lines=['# 484 independent numerical comparison reviews','','Both original 48-case campaign parents actually terminated with exit 0 before readout: Linux84364/tool52f183 and Mac2749/tool1812fd. Both hosts retain all process/validator/readback outcomes. The two original campaign archives are sealed separately and remain unchanged. This central archive preserves both exact frozen READOUT/PROVENANCE pairs, the frozen reader, GO/END, terminal/command provenance, and the independent96-case raw recomputation.','','Independent recomputation matches all 96 native boundary counts/rates; nearest-rank intended/service quantiles for applied originals and dispatch quantiles for every offer; group counts/pending/backlog; recovery/retry/join; whole-child CPU/RSS and physical deltas. The full per-pair and per-group regression sets match the frozen reader. The independent19-file seal and its original initial Linux parser failure are preserved: the first parser expected semicolon-separated histogram values, whereas actual raw uses |. A focused parser correction succeeded without changing any data or rerunning workloads. The separate Mac compact-display helper field-name error is also retained.','','Results reject483 as a portable useful-throughput/fairness improvement. Linux loses completed throughput in 17/24 pairs and intended p99 in 16/24; all singleton TCP and moderate protocol pairs lose throughput. Mac one-group loaded throughput improves in all six pairs, but all six Mac skew cold tails worsen. Every measured success and regression remains available; no prior481 score is spliced into484. The exact480 restoration is a separate source slice.','','| Host | Cell | Protocol | Baseline completed/s | Candidate completed/s | Baseline p99 ms | Candidate p99 ms |','| --- | --- | --- | ---: | ---: | ---: | ---: |']
for host in ['linux','mac']:
 r=load(review/('actual484-'+host)/'READOUT.json')
 for cell in ['singleton','moderate','loaded','skew']:
  for protocol in ['tcp','quic']:
   a,b=[next(x for x in r['cells']if(x['cell'],x['protocol'],x['arm'])==(cell,protocol,arm))for arm in ['baseline','candidate']]
   lines.append(f"| {host} | {cell} | {protocol} | {a['rates']['completed_ops_s']['median']:.3f} | {b['rates']['completed_ops_s']['median']:.3f} | {a['quantiles']['intended']['p99_ms']['median']:.3f} | {b['quantiles']['intended']['p99_ms']['median']:.3f} |")
lines+=['','Three-trial medians, with all trial values/ranges/pairs retained in JSON. These finite offered-cap/window cases do not prove maximum sustained capacity, causal tail attribution, complete fairness, the framework floors or fullP9 acceptance. CPU/RSS span the entire child, including startup/warmup/verification/recovery/cleanup; CPU per completed original is broader than the native timer and is not stage attribution. Peak RSS is not a debt plateau. Storage/publication elapsed is nested and cannot be summed as CPU; append units include metadata and are not literal proposal width. Encoded bytes and block counts are not device-I/O bytes. Three replicas share one physical volume per host.','','SELECTION records exact original paths/SHA256/bytes/modes. PACKING records reversible lossless gzip; ORIGINAL_CHECKS records original roundtrips, both root host-archive verifications and referenced host seal hashes. SHA256SUMS seals all central payloads. Raw logs are gzip-packed even when small to preserve whitespace. No binaries, build trees, full production source or generated WAL states are included. P9 remains active with its full acceptance objective.']
(out/'README.md').write_text('\n'.join(lines)+'\n')
files=sorted(p for p in out.rglob('*')if p.is_file());(out/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in files))
receipt={**checks,'archive':str(out),'payloads':len(files),'archive_bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file()),'manifest_sha256':sha(out/'SHA256SUMS')};save(review/'CENTRAL_ARCHIVE_RECEIPT.json',receipt);print(json.dumps(receipt,indent=2))
