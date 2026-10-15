"""Add actual completed packer streams/exit and seal the final evidence set."""
from pathlib import Path
import gzip,hashlib,json,shutil
job=Path(__file__).resolve().parent;out=Path('validation/performance/native478-ready/linux')
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
save=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
assert(job/'archive.exit').read_text().strip()=='0'
shutil.copy2(job/'ARCHIVE_RECEIPT.json',job/'PACK_RECEIPT.json')
selection=load(out/'SELECTION.json')
for p in [job/'archive.stdout',job/'archive.stderr',job/'archive.exit',job/'PACK_RECEIPT.json',Path(__file__)]:
 rel=str(Path('campaign-job')/p.name);target=out/rel;shutil.copy2(p,target)
 selection=[r for r in selection if r['archived_path']!=rel]
 selection.append({'source_path':str(p.resolve()),'archived_path':rel,'original_sha256':sha(p),'original_bytes':p.stat().st_size,'source_mode':oct(p.stat().st_mode&0o7777)})
save(out/'SELECTION.json',sorted(selection,key=lambda r:r['archived_path']))
p=out/'README.md';s=p.read_text()
for old,new in [('#478','# 478'),('processes,48','processes, 48'),('validators and48','validators and 48'),('joins.623','joins. 623'),('checkpoints,1869','checkpoints, 1869'),('4096.64','4096. 64'),('stable1.98.1','stable 1.98.1'),('unchanged476','unchanged 476'),('defaults.477','defaults. 477'),('sustained20M','sustained 20M'),('31.800ms','31.800 ms'),('26.944ms','26.944 ms'),('1240.678ms','1240.678 ms'),('1598.424ms','1598.424 ms')]:s=s.replace(old,new)
p.write_text(s)
for r in selection:
 p=out/r['archived_path'];b=gzip.decompress(p.read_bytes())if p.suffix=='.gz' else p.read_bytes()
 assert len(b)==r['original_bytes'] and hashlib.sha256(b).hexdigest()==r['original_sha256'],r['archived_path']
 assert sha(r['source_path'])==r['original_sha256'],r['source_path']
manifest=''.join(sha(p)+'  '+str(p.relative_to(out))+'\n'for p in sorted(out.rglob('*'))if p.is_file()and p.name!='SHA256SUMS')
(out/'SHA256SUMS').write_text(manifest)
receipt=load(job/'ARCHIVE_RECEIPT.json');receipt.update({'payloads':len(manifest.splitlines()),'manifest_sha256':sha(out/'SHA256SUMS'),'finalization':'Actual packer stdout/stderr/exit and immutable initial PACK_RECEIPT are included after original packer24561 actually terminated0; initial manifest hash remains in those original streams. README spacing corrected. Final manifest independently checks every selected original byte/hash and gzip roundtrip. No workload repeated.','archive_bytes':sum(p.stat().st_size for p in out.rglob('*')if p.is_file())})
save(job/'ARCHIVE_RECEIPT.json',receipt)
print(json.dumps(receipt,indent=2))
