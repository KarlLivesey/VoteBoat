"""Read-only independent archive/original verification after both482 terminals."""
from pathlib import Path
import gzip,hashlib,json,stat

base=Path('target/p9/native482-current-profile')
archive_base=Path('validation/performance/native482-current-profile')
sha=lambda data:hashlib.sha256(data).hexdigest()
load=lambda p:json.loads(Path(p).read_text())
results={}
for host in ['linux','mac']:
 archive=archive_base/host
 manifest={line.split('  ',1)[1]:line.split('  ',1)[0]for line in(archive/'SHA256SUMS').read_text().splitlines()}
 actual={p.relative_to(archive).as_posix():sha(p.read_bytes())for p in archive.rglob('*')if p.is_file()and p.name!='SHA256SUMS'}
 assert actual==manifest,(host,'recursive seal')
 packing=load(archive/'PACKING.json');packed={r['original_path']:r for r in packing}
 def original(name):
  r=packed.get(name)
  if r is None:return(archive/name).read_bytes()
  container=archive/r['archived_path'];content=gzip.decompress(container.read_bytes())
  assert len(content)==r['original_bytes']and sha(content)==r['original_sha256']
  if 'container_sha256'in r:assert sha(container.read_bytes())==r['container_sha256']and container.stat().st_size==r['container_bytes']
  return content
 for r in packing:original(r['original_path'])
 checked=0;mode_checked=0
 if host=='linux':
  selection=load(archive/'SELECTION.json')
  for r in selection:
   p=Path(r['source_path']);content=p.read_bytes()
   assert len(content)==r['original_bytes']and sha(content)==r['original_sha256']
   assert oct(stat.S_IMODE(p.stat().st_mode))==r['source_mode']
   name=next((r0['original_path']for r0 in packing if r0['archived_path']==r['archived_path']),r['archived_path'])
   assert original(name)==content
   checked+=1;mode_checked+=1
  perf=load(base/'linux/ARCHIVE_RECEIPT.json')['raw_perf_ignored_location']
  p=Path(perf['path']);assert p.stat().st_size==perf['bytes']and sha(p.read_bytes())==perf['sha256']
  raw_profile={'path':str(p),'sha256':perf['sha256'],'bytes':perf['bytes'],'retention':'Ignored raw perf.data preserved; archive binds identity and converted data, not raw binary perf.data.'}
 else:
  for relative in manifest:
   if relative in ['README.md','PACKING.json']:continue
   name=next((r['original_path']for r in packing if r['archived_path']==relative),relative)
   p=(base/name[len('shared/'):])if name.startswith('shared/')else(base/'mac'/name)
   assert original(name)==p.read_bytes(),name
   if name not in packed:assert stat.S_IMODE((archive/name).stat().st_mode)==stat.S_IMODE(p.stat().st_mode),name
   checked+=1
  collected=load(base/'mac/raw/COLLECTED.json')
  for name,r in collected['files'].items():
   p=base/'mac/raw'/name;content=original('raw/'+name)
   assert len(content)==r['bytes']and sha(content)==r['sha256']
   assert oct(stat.S_IMODE(p.stat().st_mode))==r['mode']
   mode_checked+=1
  p=base/'mac/raw/profile.json.gz';raw_profile={'path':str(p),'sha256':sha(p.read_bytes()),'bytes':p.stat().st_size,'retention':'Original saved JSON.gz preserved byte-for-byte in archive and ignored collection.'}
 results[host]={'payloads':len(manifest),'packed_originals':len(packing),'manifest_sha256':sha((archive/'SHA256SUMS').read_bytes()),'all_recursive_payload_hashes_exact':True,'all_packed_lengths_hashes_exact':True,'archived_originals_compared_to_ignored_files':checked,'recorded_source_or_transfer_modes_verified':mode_checked,'raw_profile':raw_profile}
proof={'exit':0,'scope':'Independent read-only comparison of sealed482 archives against original ignored files/mode inventories; no capture, rebuild, workload or source change. Generated README/PACKING/SELECTION metadata is sealed; original modes for packed payloads remain in transfer/selection inventories rather than encoded in gzip.', 'hosts':results,'originals':load(base/'ROOT_QUIET_END.json'),'independent_numerical_review_sha256':sha((base/'INDEPENDENT_PROFILE_REVIEW.json').read_bytes())}
(base/'ROOT_ARCHIVE_VERIFIED.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof,indent=2))
