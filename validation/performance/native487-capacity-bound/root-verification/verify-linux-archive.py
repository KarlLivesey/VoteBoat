from pathlib import Path
import gzip,hashlib,json
root=Path.cwd();archive=root/'validation/performance/native487-capacity-bound/linux';manifest=archive/'SHA256SUMS';h=lambda data:hashlib.sha256(data).hexdigest();entries={}
for line in manifest.read_text().splitlines():
 digest,relative=line.split('  ',1);assert relative not in entries;entries[relative]=digest
actual={p.relative_to(archive).as_posix() for p in archive.rglob('*')if p.is_file()and p!=manifest};assert actual==set(entries)
for name,digest in entries.items():assert h((archive/name).read_bytes())==digest,name
packing={x['archived_path']:x for x in json.loads((archive/'PACKING.json').read_text())};selection=json.loads((archive/'SELECTION.json').read_text())
for row in selection:
 p=archive/row['archived_path'];raw=p.read_bytes();data=gzip.decompress(raw)if row['archived_path']in packing else raw
 assert h(data)==row['original_sha256']and len(data)==row['original_bytes']
 source=Path(row['source_path']);assert source.read_bytes()==data and oct(source.stat().st_mode&0o7777)==row['source_mode']
 if row['archived_path']in packing:
  pack=packing[row['archived_path']];assert h(raw)==pack['container_sha256']and len(raw)==pack['container_bytes']
assert len(entries)==224 and len(selection)==220 and len(packing)==82
report={'exit':0,'payloads':len(entries),'originals':len(selection),'lossless_gzip':len(packing),'manifest_sha256':h(manifest.read_bytes()),'scope':'Independent root recursive exact path-set/seal/gzip/original byte/hash/length/mode comparison; actual artifact review is separately sealed in archive. No workload, check suite or build repeated.'};(root/'target/p9/native486-capacity-bound/linux/ROOT_ARCHIVE_VERIFIED.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report))
