from pathlib import Path
import json,gzip,hashlib,shutil
root=Path.cwd();source=root/'target/p9/native486-capacity-bound';out=root/'validation/performance/native486-capacity-bound/main';assert not out.exists();out.mkdir(parents=True)
h=lambda b:hashlib.sha256(b).hexdigest();selection=[];packing=[]
for p in sorted(source.rglob('*')):
 if not p.is_file():continue
 if p.relative_to(source).parts[0]not in {'checked','review','SCHEMA.md','run.py','ORIGINALS.json','SOURCE_DIFF.patch','NEW_TEST_MODULE.rs','CHECKED.json','archive.py'}:continue
 data=p.read_bytes();relative=p.relative_to(source).as_posix();name=relative;packed=len(data)>8192 or p.suffix in {'.stdout','.stderr','.patch'}
 if packed:name+='.gz'
 dest=out/name;dest.parent.mkdir(parents=True,exist_ok=True);blob=gzip.compress(data,mtime=0)if packed else data;dest.write_bytes(blob)
 row={'source_path':str(p),'archived_path':name,'original_sha256':h(data),'original_bytes':len(data),'source_mode':oct(p.stat().st_mode&0o7777)};selection.append(row)
 if packed:packing.append({**row,'container_sha256':h(blob),'container_bytes':len(blob)})
 assert (gzip.decompress(blob)if packed else blob)==data
(out/'SELECTION.json').write_text(json.dumps(selection,indent=2)+'\n');(out/'PACKING.json').write_text(json.dumps(packing,indent=2)+'\n')
manifest=out/'SHA256SUMS';payloads=sorted(p for p in out.rglob('*')if p.is_file());manifest.write_text(''.join(h(p.read_bytes())+'  '+p.relative_to(out).as_posix()+'\n'for p in payloads));print(json.dumps({'exit':0,'payloads':len(payloads),'originals':len(selection),'lossless_gzip':len(packing),'manifest_sha256':h(manifest.read_bytes())}))
