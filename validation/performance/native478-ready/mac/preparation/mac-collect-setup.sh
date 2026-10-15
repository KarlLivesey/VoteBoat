#!/usr/bin/env bash
set -euo pipefail
/Applications/Xcode-beta.app/Contents/Developer/Library/Frameworks/Python3.framework/Versions/3.9/bin/python3.9 - <<'PY'
from pathlib import Path
import hashlib,io,json,sys,tarfile
base=Path('/private/tmp/voteboat-native478-esZLG7');selected=[]
for p in sorted(base.rglob('*')):
 if not p.is_file():continue
 rel=p.relative_to(base)
 if '__pycache__' in rel.parts:continue
 if rel.parts[0]=='wal':continue
 if len(rel.parts)>=3 and rel.parts[0]=='controls' and rel.parts[1]in ['tcp-wal','quic-wal'] and len(rel.parts)>3:continue
 selected.append(p)
assert len(selected)>=70,len(selected)
manifest={'remote_base':str(base),'files':{p.relative_to(base).as_posix():{'sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'bytes':p.stat().st_size,'mode':oct(p.stat().st_mode&0o777)}for p in selected},'scope':'Original Mac478 preparation and two functional control text receipts only. Excludes generated WAL states, binaries, source and builds.'}
with tarfile.open(fileobj=sys.stdout.buffer,mode='w|')as tar:
 for p in selected:tar.add(p,arcname=p.relative_to(base).as_posix(),recursive=False)
 data=(json.dumps(manifest,indent=2)+'\n').encode();item=tarfile.TarInfo('COLLECTION.json');item.size=len(data);item.mode=0o644;tar.addfile(item,io.BytesIO(data))
PY
