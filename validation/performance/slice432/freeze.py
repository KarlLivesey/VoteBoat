from pathlib import Path
import json,hashlib,stat,difflib
J=Path(__file__).resolve().parent;P=J/'source';B=json.loads((J/'SOURCE_BEFORE.json').read_text());orig=B['published_inventory'];sha={};modes={};changed=[];patch=''
for p in sorted(P.rglob('*')):
 if not p.is_file():continue
 name=str(p.relative_to(P));sha[name]=hashlib.sha256(p.read_bytes()).hexdigest();modes[name]=oct(stat.S_IMODE(p.stat().st_mode))
 if orig['sha256'].get(name)==sha[name]:continue
 changed.append(name);before=(Path(orig['source_root'])/name).read_text() if name in orig['sha256'] else ''
 patch+=''.join(difflib.unified_diff(before.splitlines(True),p.read_text().splitlines(True),fromfile='a/'+name if before else '/dev/null',tofile='b/'+name))
(J/'PATCH.diff').write_text(patch);h=hashlib.sha256(patch.encode()).hexdigest();(J/'SOURCE.json').write_text(json.dumps({'base_commit':orig['commit'],'source_files':len(sha),'new_files':sorted(set(sha)-set(orig['sha256'])),'changed_files':changed,'sha256':sha,'modes':modes,'patch_sha256':h,'scope':'Isolated432 fieldwise inline clone; root original payload Clone unchanged; no performance artifact/timing or main production edits'},indent=2)+'\n');(J/'source.sha256').write_text(''.join(f'{h}  {n}\n' for n,h in sha.items()));print(len(sha),changed,h)
