from pathlib import Path
import json,hashlib,stat,difflib
J=Path(__file__).resolve().parent;P=J/'source';B=json.loads((J/'SOURCE_BEFORE.json').read_text());R=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat');sha={};modes={};changed=[];patch=''
for p in sorted(P.rglob('*')):
 if not p.is_file():continue
 name=str(p.relative_to(P));sha[name]=hashlib.sha256(p.read_bytes()).hexdigest();modes[name]=oct(stat.S_IMODE(p.stat().st_mode))
 if B['sha256'].get(name)==sha[name]:continue
 changed.append(name);before=(R/name).read_text() if name in B['sha256'] else ''
 patch+=''.join(difflib.unified_diff(before.splitlines(True),p.read_text().splitlines(True),fromfile='a/'+name if before else '/dev/null',tofile='b/'+name))
root={k:h for k,h in B['sha256'].items() if not k.startswith('benchmarks/matched/')};assert all(sha[k]==h and modes[k]==B['modes'][k] for k,h in root.items())
assert all(hashlib.sha256((R/k).read_bytes()).hexdigest()==h and oct(stat.S_IMODE((R/k).stat().st_mode))==B['modes'][k] for k,h in B['sha256'].items())
(J/'PATCH.diff').write_text(patch);h=hashlib.sha256(patch.encode()).hexdigest();(J/'SOURCE.json').write_text(json.dumps({'base_commit':B['base_commit'],'source_files':len(sha),'new_files':sorted(set(sha)-set(B['sha256'])),'changed_files':changed,'sha256':sha,'modes':modes,'patch_sha256':h,'scope':'Private434 flat request only; root unchanged, no performance artifact/timing or main edits'},indent=2)+'\n');(J/'source.sha256').write_text(''.join(f'{h}  {n}\n' for n,h in sha.items()));(J/'ROOT_REUSE.json').write_text(json.dumps({'baseline':B['base_commit'],'unchanged_root_inputs':len(root),'all_root_hashes_and_modes_equal':True,'main_all_inputs_hashes_and_modes_unchanged':True,'scope':'No unchangedwhole Root tests rerun; prior433/431 production qualification by exactsource identity; standalone changed source recompiled using terminalcached dependencies; freshperformanceproof notclaimed','root_sha256':root},indent=2)+'\n');print(len(sha),changed,h)
