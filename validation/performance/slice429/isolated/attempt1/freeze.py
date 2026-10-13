import difflib,hashlib,json,subprocess
from pathlib import Path
j=Path(__file__).resolve().parent;s=j/'source';base=json.loads((j/'BASE_SOURCE.json').read_text());sha={p.relative_to(s).as_posix():hashlib.sha256(p.read_bytes()).hexdigest() for p in s.rglob('*') if p.is_file()};new=sorted(set(sha)-set(base['sha256']));changed=sorted(p for p,h in sha.items() if base['sha256'].get(p)!=h)
assert set(base['sha256'])<=set(sha)
assert new==['tests/provider_conformance/log_store/entry_equality.rs']
assert changed==['src/log.rs','tests/provider_conformance/log_store.rs','tests/provider_conformance/log_store/entry_equality.rs']
patch=[]
for p in changed:
 old=subprocess.check_output(['git','show',base['commit']+':'+p],text=True) if p in base['sha256'] else ''
 patch.extend(difflib.unified_diff(old.splitlines(keepends=True),(s/p).read_text().splitlines(keepends=True),fromfile='a/'+p if old else '/dev/null',tofile='b/'+p))
(j/'PATCH.diff').write_text(''.join(patch));(j/'source.sha256').write_text(''.join(f'{h}  {p}\n' for p,h in sorted(sha.items())));(j/'overlay.sha256').write_text(''.join(f'{sha[p]}  {p}\n' for p in changed));(j/'SOURCE.json').write_text(json.dumps({'base_commit':base['commit'],'source_files':len(sha),'new_files':new,'changed_files':changed,'sha256':sha,'patch_sha256':hashlib.sha256((j/'PATCH.diff').read_bytes()).hexdigest()},indent=2)+'\n')
print('frozen',len(sha),'inputs;',len(changed),'changed; patch',hashlib.sha256((j/'PATCH.diff').read_bytes()).hexdigest())
