import difflib, hashlib, json, shutil, subprocess
from pathlib import Path
job=Path(__file__).resolve().parent
root=job.parents[2]
base=json.loads((root/'target/p9/matched430-linux/SOURCE.json').read_text())
commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
paths=sorted(set(base['sha256'])|{'src/raft/owned_append_tests.rs'})
changed=['src/raft/membership_tests.rs','src/raft/owned_append_tests.rs','src/raft/receive.rs']
sha={p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in paths}
actual=sorted(p for p in paths if sha[p]!=base['sha256'].get(p))
assert actual==changed, actual
source=job/'source'; source.mkdir()
for p in paths:
 dst=source/p; dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(root/p,dst)
 assert hashlib.sha256(dst.read_bytes()).hexdigest()==sha[p]
patch=[]
for p in changed:
 old=subprocess.check_output(['git','show',commit+':'+p],cwd=root,text=True) if p in base['sha256'] else ''
 patch.extend(difflib.unified_diff(old.splitlines(keepends=True),(root/p).read_text().splitlines(keepends=True),fromfile='a/'+p if old else '/dev/null',tofile='b/'+p))
(job/'PATCH.diff').write_text(''.join(patch))
m={'base_commit':commit,'source_files':len(paths),'new_files':['src/raft/owned_append_tests.rs'],'changed_files':changed,'sha256':sha,'modes':{p:oct((root/p).stat().st_mode&0o777) for p in paths},'patch_sha256':hashlib.sha256((job/'PATCH.diff').read_bytes()).hexdigest(),'scope':'final formatted main431 copied into isolated source; reused terminal429 check target dependency cache only, never performance artifacts; current source root recompiled'}
(job/'SOURCE.json').write_text(json.dumps(m,indent=2)+'\n')
(job/'source.sha256').write_text(''.join(f'{v}  {p}\n' for p,v in sorted(sha.items())))
print(len(paths),'exact inputs',len(changed),'changed',m['patch_sha256'])
