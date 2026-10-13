from pathlib import Path
import hashlib,shutil,json
j=Path(__file__).resolve().parent
root=j.parents[2]
out=root/'validation/performance/slice428/isolated'
out.mkdir(parents=True,exist_ok=True)
for p in sorted(j.iterdir()):
 if p.is_file():shutil.copy2(p,out/p.name)
 elif p.name.startswith('attempt'):
  for f in sorted(p.rglob('*')):
   if f.is_file():
    d=out/f.relative_to(j);d.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(f,d)
assert not any(p.is_symlink() for p in out.rglob('*'))
paths=[p for p in sorted(out.rglob('*')) if p.is_file() and p.name!='EVIDENCE.sha256']
(out/'EVIDENCE.sha256').write_text(''.join(f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.relative_to(out).as_posix()}\n' for p in paths))
print('archived',len(paths),'regular evidence files to',out)
