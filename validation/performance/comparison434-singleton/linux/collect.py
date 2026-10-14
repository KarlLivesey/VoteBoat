from pathlib import Path
import hashlib,json,gzip,shutil
R=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat')
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
def collect(source,dest,excluded):
 assert not dest.exists(),str(dest);dest.mkdir(parents=True);containers=[]
 for p in sorted(source.rglob('*')):
  if not p.is_file() or any(x in p.relative_to(source).parts for x in excluded):continue
  assert not p.is_symlink(),str(p);q=dest/p.relative_to(source);q.parent.mkdir(parents=True,exist_ok=True)
  if p.stat().st_size>1024*1024 and p.suffix!='.gz':
   q=Path(str(q)+'.gz')
   with p.open('rb') as i,q.open('wb') as o,gzip.GzipFile(filename='',mode='wb',fileobj=o,mtime=0,compresslevel=5) as z:shutil.copyfileobj(i,z)
   h=hashlib.sha256()
   with gzip.open(q,'rb') as f:
    for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
   assert h.hexdigest()==sha(p);containers.append({'archive':str(q.relative_to(dest)),'original':str(p.relative_to(source)),'original_sha256':h.hexdigest(),'original_bytes':p.stat().st_size,'encoding':'gzip lossless'})
  else:shutil.copy2(p,q)
 (dest/'LOSSLESS_CONTAINERS.json').write_text(json.dumps(containers,indent=2)+'\n');files=sorted(p for p in dest.rglob('*') if p.is_file());assert not any(p.is_symlink() for p in dest.rglob('*'));(dest/'SHA256SUMS').write_text(''.join(sha(p)+'  '+str(p.relative_to(dest))+'\n' for p in files))
 for line in (dest/'SHA256SUMS').read_text().splitlines():
  h,n=line.split('  ',1);assert sha(dest/n)==h,n
 return {'archive':str(dest.relative_to(R)),'regular_files':len(files)+1,'bytes':sum(p.stat().st_size for p in dest.rglob('*') if p.is_file()),'all_hashes_verified':True,'lossless_containers':len(containers)}
p=R/'target/p9/comparison434-singleton';assert (p/'LINUX_STATUS').read_text().strip()=='0'
print(json.dumps(collect(p,R/'validation/performance/comparison434-singleton/linux',{'__pycache__'})))
