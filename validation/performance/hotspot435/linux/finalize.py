from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,gzip,shutil
P=Path(__file__).resolve().parent
R=Path('/home/karllivesey/Documents/ChatGPT/VoteBoat')
def read(p):return json.loads(p.read_text())
def write(p,d):p.write_text(json.dumps(d,indent=2)+'\n')
def sha(p):
 h=hashlib.sha256()
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
 return h.hexdigest()
readout={}
for cell in ['bulk','singleton']:
 d=P/'symbolication'/(cell+'-symbolicated');s=read(d/'SUMMARY.json');a=read(d/'SUBSETS.json');c=read(d/'CLIENT_CONTEXTS.json');v=read(P/'perf-quiet'/(cell+'.stdout'));n=s['denominator']
 readout[cell]={'selected_sample_weight':n,'unit':s['weight'],'totals':s['totals'],'threads':s['threads'],'physical_depth_sample_weights':s['physical_depth_sample_weights'],'physical_depth_le2_percent':100*sum(v for k,v in s['physical_depth_sample_weights'].items() if int(k)<=2)/n,'unresolved_leaf_percent':100*s['totals']['unresolved_leaf_sample_weight']/n,'any_unresolved_percent':100*s['totals']['any_unresolved_stack_sample_weight']/n,'top5_native_self':s['self_native'][:5],'top5_inline_self':s['self_including_inlines'][:5],'inclusive_contexts':a['subsets'],'entry_clone_top5_project_ancestries':a['entry_clone_top5_project_ancestries'],'client_contexts':c,'profiled_workload':{'completed_originals':v['completed_originals'],'completed_requests':v['completed_requests'],'originals_per_second':v['operations_per_second'],'p99_ms':v['request_p99_ns']/1e6,'applied_frontiers':v['applied_frontiers'],'not_unprofiled_capacity_or_acceptance':True},'libraries':s['libraries'],'unique_symbol_queries':s['unique_symbol_queries'],'limits':s['limits']+a['limitations']}
write(P/'symbolication/READOUT.json',readout)
A=R/'validation/performance/hotspot435/linux';assert not A.exists(),str(A);A.mkdir(parents=True)
containers=[]
for src in sorted(P.rglob('*')):
 if not src.is_file() or '__pycache__' in src.parts:continue
 assert not src.is_symlink(),str(src)
 rel=src.relative_to(P);dst=A/rel;dst.parent.mkdir(parents=True,exist_ok=True)
 compress=src.suffix=='.data' or src.stat().st_size>1024*1024 and src.suffix!='.gz'
 if compress:
  dst=Path(str(dst)+'.gz')
  with src.open('rb') as i,dst.open('wb') as o,gzip.GzipFile(filename='',mode='wb',fileobj=o,mtime=0,compresslevel=5) as z:shutil.copyfileobj(i,z)
  h=hashlib.sha256()
  with gzip.open(dst,'rb') as f:
   for b in iter(lambda:f.read(1024*1024),b''):h.update(b)
  assert h.hexdigest()==sha(src)
  with gzip.open(dst,'rb') as f: assert sum(len(b) for b in iter(lambda:f.read(1024*1024),b''))==src.stat().st_size
  containers.append({'archive':str(dst.relative_to(A)),'original':str(rel),'original_bytes':src.stat().st_size,'original_sha256':h.hexdigest(),'encoding':'gzip lossless','compressed_bytes':dst.stat().st_size,'compressed_sha256':sha(dst)})
 else:shutil.copy2(src,dst)
shutil.copytree(R/'validation/performance/matched435-linux',A/'artifact-identity')
for name in ['CONFIG_PAIR.linux.json','artifact-guard.py']:
 shutil.copy2(R/'target/p9/comparison435'/name,A/name)
write(A/'LOSSLESS_CONTAINERS.json',containers)
files=sorted(x for x in A.rglob('*') if x.is_file());assert not any(x.is_symlink() for x in A.rglob('*'))
(A/'SHA256SUMS').write_text(''.join(sha(x)+'  '+str(x.relative_to(A))+'\n' for x in files))
for line in (A/'SHA256SUMS').read_text().splitlines():
 expected,name=line.split('  ',1);assert sha(A/name)==expected,name
print(json.dumps({'archive':str(A),'regular_files':len(files)+1,'bytes':sum(x.stat().st_size for x in A.rglob('*') if x.is_file()),'all_hashes_verified':True,'lossless_containers':len(containers),'capture_terminal':0,'analysis_terminal':0}))
