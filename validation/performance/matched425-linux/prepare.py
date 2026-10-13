import sys,re,json,subprocess,hashlib,tarfile,io
from pathlib import Path
# Explicit parent release supplies this commit; this script does not infer publication.
assert len(sys.argv)==2 and re.fullmatch('[0-9a-f]{40}',sys.argv[1]),'parent released full425 commit required'
commit=sys.argv[1];assert subprocess.check_output(['git','rev-parse',commit],text=True).strip()==commit
job=Path(__file__).resolve().parent;repo=job.parents[2];base=repo/'target/p9/matched424-linux/SOURCE.json';old=json.loads(base.read_text());approved=json.loads((job/'approved-six.json').read_text());assert len(approved['files'])==6
compiled=set(old['sha256'])-set(old['supplemental_inputs']);compiled|={'benchmarks/matched/src/attribution/gaps.rs','benchmarks/matched/src/attribution/gaps/tests.rs'};assert len(compiled)==1406
supplemental=set(old['supplemental_inputs']);paths=sorted(compiled|supplemental);source=job/'source';source.mkdir(exist_ok=False)
archive=subprocess.check_output(['git','archive',commit,*paths]);tarfile.open(fileobj=io.BytesIO(archive)).extractall(source,filter='data');sha={name:hashlib.sha256((source/name).read_bytes()).hexdigest()for name in paths}
for name,digest in approved['files'].items():assert sha[name]==digest,name
for lock in ['Cargo.lock','benchmarks/matched/Cargo.lock','benchmarks/matched/Cargo.toml','clippy.toml']:assert sha[lock]==old['sha256'][lock],lock
blobs={}
for line in subprocess.check_output(['git','ls-tree','-r',commit],text=True).splitlines():
 meta,name=line.split('\t',1)
 if name in sha:blobs[name]=meta.split()[2]
assert len(blobs)==len(paths)
m={'commit':commit,'inputs':len(paths),'compiled_inputs':len(compiled),'supplemental_inputs':sorted(supplemental),'source_root':str(source),'scope':'freshLinux425 release only;1406compiledGitinputs plus3supplemental; no tests/timing','previous_inventory_sha256':hashlib.sha256(base.read_bytes()).hexdigest(),'approved_patch_sha256':approved['patch_sha256'],'sha256':sha,'git_blob_ids':blobs}
(job/'SOURCE.json').write_text(json.dumps(m,indent=2)+'\n');(job/'source.sha256').write_text(''.join(f'{digest}  {name}\n'for name,digest in sorted(sha.items())));(job/'slice425-source.sha256').write_text(''.join(f'{digest}  {name}\n'for name,digest in sorted(approved['files'].items())))
print(f'exact{commit}: {len(paths)} frozen inputs, six approved gap paths; ready to build')
