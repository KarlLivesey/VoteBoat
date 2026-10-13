import hashlib, io, json, subprocess, sys, tarfile
from pathlib import Path
commit=(Path(__file__).resolve().parent/'REQUIRED_COMMIT').read_text().strip()
assert len(commit)==40, 'Refusing an unpublished/unset430 candidate'
assert sys.argv==[sys.argv[0],commit]
assert subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip()==commit
assert subprocess.check_output(['git','rev-parse','refs/remotes/origin/main'],text=True).strip()==commit
job=Path(__file__).resolve().parent
tracked={}
for line in subprocess.check_output(['git','ls-tree','-r',commit],text=True).splitlines():
 meta,path=line.split('\t',1);mode,kind,blob=meta.split();tracked[path]={'mode':mode,'kind':kind,'blob':blob}
compiled={p for p in tracked if p.startswith(('src/','tests/','examples/','benchmarks/matched/src/','.githooks/','.cargo/')) or p in ['Cargo.toml','Cargo.lock','clippy.toml','LICENSE','build.rs','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock','benchmarks/matched/build.rs']}
supplemental={'benchmarks/matched/README.md','benchmarks/matched/SCHEMA.md','benchmarks/matched/COMPARISON.md','docs/component-contracts.json','docs/provider-conformance.json','AGENTS.md'}
paths=sorted(compiled|supplemental)
assert all(tracked[p]['kind']=='blob' for p in paths)
source=job/'source';source.mkdir(exist_ok=True)
archive=subprocess.check_output(['git','archive',commit,*paths]);tarfile.open(fileobj=io.BytesIO(archive)).extractall(source,filter='data')
sha={p:hashlib.sha256((source/p).read_bytes()).hexdigest() for p in paths}
for p in paths:
 assert hashlib.sha256(subprocess.check_output(['git','show',commit+':'+p])).hexdigest()==sha[p],p
 assert (bool((source/p).stat().st_mode & 0o111))==(tracked[p]['mode']=='100755'),p
frozen=json.loads(Path('target/p9/slice430/SOURCE.json').read_text())
changed=set(frozen['changed_files']);assert len(changed)==3 and 'benchmarks/matched/src/voteboat_engine/actor/tests/replies.rs' in changed
assert all(sha[p]==frozen['sha256'][p] for p in paths)
m={'commit':commit,'inputs':len(paths),'compiled_inputs':len(compiled),'supplemental_inputs':sorted(supplemental),'source_root':str(source),'scope':'fresh published Linux430 artifacts only; small functional CLI controls after build, no timed capacity campaign','sha256':sha,'git_blob_ids':{p:tracked[p]['blob'] for p in paths},'git_modes':{p:tracked[p]['mode'] for p in paths},'archive_sha256':hashlib.sha256(archive).hexdigest(),'changed_inputs':sorted(changed)}
(job/'SOURCE.json').write_text(json.dumps(m,indent=2)+'\n');(job/'source.sha256').write_text(''.join(f'{v}  {p}\n' for p,v in sorted(sha.items())));(job/'slice430-source.sha256').write_text(''.join(f'{sha[p]}  {p}\n' for p in sorted(changed)))
print(f'{commit}: {len(paths)} exact inputs; {len(changed)} changed430 inputs; Git modes preserved')
