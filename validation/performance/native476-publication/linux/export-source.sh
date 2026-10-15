#!/usr/bin/env bash
# Called only after root supplies actual successful publication + qualification receipts.
set -euo pipefail
python3 - <<'PY'
from pathlib import Path
import hashlib,json,subprocess,tarfile
root=Path.cwd();job=root/'target/p9/native476-linux';h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
subprocess.run(['python3',str(job/'guard.py'),str(job),'qualification'],check=True)
commit=(job/'REQUIRED_COMMIT.txt').read_text().strip()
q=json.loads((job/'FUNCTIONAL_QUALIFICATION.json').read_text())
inventory=json.loads(Path(q['source_inventory']['path']).read_text())['files']
qualified={name:item['sha256'] for name,item in inventory.items()}
tree=subprocess.check_output(['git','ls-tree','-r',commit],text=True)
tracked={}
for line in tree.splitlines():
 meta,name=line.split('\t',1);mode,kind,blob=meta.split();assert kind=='blob';tracked[name]=(mode,blob)
extras={'.gitignore','README.md','rust-toolchain.toml','LICENSE','AGENTS.md','Cargo.toml','Cargo.lock','clippy.toml','build.rs','docs/COMMAND_BYTES.md','docs/PERFORMANCE.md','docs/component-contracts.md','docs/component-contracts.json','docs/provider-conformance.json','validation/performance/native476-publication/SCHEMA.md','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock','benchmarks/matched/README.md','benchmarks/matched/SCHEMA.md','benchmarks/matched/COMPARISON.md'}|{name for name in tracked if name.startswith(('.cargo/','.githooks/','src/','tests/','examples/')) or (name.startswith('validation/') and name.count('/')==1 and name.endswith('.mjs'))}
extras={name for name in extras if name in tracked}
inputs=sorted(set(qualified)|extras)
assert set(inputs)<=set(tracked)
assert {n for n in tracked if n.startswith(('src/','tests/','examples/','.cargo/'))}<=set(inputs)
expected={name:hashlib.sha256(subprocess.check_output(['git','show',commit+':'+name])).hexdigest() for name in inputs}
assert all(expected[name]==digest for name,digest in qualified.items()), 'committed bytes differ from final checked projection'
physical={name:inventory[name]['mode'] for name in qualified}
for name in extras-set(qualified):
 assert h(root/name)==expected[name], 'added configuration/current committed bytes differ'
 physical[name]=oct((root/name).stat().st_mode & 0o777)
assert not (job/'source').exists() and not (job/'target-native-release').exists(), 'refusing existing exported source/target'
cmd=['git','archive','--format=tar','--output='+str(job/'source.tar'),commit,*inputs]
(job/'source-export.command.json').write_text(json.dumps(cmd,indent=2)+'\n');subprocess.run(cmd,check=True)
(job/'source').mkdir()
with tarfile.open(job/'source.tar') as archive:archive.extractall(job/'source',filter='data')
# Preserve observed directory permissions before first source freeze, and qualified exact file modes.
directories={'.'}
for name in inputs:
 directories.update(p.as_posix() for p in Path(name).parents if p.as_posix()!='.')
directory_modes={name:oct((root/name).stat().st_mode&0o777) for name in sorted(directories)}
for name,mode in directory_modes.items():(job/'source'/name).chmod(int(mode,8))
for name,mode in physical.items():(job/'source'/name).chmod(int(mode,8))
actual={p.relative_to(job/'source').as_posix() for p in (job/'source').rglob('*') if p.is_file()};assert actual==set(expected)
assert all(h(job/'source'/name)==digest for name,digest in expected.items())
baseline=json.loads((root/'target/p9/native475-linux/SOURCE.json').read_text())
changed=sorted(name for name,digest in expected.items() if baseline['sha256'].get(name)!=digest)
supplemental=sorted(set(name for name in inputs if name.endswith('.md') or name in {'LICENSE','.gitignore'} or name.startswith('docs/') or name.startswith('.githooks/')))
source={'commit':commit,'source_root':str(job/'source'),'inputs':len(inputs),'compiled_inputs':len(inputs)-len(supplemental),'supplemental_inputs':supplemental,'changed_inputs':changed,'sha256':expected,'git_modes':{name:tracked[name][0] for name in inputs},'git_blob_ids':{name:tracked[name][1] for name in inputs},'physical_modes':physical,'physical_directory_modes':directory_modes,'archive_sha256':h(job/'source.tar'),'functional_projection_inputs':len(qualified),'added_artifact_configuration_inputs':sorted(set(inputs)-set(qualified)),'scope_note':'compiled_inputs preserves historical input projection label; Cargo JSON records prove actual compiled roots; added toolchain/config inputs captured without claiming extra functional test coverage'}
(job/'SOURCE.json').write_text(json.dumps(source,indent=2)+'\n')
(job/'source.sha256').write_text(''.join(digest+'  '+name+'\n' for name,digest in sorted(expected.items())))
(job/'slice476-source.sha256').write_text(''.join(expected[name]+'  '+name+'\n' for name in changed))
print(json.dumps({'source_inputs':len(inputs),'functional_projection':len(qualified),'added_build_configuration':source['added_artifact_configuration_inputs'],'changed_inputs':len(changed),'fresh_target_absent':True,'build_started':False}))
PY
