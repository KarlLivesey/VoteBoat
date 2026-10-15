#!/usr/bin/env bash
# Called only after root supplies actual successful publication + qualification receipts.
set -euo pipefail
python3 - <<'PY'
from pathlib import Path
import hashlib,json,subprocess,tarfile
root=Path.cwd();job=root/'target/p9/matched468-linux';h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
commit=(job/'REQUIRED_COMMIT.txt').read_text().strip()
assert commit!='UNPUBLISHED_CANDIDATE_NOT_AUTHORIZED_FOR_TIMING', '468 candidate unpublished: export refused'
publication=json.loads((job/'PUBLICATION.json').read_text());assert publication['commit']==commit and publication['exit']==0
proof=publication['proof'];assert h(Path(proof['path']))==proof['sha256']
reuse=json.loads((job/'FUNCTIONAL_REUSE.json').read_text());assert reuse['source_commit']==commit
qualified=reuse['final_source_sha256'];assert qualified and reuse['matched_functional_reuse']['fmt_strict_sourceguards_all_exit0'] and reuse['root_functional_reuse']['all_unchanged_root_compiled_inputs_verified']
assert {'benchmarks/matched/src/voteboat_engine/wait_cache/tests.rs', 'src/log/command_bytes/tests.rs', 'src/format_crc.rs', 'benchmarks/matched/src/voteboat_engine/wait_cache.rs', 'src/format_crc/tests.rs', 'src/log/command_bytes.rs'}<=set(qualified), 'current immutable byte/cache source/controls absent from checked projection'
assert all(h(Path(p['path']))==p['sha256'] for p in reuse['proofs'])
tree=subprocess.check_output(['git','ls-tree','-r',commit],text=True)
tracked={}
for line in tree.splitlines():
 meta,name=line.split('\t',1);mode,kind,blob=meta.split();assert kind=='blob';tracked[name]=(mode,blob)
extras={'.gitignore','README.md','rust-toolchain.toml'}|{name for name in tracked if name.startswith('.cargo/')}
inputs=sorted(set(qualified)|extras)
assert set(inputs)<=set(tracked)
expected={name:hashlib.sha256(subprocess.check_output(['git','show',commit+':'+name])).hexdigest() for name in inputs}
assert all(expected[name]==digest for name,digest in qualified.items()), 'committed bytes differ from final checked projection'
physical={name:reuse['exact_physical_modes'][name] for name in qualified}
for name in extras-set(qualified):
 assert h(root/name)==expected[name], 'added configuration/current committed bytes differ'
 physical[name]=oct((root/name).stat().st_mode & 0o777)
assert not (job/'source').exists() and not (job/'target-matched-release').exists(), 'refusing existing exported source/target'
cmd=['git','archive','--format=tar','--output='+str(job/'source.tar'),commit,*inputs]
(job/'source-export.command.json').write_text(json.dumps(cmd,indent=2)+'\n');subprocess.run(cmd,check=True)
(job/'source').mkdir()
with tarfile.open(job/'source.tar') as archive:archive.extractall(job/'source',filter='data')
# Apply exact qualified permissions before the first inventory/guard; no later readonly chmod.
for name,mode in physical.items():(job/'source'/name).chmod(int(mode,8))
actual={p.relative_to(job/'source').as_posix() for p in (job/'source').rglob('*') if p.is_file()};assert actual==set(expected)
assert all(h(job/'source'/name)==digest for name,digest in expected.items())
baseline=json.loads((root/'target/p9/matched455-linux/SOURCE.json').read_text())
changed=sorted(name for name,digest in expected.items() if baseline['sha256'].get(name)!=digest)
supplemental=sorted(set(name for name in inputs if name.endswith('.md') or name in {'LICENSE','.gitignore'} or name.startswith('docs/') or name.startswith('.githooks/')))
source={'commit':commit,'source_root':str(job/'source'),'inputs':len(inputs),'compiled_inputs':len(inputs)-len(supplemental),'supplemental_inputs':supplemental,'changed_inputs':changed,'sha256':expected,'git_modes':{name:tracked[name][0] for name in inputs},'git_blob_ids':{name:tracked[name][1] for name in inputs},'physical_modes':physical,'archive_sha256':h(job/'source.tar'),'functional_projection_inputs':len(qualified),'added_artifact_configuration_inputs':sorted(set(inputs)-set(qualified)),'scope_note':'compiled_inputs preserves historical input projection label; Cargo JSON records prove actual compiled roots; added toolchain/config inputs captured without claiming extra functional test coverage'}
(job/'SOURCE.json').write_text(json.dumps(source,indent=2)+'\n')
(job/'source.sha256').write_text(''.join(digest+'  '+name+'\n' for name,digest in sorted(expected.items())))
(job/'slice468-source.sha256').write_text(''.join(expected[name]+'  '+name+'\n' for name in changed))
print(json.dumps({'source_inputs':len(inputs),'functional_projection':len(qualified),'added_build_configuration':source['added_artifact_configuration_inputs'],'changed_inputs':len(changed),'fresh_target_absent':True,'build_started':False}))
PY
