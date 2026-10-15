"""Read-only independent exact480 restoration review; no tests or workloads."""
from pathlib import Path
import hashlib,io,json,stat,subprocess,tarfile
base='f7b0ef12709bb7e7aad4f0279851aa035b508e9e';rejected='efd383dc36b4b9edf8f66021b90115824440b468';out=Path('target/p9/native485-restore/review');sha=lambda x:hashlib.sha256(x).hexdigest()
def git(*args):return subprocess.check_output(['git',*args])
qualified=json.loads(Path('target/p9/native480-admission/linux/SOURCE.json').read_text());assert qualified['commit']==base
names=git('ls-tree','-r','--name-only',base).decode().splitlines()
root_files=['Cargo.toml','Cargo.lock','build.rs','clippy.toml','rust-toolchain.toml','LICENSE']
selected=[n for n in names if n.startswith(('src/','tests/','examples/','benchmarks/','.githooks/','.github/'))or n in root_files+['docs/component-contracts.json','docs/provider-conformance.json']or(n.startswith('validation/')and n.count('/')==1 and Path(n).suffix in ['.mjs','.js','.py','.sh','.rs'])]
assert set(qualified['sha256'])<=set(selected)|{'AGENTS.md'},set(qualified['sha256'])-set(selected)
selected=sorted(set(selected)|set(qualified['sha256']))
blob=git('archive','--format=tar',base,*selected)
expected={}
with tarfile.open(fileobj=io.BytesIO(blob))as archive:
 for member in archive:
  if member.isfile():expected[member.name]={'sha256':sha(archive.extractfile(member).read()),'git_archive_mode':oct(member.mode)}
assert set(expected)==set(selected)
proof={};failures=[]
for name,e in expected.items():
 p=Path(name);mode=oct(stat.S_IMODE(p.stat().st_mode))if p.exists()else None;expect_mode=qualified['physical_modes'].get(name,e['git_archive_mode']);expect_mode=oct(int(expect_mode,8))if isinstance(expect_mode,str)else oct(expect_mode)
 if name in qualified['sha256']:assert qualified['sha256'][name]==e['sha256']
 actual=sha(p.read_bytes())if p.exists()else None
 proof[name]={'expected_sha256':e['sha256'],'actual_sha256':actual,'expected_mode':expect_mode,'actual_mode':mode}
 if actual!=e['sha256']or mode!=expect_mode:failures.append(name)
root_restoration=json.loads(Path('target/p9/native485-restore/RESTORATION.json').read_text());removed=[r['path']for r in root_restoration['restored']if r['status']=='A'];assert len(removed)==8
for name in removed:
 assert name not in names and not Path(name).exists()
 assert git('cat-file','-e',rejected+':'+name)==b''
actual_deletions=git('diff','--name-only','--diff-filter=D','HEAD').decode().splitlines();assert set(actual_deletions)==set(removed)
changed=git('diff','--name-only','HEAD').decode().splitlines();experiment=git('diff','--name-only',base,rejected,'--','src','tests','examples','docs/component-contracts.json','docs/provider-conformance.json','validation/check-provider-conformance.test.mjs').decode().splitlines()
source_changes=[n for n in changed if n.startswith(('src/','tests/','examples/'))or n in ['docs/component-contracts.json','docs/provider-conformance.json','validation/check-provider-conformance.test.mjs']]
assert set(source_changes)==set(experiment),(set(source_changes)-set(experiment),set(experiment)-set(source_changes))
assert Path('tests/application/ready_admission.rs').exists()
schema=Path('validation/performance/native485-restore/SCHEMA.md');text=schema.read_text();assert all(s in text for s in ['1342.682','1499.466','854.009','809.077','all six','480','ext4/XFS','P9'])
manifest={'baseline':base,'rejected':rejected,'files':proof,'removed_rejected_only_modules':removed,'exact_unrelated_deletion_check':True,'qualified480_source_sha256':sha(Path('target/p9/native480-admission/linux/SOURCE.json').read_bytes()),'schema_sha256':sha(schema.read_bytes())};(out/'INPUT_PROOF.json').write_text(json.dumps(manifest,indent=2)+'\n')
result={'actual_exit':int(bool(failures)),'baseline':base,'rejected':rejected,'selected_files':len(proof),'qualified480_inputs':len(qualified['sha256']),'all_selected_bytes_modes_exact':not failures,'failures':failures,'removed_only_rejected_modules':removed,'changed_experiment_paths':source_changes,'unrelated_tracked_deletions':[],'current480_ready_oracle_retained':True,'schema_adequate':True,'schema_note':'Retains all6 Mac loaded paired gains and all6 Mac cold skew tail losses. Exact restoration/no migration, affected checks+enabled hooks, both-host raw evidence and fullP9 objective preserved. Validation checker expected-count restoration must be included among experiment inputs.','scope':'Independent read-only git480 archive bytes+qualified physical modes compared to current full source/tests/examples/benchmarks/root build/license/toolchain/hooks/direct code validators/two contract ledgers. Docs/evidence additions are excluded from rollback; no tests/builds/workloads or source edits by reviewer.','input_proof_sha256':sha((out/'INPUT_PROOF.json').read_bytes())};(out/'RESTORATION_REVIEW.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2));raise SystemExit(result['actual_exit'])
