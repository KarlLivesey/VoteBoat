"""Archive only a published committed candidate; never contacts Mac or builds."""
import hashlib
import io
import json
import re
import subprocess
import tarfile
import stat
from pathlib import Path

here = Path(__file__).resolve().parent
commit = (here / 'REQUIRED_COMMIT').read_text().strip()
if not re.fullmatch('[0-9a-f]{40}', commit):
    raise SystemExit('unpublished candidate refused: REQUIRED_COMMIT is empty/invalid')
subprocess.run(['git', 'merge-base', '--is-ancestor', commit, 'refs/remotes/origin/main'], check=True)
qualified=json.loads((here/'ROOT_FINAL_INPUTS.json').read_text())
assert qualified['published_commit']==commit
assert qualified['receipts'], 'actual final root/publication receipts required'
for receipt in qualified['receipts']:
    assert receipt['exit']==0
    path=Path(receipt['path'])
    assert path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest()==receipt['sha256'],str(path)

selectors = ['Cargo.toml', 'Cargo.lock', 'build.rs', 'clippy.toml', 'rust-toolchain.toml',
             'LICENSE', 'AGENTS.md', 'README.md', '.gitignore', '.githooks',
             'src', 'tests', 'examples', 'benchmarks/matched/Cargo.toml',
             'benchmarks/matched/Cargo.lock', 'benchmarks/matched/src',
             'benchmarks/matched/README.md', 'benchmarks/matched/SCHEMA.md',
             'benchmarks/matched/COMPARISON.md', 'docs/component-contracts.json',
             'docs/provider-conformance.json', 'docs/COMMAND_BYTES.md', 'docs/PERFORMANCE.md',
             'validation']
optional_cargo_config_present = bool(subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', commit, '.cargo'], text=True).strip())
if optional_cargo_config_present:
    selectors.append('.cargo')
for name in ['source.tar', 'SOURCE.json', 'source.sha256']:
    if (here / name).exists():
        raise SystemExit('refuse replacing existing committed archive: ' + name)
selectors.remove('validation')
selectors.extend(subprocess.check_output(['git','ls-tree','-r','--name-only',commit,'validation'],text=True).splitlines())
selectors=[p for p in selectors if not p.startswith('validation/') or ('/' not in p[len('validation/'): ] and p.endswith('.mjs'))]
raw = subprocess.check_output(['git', 'archive', '--format=tar', commit, *selectors])
committed_archive_modes = {}
protected_fixture_keys = []
original_raw = raw
with tarfile.open(fileobj=io.BytesIO(original_raw)) as original:
    protected = io.BytesIO()
    with tarfile.open(fileobj=protected, mode='w') as destination:
        for member in original:
            committed_archive_modes[member.name] = oct(member.mode)
            data = original.extractfile(member) if member.isfile() else None
            if member.isfile() or member.isdir():
                member.mode = stat.S_IMODE(Path(member.name).stat().st_mode)
            if member.isfile() and member.name.startswith('tests/fixtures/') and member.name.endswith('-key.der'):
                member.mode = 0o600
                protected_fixture_keys.append(member.name)
            destination.addfile(member, data)
    raw = protected.getvalue()
assert len(protected_fixture_keys) == 7
hashes = {}
modes = {}
directory_modes = {}
with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
    for member in archive:
        if member.isdir():
            directory_modes[member.name.rstrip("/")] = oct(member.mode)
            continue
        if not member.isfile():
            raise SystemExit('unsupported nonregular source: ' + member.name)
        hashes[member.name] = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
        modes[member.name] = oct(member.mode)
for required in ['src/runtime/clients/ingress.rs','tests/client_admission/ingress.rs','tests/support/node_ingress.rs','tests/startup/multi/ingress.rs','tests/startup/multi/ingress/application.rs','tests/startup/multi/ingress/history.rs','src/application/ready_admission.rs','tests/application/ready_admission.rs','tests/client_admission/ready_batch/temporary_context.rs','examples/native_benchmark.rs','examples/benchmark/offered_capacity.rs','examples/benchmark/offered_capacity/tests.rs','src/format_crc.rs','src/format_crc/tests.rs','src/log/command_bytes.rs','docs/COMMAND_BYTES.md','rust-toolchain.toml','validation/check-native-benchmark.mjs','validation/check-offered.test.mjs','validation/check-rounded-rate.mjs','src/native/log_store/witness.rs','src/native/log_store/witness/format.rs','src/native/log_store/witness/publication.rs','src/native/log_store/witness/tests/crash.rs','src/native/log_store/witness/tests/selection.rs','tests/witness_log_store.rs','tests/journal_barrier.rs','examples/benchmark/publication.rs','examples/benchmark/publication/tests.rs','examples/benchmark/observe/publication_tests.rs','validation/check-publication-profile.mjs','validation/check-publication-timings.mjs']:
    assert required in hashes, required
qualified=qualified['files']
assert qualified, 'dynamic actual root-qualified source projection required'
assert set(hashes)==set(qualified), (set(hashes)-set(qualified),set(qualified)-set(hashes))
for relative,record in qualified.items():
    assert hashes[relative]==record['sha256'],relative
    assert modes[relative]==record['mode'],relative

inventory = {'source_root_mode': oct(stat.S_IMODE(Path.cwd().stat().st_mode)), 'commit': commit, 'inputs': len(hashes), 'sha256': hashes, 'modes': modes, 'directory_modes': directory_modes,
             'archive_sha256': hashlib.sha256(raw).hexdigest(), 'committed_archive_sha256': hashlib.sha256(original_raw).hexdigest(), 'committed_archive_modes': committed_archive_modes, 'protected_fixture_keys': protected_fixture_keys, 'permission_policy': 'Exact Linux physical source file/directory modes preserved; seven private fixture keys protected0600; committed bytes unchanged', 'selectors': selectors,
             'optional_cargo_config_present': optional_cargo_config_present, 'inventory_scope': 'Selected committed production/harness source, fixtures, locks, build/config/hooks and contract documents; no builds/WAL states; not a claim every input compiled.'}
(here / 'source.tar').write_bytes(raw)
(here / 'source.tar').chmod(0o600)
(here / 'SOURCE.json').write_text(json.dumps(inventory, indent=2) + '\n')
(here / 'source.sha256').write_text(''.join(value + '  ' + name + '\n' for name, value in sorted(hashes.items())))
print(json.dumps({'commit': commit, 'inputs': len(hashes), 'archive_sha256': inventory['archive_sha256']}, sort_keys=True))
