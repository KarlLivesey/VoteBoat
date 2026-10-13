"""Archive only a published committed candidate; never contacts Mac or builds."""
import hashlib
import io
import json
import re
import subprocess
import tarfile
from pathlib import Path

here = Path(__file__).resolve().parent
commit = (here / 'REQUIRED_COMMIT').read_text().strip()
if not re.fullmatch('[0-9a-f]{40}', commit):
    raise SystemExit('unpublished candidate refused: REQUIRED_COMMIT is empty/invalid')
subprocess.run(['git', 'merge-base', '--is-ancestor', commit, 'refs/remotes/origin/main'], check=True)
selectors = ['Cargo.toml', 'Cargo.lock', 'build.rs', 'clippy.toml', 'rust-toolchain.toml',
             'LICENSE', 'AGENTS.md', 'README.md', '.gitignore', '.githooks',
             'src', 'tests', 'examples', 'benchmarks/matched/Cargo.toml',
             'benchmarks/matched/Cargo.lock', 'benchmarks/matched/src',
             'benchmarks/matched/README.md', 'benchmarks/matched/SCHEMA.md',
             'benchmarks/matched/COMPARISON.md', 'docs/component-contracts.json',
             'docs/provider-conformance.json']
optional_cargo_config_present = bool(subprocess.check_output(['git', 'ls-tree', '-r', '--name-only', commit, '.cargo'], text=True).strip())
if optional_cargo_config_present:
    selectors.append('.cargo')
for name in ['source.tar', 'SOURCE.json', 'source.sha256']:
    if (here / name).exists():
        raise SystemExit('refuse replacing existing committed archive: ' + name)
raw = subprocess.check_output(['git', 'archive', '--format=tar', commit, *selectors])
hashes = {}
modes = {}
with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
    for member in archive:
        if member.isdir():
            continue
        if not member.isfile():
            raise SystemExit('unsupported nonregular source: ' + member.name)
        hashes[member.name] = hashlib.sha256(archive.extractfile(member).read()).hexdigest()
        modes[member.name] = oct(member.mode)
for required in ['build.rs', 'tests/provider_conformance/log_store/entry_equality.rs',
                 'src/log/comparison.rs', 'src/log/comparison/tests.rs',
                 'src/log/owned/tests/checkpoint.rs', 'src/native/log_store/accepted/tests/checkpoint.rs',
                 'src/raft/checkpoint_tests.rs', 'benchmarks/matched/src/voteboat_engine/actor/tests/replies.rs']:
    assert required in hashes, required
inventory = {'commit': commit, 'inputs': len(hashes), 'sha256': hashes, 'modes': modes,
             'archive_sha256': hashlib.sha256(raw).hexdigest(), 'selectors': selectors,
             'optional_cargo_config_present': optional_cargo_config_present, 'inventory_scope': 'Selected committed production/harness source, fixtures, locks, build/config/hooks and contract documents; no builds/WAL states; not a claim every input compiled.'}
(here / 'source.tar').write_bytes(raw)
(here / 'SOURCE.json').write_text(json.dumps(inventory, indent=2) + '\n')
(here / 'source.sha256').write_text(''.join(value + '  ' + name + '\n' for name, value in sorted(hashes.items())))
print(json.dumps({'commit': commit, 'inputs': len(hashes), 'archive_sha256': inventory['archive_sha256']}, sort_keys=True))
