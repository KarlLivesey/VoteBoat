#!/usr/bin/env python3
"""Prepare, but do not build/run, a fresh published464 native source job.

Invoke after publication with --commit FULL_SHA --publication ACTUAL_RECEIPT.
The existing463 native DEBUG0 runner/guard is adapted without profile changes.
Pilot/controller/validator resources are intentionally assembled after building.
"""
from pathlib import Path, PurePosixPath
import argparse
import hashlib
import json
import re
import stat
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = ROOT / 'target/p9/native463-linux'
JOB = ROOT / 'target/p9/native464-linux'
REQUIRED_COMMIT = '6fce8989e2c57afa033b5ee982aaa201eae04ac6'
TOOLS = (
    'check-benchmark-storage.mjs', 'check-lanes.mjs', 'check-maintenance.mjs',
    'check-native-benchmark.mjs', 'check-offered.mjs',
    'check-publication-timings.mjs', 'check-rounded-rate.mjs', 'check-joined.mjs',
)
SELECTORS = (
    'Cargo.toml', 'Cargo.lock', 'clippy.toml', 'build.rs', 'LICENSE', 'AGENTS.md',
    'rust-toolchain.toml', '.cargo', '.githooks', 'src', 'tests', 'examples',
    'docs/component-contracts.json', 'docs/provider-conformance.json',
    'docs/COMMAND_BYTES.md', 'docs/PERFORMANCE.md',
    *(f'validation/{name}' for name in TOOLS),
)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def git(*args, **kwargs):
    return subprocess.run(['git', *args], cwd=ROOT, check=True,
                          capture_output=True, **kwargs).stdout


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def committed_inputs(commit):
    rows = []
    for entry in git('ls-tree', '-r', '-z', '--full-tree', commit, '--', *SELECTORS).split(b'\0'):
        if not entry:
            continue
        header, encoded_path = entry.split(b'\t', 1)
        mode, kind, oid = header.decode().split()
        name = encoded_path.decode()
        rel = PurePosixPath(name)
        assert not rel.is_absolute() and '..' not in rel.parts, name
        assert kind == 'blob' and mode in ('100644', '100755'), (name, mode, kind)
        rows.append((name, mode, oid))
    names = {name for name, _, _ in rows}
    mandatory = {name for name in SELECTORS if name not in ('.cargo', '.githooks', 'src', 'tests', 'examples')}
    assert mandatory <= names, sorted(mandatory - names)
    for prefix in ('.githooks/', 'src/', 'tests/', 'examples/'):
        assert any(name.startswith(prefix) for name in names), prefix
    assert {name for name in names if name.startswith('validation/')} == {
        f'validation/{name}' for name in TOOLS
    }
    blobs = git('cat-file', '--batch', input=''.join(f'{oid}\n' for _, _, oid in rows).encode())
    hashes, physical, git_modes = {}, {}, {}
    offset = 0
    for name, mode, oid in rows:
        end = blobs.index(b'\n', offset)
        actual_oid, kind, size = blobs[offset:end].decode().split()
        assert actual_oid == oid and kind == 'blob', name
        offset = end + 1
        body = blobs[offset:offset + int(size)]
        offset += int(size)
        assert blobs[offset:offset + 1] == b'\n', name
        offset += 1
        parent = ROOT / name
        assert parent.is_file() and not parent.is_symlink(), name
        hashes[name] = digest(body)
        assert digest(parent.read_bytes()) == hashes[name], ('parent differs from committed source', name)
        actual_mode = stat.S_IMODE(parent.stat().st_mode)
        assert actual_mode & 0o111 == int(mode, 8) & 0o111, ('executable bits differ', name)
        if name.startswith('tests/fixtures/') and name.endswith('-key.der'):
            assert actual_mode == 0o600, ('private fixture parent mode', name, oct(actual_mode))
        physical[name] = oct(actual_mode)
        git_modes[name] = mode
    assert offset == len(blobs)
    return hashes, physical, git_modes


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--publication', type=Path, required=True)
    args = parser.parse_args()
    assert re.fullmatch(r'[0-9a-f]{40}', args.commit), 'Require the full published commit'
    assert args.commit == REQUIRED_COMMIT, 'Require the published464 candidate, not another slice'
    receipt_path = args.publication.resolve()
    receipt_bytes = receipt_path.read_bytes()
    receipt = json.loads(receipt_bytes)
    assert receipt['commit'] == args.commit
    for key in ('actual_push_exit', 'qualified_main_exit'):
        assert type(receipt[key]) is int and receipt[key] == 0, key
    assert git('rev-parse', '--verify', args.commit + '^{commit}').decode().strip() == args.commit
    # Local publication tracking and the separately retained actual push receipt
    # must agree. This command neither fetches nor changes refs.
    git('merge-base', '--is-ancestor', args.commit, 'refs/remotes/origin/main')
    assert not JOB.exists(), 'Refuse to overwrite any prior/partial464 setup'
    hashes, physical, git_modes = committed_inputs(args.commit)
    old_commit = json.loads((TEMPLATE / 'SOURCE.json').read_text())['commit']
    assert args.commit != old_commit, 'Do not export the previous463 baseline as464'
    runner_bytes = (TEMPLATE / 'run.sh').read_bytes()
    guard_bytes = (TEMPLATE / 'guard.py').read_bytes()
    runner = runner_bytes.decode()
    guard = guard_bytes.decode()
    old_assertion = f"assert manifest['commit']=='{old_commit}'"
    assert runner.count(str(TEMPLATE)) == 1 and guard.count(old_assertion) == 1
    runner = runner.replace(str(TEMPLATE), str(JOB))
    guard = guard.replace(old_assertion, f"assert manifest['commit']=='{args.commit}'")
    assert str(TEMPLATE) not in runner and old_assertion not in guard

    JOB.mkdir()
    source = JOB / 'source'
    source.mkdir()
    (JOB / 'logs').mkdir()
    (JOB / 'PUBLICATION.json').write_bytes(receipt_bytes)
    command = ['git', 'archive', '--format=tar', args.commit, *sorted(hashes)]
    write_json(JOB / 'source-export.command.json', command)
    with (JOB / 'source.tar').open('xb') as archive:
        subprocess.run(command, cwd=ROOT, stdout=archive, check=True)
    with tarfile.open(JOB / 'source.tar') as archive:
        for member in archive.getmembers():
            rel = PurePosixPath(member.name)
            assert not rel.is_absolute() and '..' not in rel.parts
            assert member.isdir() or member.isfile(), member.name
            if member.isfile():
                assert member.name in hashes, member.name
        archive.extractall(source, filter='data')
    actual = {p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()}
    assert actual == set(hashes), (actual - set(hashes), set(hashes) - actual)
    for name, expected in hashes.items():
        path = source / name
        path.chmod(int(physical[name], 8))
        assert digest(path.read_bytes()) == expected, name
        assert oct(stat.S_IMODE(path.stat().st_mode)) == physical[name], name
    write_json(JOB / 'SOURCE.json', {
        'commit': args.commit, 'source_root': str(source), 'inputs': len(hashes),
        'sha256': hashes, 'physical_modes': physical, 'git_modes': git_modes,
        'scope': 'Committed root/build/config/source/tests/examples/contracts and exact eight validator closure; excludes historical validation exports and unrelated comparison workspace. Physical permissions match qualified parent, including private0600 fixtures.',
    })
    (JOB / 'source.sha256').write_text(''.join(f'{value}  {name}\n' for name, value in sorted(hashes.items())))
    (JOB / 'run.sh').write_text(runner)
    (JOB / 'run.sh').chmod(0o755)
    (JOB / 'guard.py').write_text(guard)
    (JOB / 'guard.py').chmod(0o644)
    write_json(JOB / 'PREPARATION.json', {
        'commit': args.commit, 'source_inputs': len(hashes),
        'publication_receipt': str(receipt_path), 'publication_sha256': digest(receipt_bytes),
        'template_runner_sha256': digest(runner_bytes), 'template_guard_sha256': digest(guard_bytes),
        'adapted_runner_sha256': digest(runner.encode()), 'adapted_guard_sha256': digest(guard.encode()),
        'script_sha256': digest(Path(__file__).read_bytes()),
        'source_only_setup': True, 'build_run_checks_or_timing_started': False,
        'next_step': 'Root coordinates the original fresh native DEBUG0 build runner; assemble private control-tools/resources/pilot config only after actual artifacts exist.',
    })
    print(json.dumps({'prepared': str(JOB), 'commit': args.commit, 'inputs': len(hashes), 'build_started': False}))


if __name__ == '__main__':
    main()
