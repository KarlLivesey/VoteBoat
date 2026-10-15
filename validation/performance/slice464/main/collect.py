#!/usr/bin/env python3
"""Collect slice464 ONLY after root supplies an explicit terminal-scope receipt.

Usage: python3 target/p9/slice464/collect.py --terminal-scope PATH
Receipt keys: all_recorded_jobs_terminal:true, source_before/source_after (job-
relative inventory names), original_runs:[{handle,actual_exit,status_path}],
scope_notes:[strings]. status_path is job-relative. No command here runs tests,
workloads, compilers, or source changes. Source versions must already exist.
"""
from pathlib import Path
import argparse
import gzip
import hashlib
import json
import re
import shutil
import stat
import subprocess

ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / 'target/p9/slice464'
OUT = ROOT / 'validation/performance/slice464/main'

def sha(data):
    return hashlib.sha256(data).hexdigest()

def read_json(path):
    return json.loads(path.read_text())

def rows(inventory):
    return {row['path']: row for row in inventory['files']}

def matches(path, row):
    return (path.is_file() and not path.is_symlink()
            and sha(path.read_bytes()) == row['sha256']
            and oct(stat.S_IMODE(path.stat().st_mode)) == row['mode'])

def main():
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument('--terminal-scope', type=Path, required=True)
    receipt = args.parse_args().terminal_scope.resolve()
    scope = read_json(receipt)
    assert scope['all_recorded_jobs_terminal'] is True
    assert scope['original_runs'] and scope['scope_notes']
    assert not OUT.exists(), 'Refuse to overwrite a sealed/partial archive'
    for run in scope['original_runs']:
        assert isinstance(run['handle'], int) and isinstance(run['actual_exit'], int)
        assert int((JOB / run['status_path']).read_text().strip()) == run['actual_exit']
    status_paths = {str(p.relative_to(JOB)) for p in JOB.rglob('STATUS')
                    if not any(part.endswith('-source') or part.startswith('source-') for part in p.relative_to(JOB).parts)}
    assert status_paths <= {run['status_path'] for run in scope['original_runs']}, status_paths

    before_name, after_name = scope['source_before'], scope['source_after']
    before, after = read_json(JOB / before_name), read_json(JOB / after_name)
    initial, final = rows(before), rows(after)
    for name, row in final.items():
        assert matches(ROOT / name, row), ('Final source/mode differs', name)
    inventories = {}
    for path in JOB.glob('*.json'):
        value = read_json(path)
        if isinstance(value, dict) and isinstance(value.get('files'), list):
            if all(isinstance(row, dict) and {'path', 'sha256', 'mode'} <= row.keys()
                   for row in value['files']):
                inventories[path.name] = rows(value)
    assert before_name in inventories and after_name in inventories
    tree = subprocess.run(['git', 'ls-tree', '-rz', before['parent']], cwd=ROOT,
                          check=True, capture_output=True).stdout
    tracked = {record.split(b'\t', 1)[1].decode()
               for record in tree.split(b'\0') if record}
    changed = set()
    for patch in sorted([*JOB.glob('*.patch'), *JOB.glob('*.diff')]):
        for line in patch.read_text().splitlines():
            if line.startswith(('+++ b/', '--- a/')):
                changed.add(line[6:].split('\t', 1)[0])
    for version in inventories.values():
        changed.update(name for name, row in version.items()
                       if name not in tracked or initial.get(name) != row)
        changed.update(set(initial) - set(version))
    # INITIAL.patch defines the initial material delta; later inventory deltas
    # and untracked module names add focused fixes. Never archive a full tree.
    assert changed, 'No changed paths identified'
    snapshot_roots = sorted(path for path in JOB.iterdir()
                            if path.is_dir() and (path.name.endswith('-source') or path.name.startswith('source-')))
    source_copies = []
    for label, version in inventories.items():
        preferred = JOB / (label.removesuffix('.json').lower() + '-source')
        candidates = [preferred, *snapshot_roots, ROOT]
        for name in sorted(changed & version.keys()):
            row = version[name]
            source = next((base / name for base in candidates if matches(base / name, row)), None)
            assert source is not None, ('Unavailable historical source bytes', label, name, row)
            source_copies.append((source, Path('source-versions') / label.removesuffix('.json') / name))

    # All status/source preconditions above precede archive creation. These
    # exclusions are directories, not filename suffix guesses inside raw logs.
    evidence = []
    for path in JOB.rglob('*'):
        if not path.is_file():
            continue
        relative = path.relative_to(JOB)
        if any(part.endswith('-source') or part.startswith('source-') or part in {'target', 'build', 'artifacts', '__pycache__'}
               for part in relative.parts):
            continue
        assert not path.is_symlink(), path
        evidence.append((path, relative))
    OUT.mkdir(parents=True)
    compressed = []
    selections = []
    def copy(source, relative):
        raw = source.read_bytes()
        target = OUT / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if len(raw) > 1024 * 1024:
            target = target.with_name(target.name + '.gz')
            encoded = gzip.compress(raw, compresslevel=9, mtime=0)
            target.write_bytes(encoded)
            target.chmod(stat.S_IMODE(source.stat().st_mode))
            assert gzip.decompress(target.read_bytes()) == raw
            compressed.append({'original_path': str(relative), 'original_bytes': len(raw),
                               'original_sha256': sha(raw), 'archived_path': str(target.relative_to(OUT)),
                               'gzip_bytes': len(encoded), 'gzip_sha256': sha(encoded)})
        else:
            shutil.copy2(source, target)
        selections.append({'source_path': str(source.relative_to(ROOT)),
                           'archived_path': str(target.relative_to(OUT)), 'original_sha256': sha(raw)})
    for source, relative in sorted(evidence, key=lambda pair: str(pair[1])):
        copy(source, relative)
    for source, relative in source_copies:
        copy(source, relative)
    if not any(source.resolve() == receipt for source, _ in evidence):
        copy(receipt, Path('TERMINAL_SCOPE.json'))
    copy(JOB / before_name, Path('SOURCE_BEFORE.json'))
    copy(JOB / after_name, Path('SOURCE_AFTER.json'))
    results = {}
    pattern = re.compile(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out')
    for source, relative in evidence:
        if source.suffix == '.log':
            found = pattern.findall(source.read_text(errors='replace'))
            if found:
                results[str(relative)] = [dict(zip(['result', 'passed', 'failed', 'ignored', 'measured', 'filtered'], record)) for record in found]
    (OUT / 'ACTUAL_TEST_RESULTS.json').write_text(json.dumps(results, indent=2) + '\n')
    (OUT / 'SOURCE_SCOPE.json').write_text(json.dumps({
        'initial_inventory': before_name, 'final_inventory': after_name,
        'initial_inputs': len(initial), 'final_inputs': len(final),
        'changed_or_new_paths': sorted(changed),
        'unchanged_initial_final': sum(initial.get(name) == row for name, row in final.items()),
        'historical_versions': len(source_copies),
        'scope': 'Raw statuses/counts remain per attempt; no aggregate is relabeled as one final-source passing run.'
    }, indent=2) + '\n')
    (OUT / 'COMPRESSION.json').write_text(json.dumps(compressed, indent=2) + '\n')
    (OUT / 'SELECTION.json').write_text(json.dumps(selections, indent=2) + '\n')
    notes = '\n'.join('- ' + note for note in scope['scope_notes'])
    (OUT / 'README.md').write_text(
        '# Slice464 acknowledged-prefix qualification\n\n'
        'This archive preserves actual attempts, commands, raw stdout/stderr, statuses, source inventories and focused source versions. '
        'See TERMINAL_SCOPE.json for original handles and exact scope; ACTUAL_TEST_RESULTS.json extracts raw per-log counts without combining attempts.\n\n'
        + notes + '\n\n'
        'The initial sandbox socket failures remain failures. Later authorized loopback qualification is recorded separately; no raw exit is replaced. '
        'Independent production review is INDEPENDENT_REVIEW.json. No performance benefit is claimed by these functional/lint checks.\n\n'
        'Only changed/new source versions are copied; full source snapshots, binaries and build trees are excluded. '
        'Text larger than1MiB is losslessly gzip-compressed with both original and archived SHA256 in COMPRESSION.json. '
        'Every copied inventory/nested manifest remains an ordinary payload. Outer SHA256SUMS covers every regular file recursively except itself. '
        'Historical absolute command paths remain unchanged. P9 performance/headroom/fairness acceptance remains separate and open.\n')
    payloads = sorted(path for path in OUT.rglob('*') if path.is_file())
    assert all(not path.is_symlink() for path in payloads)
    (OUT / 'SHA256SUMS').write_text(''.join(sha(path.read_bytes()) + '  ' + str(path.relative_to(OUT)) + '\n' for path in payloads))
    for line in (OUT / 'SHA256SUMS').read_text().splitlines():
        digest, relative = line.split('  ', 1)
        assert sha((OUT / relative).read_bytes()) == digest
    for row in compressed:
        assert sha(gzip.decompress((OUT / row['archived_path']).read_bytes())) == row['original_sha256']
    print(json.dumps({'payloads': len(payloads), 'regular_files': len(payloads) + 1,
                      'manifest_sha256': sha((OUT / 'SHA256SUMS').read_bytes()), 'archive': str(OUT)}))

if __name__ == '__main__':
    main()
