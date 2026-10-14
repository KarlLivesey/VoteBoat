"""Archive source-frozen431 only after the root supplies an actual terminal receipt."""
import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path

JOB = Path(__file__).resolve().parent
ROOT = JOB.parents[2]
EXPECTED_PATCH = '9db76f'  # root-reviewed final prefix; full digest is validated below
STAGES = ['source-before', 'fmt-initial', 'focused-owned-append', 'clippy-all',
          'clippy-default', 'clippy-core', 'clippy-native', 'affected-all',
          'affected-core', 'actual-services', 'fmt-final', 'matched-tests',
          'matched-fmt', 'matched-clippy', 'source-after']


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, description):
    if not condition:
        raise ValueError(description)


def summaries(path):
    # Retain the actual preceding execution line; do not invent a suite from counts.
    execution = None
    rows = []
    for number, line in enumerate(path.read_text().splitlines(), 1):
        if 'Running ' in line and not line.startswith('{'):
            execution = line.strip()
        match = re.search(r'test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', line)
        if match:
            names = ['passed', 'failed', 'ignored', 'measured', 'filtered_out']
            rows.append({'result': match[1], **{name: int(match[i + 2]) for i, name in enumerate(names)},
                         'log_line': number, 'preceding_execution_line': execution})
    return rows


def copy_regular(source, destination):
    require(source.is_file() and not source.is_symlink(), 'nonregular input: ' + str(source))
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination)


def archive(terminal_path, promotion_path):
    terminal = json.loads(terminal_path.read_text())
    require(terminal.get('original_handle') == 96474, 'original root handle receipt required')
    require(terminal.get('actual_terminal_exit') == 0, 'actual terminal success required')
    require((JOB / 'CHECK_STATUS').read_text().strip() == '0', 'final check status must be0')
    require((JOB / 'CHECK_STAGE').read_text().strip() == 'DONE', 'final runner must beDONE')
    source = json.loads((JOB / 'SOURCE.json').read_text())
    require(source['source_files'] == 1427 and len(source['changed_files']) == 3, 'source scope differs')
    require(source['patch_sha256'].startswith(EXPECTED_PATCH), 'reviewed patch differs')
    require(digest(JOB / 'PATCH.diff') == source['patch_sha256'], 'patch digest differs')
    promotion = json.loads(promotion_path.read_text())
    require(promotion.get('all_selected_hashes_match') is True, 'verified promotion required')
    for relative, expected in source['sha256'].items():
        require(digest(JOB / 'source' / relative) == expected, 'frozen source changed: ' + relative)
        require(digest(ROOT / relative) == expected, 'promoted source changed: ' + relative)
    statuses = {}
    coverage = {}
    for name in STAGES:
        rc = int((JOB / (name + '.status')).read_text().strip())
        require(rc == 0, 'unqualified final stage: ' + name)
        statuses[name] = rc
        if name in ['focused-owned-append', 'affected-all', 'affected-core', 'actual-services', 'matched-tests']:
            coverage[name] = summaries(JOB / (name + '.log'))
            require(coverage[name] and all(row['result'] == 'ok' and row['failed'] == 0 for row in coverage[name]), 'actual test summaries required: ' + name)
    output = ROOT / 'validation/performance/slice431'
    require(not output.exists(), 'refuse replacing an existing archive')
    isolated = output / 'isolated'
    isolated.mkdir(parents=True)
    for path in JOB.rglob('*'):
        relative = path.relative_to(JOB)
        if relative.parts[0] in {'source', 'test-tmp'} or '__pycache__' in relative.parts:
            continue
        if path.is_file():
            copy_regular(path, isolated / relative)
    for relative in source['changed_files']:
        copy_regular(JOB / 'source' / relative, isolated / 'changed-source' / relative)
    copy_regular(terminal_path, isolated / 'ACTUAL_TERMINAL.json')
    copy_regular(promotion_path, isolated / 'ROOT_PROMOTION.json')
    qualification = {
        'source_files': source['source_files'], 'changed_files': source['changed_files'],
        'new_files': source['new_files'], 'patch_sha256': source['patch_sha256'],
        'original_terminal': terminal, 'final_statuses': statuses, 'actual_test_summaries': coverage,
        'initial_failures': [
            {'attempt': 1, 'terminal': 101, 'cause': 'typed LogRevision has no AddAssign; fixture constructor fix, no production change'},
            {'attempt': 2, 'terminal': 101, 'passed': 5, 'failed': 1,
             'cause': 'public config ingress correctly rejects unadmitted configuration; fixture now asserts InvalidMessage then exercises internal validated receive fallback'}],
        'dependency_cache': 'isolated source uses terminal429 debug/check target dependencies; current431 root recompiled and checked, not a fresh performance artifact claim',
        'configuration_fixture_scope': 'public step must reject; internal membership fixture tests configuration fallback only, no public admission or protocol activation claim',
        'durability_scope': 'unit fixture tickets model host completion; separate actual TCP/QUIC retry/recovery integration controls retained',
        'performance_gain_measured': False, 'publication': 'pending enabled root hook and publication receipt',
    }
    (output / 'QUALIFICATION.json').write_text(json.dumps(qualification, indent=2) + '\n')
    rows = []
    for name, results in coverage.items():
        rows.append('| ' + name + ' | ' + ', '.join(str(r['passed']) + '/0' for r in results) + ' |')
    (output / 'README.md').write_text('''# Slice431 isolated qualification

The ordinary incoming Append may transfer an already-owned whole vector after all existing validation and reply construction. Only offset0, tight outer capacity and Noop/tight Command payloads qualify. Configuration, overlapping suffixes and spare allocations retain the exact clone fallback. Payload eligibility is computed during existing validation, without an additional traversal. Independent pending ownership, public effects, providers, tickets, budgets and stored/wire fields remain unchanged.

Final root-supplied original96474 terminal receipt and exact source promotion are archived. SOURCE inventories1,427 selected inputs, three changed paths and one new test module; all frozen and promoted hashes matched before collection. PATCH and changed-source snapshots preserve final and failed attempts without copying full source or build trees.

Actual final test summaries (each comma-separated item is an independently printed suite; raw execution lines are in QUALIFICATION.json):

| Stage | Passed/failed |
| --- | --- |
''' + '\n'.join(rows) + '''

Every final listed formatter, four strict root profiles, standalone formatter/strict profile and before/after source stage completed0. The actual stage logs/commands/statuses remain authoritative. Debug dependency/build targets reuse terminal429 caches; current source is compiled and checked. This does not claim genuinely fresh performance artifacts, which follow publication separately.

Both initial fixture failures remain: attempt1 typed LogRevision arithmetic compile error101; attempt2 five passing tests and public unadmitted-configuration rejection101. Focused fixes use the revision constructor and explicitly assert the public InvalidMessage gate before internal membership-fixture receive. They change no production error gate or lint threshold. Internal configured fallback and modeled tickets are not public configuration admission or disk durability proofs. Existing retirement/authentication/rollback and actual service histories stay enabled.

No gain or sustained P9 acceptance is claimed. Enabled-hook publication, fresh Linux/Mac artifacts and matched paired measurements remain pending. No browser or performance profile change was used for this qualification.
''')
    files = {p.relative_to(output).as_posix(): digest(p) for p in output.rglob('*') if p.is_file()}
    require(all(not p.is_symlink() for p in output.rglob('*')), 'archive symlink found')
    (output / 'EVIDENCE.json').write_text(json.dumps({'regular_files_before_manifest': len(files), 'sha256': files,
        'original_handle': 96474, 'original_terminal_exit': 0, 'source_files': 1427,
        'excluded': 'full source, test temporary states, debug/release targets and binary artifacts'}, indent=2) + '\n')
    files['EVIDENCE.json'] = digest(output / 'EVIDENCE.json')
    (output / 'evidence.sha256').write_text(''.join(f'{value}  {name}\n' for name, value in sorted(files.items())))
    for name, expected in files.items():
        require(digest(output / name) == expected, 'archive hash mismatch: ' + name)
    print(json.dumps({'archive': str(output), 'regular_files': len(files) + 1, 'hashes_verified': True, 'coverage': coverage}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive', action='store_true')
    parser.add_argument('--terminal-receipt', type=Path)
    parser.add_argument('--promotion', type=Path)
    args = parser.parse_args()
    if args.archive:
        require(args.terminal_receipt is not None and args.promotion is not None, 'actual root terminal receipt and promotion required')
        archive(args.terminal_receipt.resolve(), args.promotion.resolve())
    else:
        print('Collector prepared only; no coverage or archive claim. Require root actual terminal receipt plus verified promotion.')
