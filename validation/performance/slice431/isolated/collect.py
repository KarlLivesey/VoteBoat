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
          'clippy-default', 'clippy-core', 'clippy-native', 'remaining-all',
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
    exits = {r['original_handle']: r['actual_terminal_exit'] for r in terminal['attempts']}
    require(exits == {96474: 101, 13664: 101, 96016: 0, 16659: 0, 34328: 0, 47583: 0}, 'actual original scope/terminals differ')
    require((JOB / 'CHECK_STATUS').read_text().strip() == '101', 'preserve original broad failure101')
    require((JOB / 'RESUME_STATUS').read_text().strip() == '0' and (JOB / 'RESUME_STAGE').read_text().strip() == 'DONE', 'remaining original96016 actual0/DONE required')
    source = json.loads((JOB / 'SOURCE.json').read_text())
    require(source['source_files'] == 1427 and len(source['changed_files']) == 3, 'source scope differs')
    require(source['patch_sha256'].startswith(EXPECTED_PATCH), 'reviewed patch differs')
    require(digest(JOB / 'PATCH.diff') == source['patch_sha256'], 'patch digest differs')
    promotion = json.loads(promotion_path.read_text())
    require(promotion.get('all_selected_hashes_match') and promotion.get('all_selected_modes_match'), 'verified promotion required')
    for relative, expected in source['sha256'].items():
        for root in [JOB / 'source', ROOT]:
            path = root / relative
            require(digest(path) == expected, 'source changed: ' + str(path))
            require(oct(path.stat().st_mode & 0o777) == source['modes'][relative], 'source mode changed: ' + str(path))
    statuses, coverage = {}, {}
    for name in STAGES:
        rc = int((JOB / (name + '.status')).read_text().strip())
        require(rc == 0, 'unqualified successful stage: ' + name)
        statuses[name] = rc
        if name in ['focused-owned-append', 'remaining-all', 'affected-core', 'actual-services', 'matched-tests']:
            coverage[name] = summaries(JOB / (name + '.log'))
            require(coverage[name] and all(r['result'] == 'ok' and r['failed'] == 0 for r in coverage[name]), 'actual successful summaries required: ' + name)
    broad = summaries(JOB / 'affected-all.log')
    require([(r['passed'], r['failed']) for r in broad] == [(392, 0), (201, 2)], 'original broad scope differs')
    coverage['original96474-affected-all'] = broad
    for name, expected in [('hundred_compacted_groups_catch_up_over_workers_and_authenticated_transport', (1, 0)), ('native_shared_peer_quota_pressure_reconnect_restart_and_retry', (0, 1))]:
        stage = 'focused-' + name
        rows = summaries(JOB / (stage + '.log'))
        require([(r['passed'], r['failed']) for r in rows] == [expected], 'original13664 focused scope differs: ' + name)
        require(int((JOB / (stage + '.status')).read_text()) == (0 if expected[1] == 0 else 101), 'focused original status differs')
        coverage['original13664-' + name] = rows
    candidate = json.loads((JOB / 'candidate-pressure/PRESSURE_RESULT.json').read_text())
    baseline_job = ROOT / 'target/p9/baseline430-pressure'
    baseline = json.loads((baseline_job / 'PRESSURE_RESULT.json').read_text())
    require(candidate['original_handle'] == 47583 and candidate['actual_terminal_exit'] == 0 and candidate['source_patch_sha256'] == source['patch_sha256'], 'candidate controlled actual proof differs')
    require(baseline['original_handle'] == 34328 and baseline['actual_terminal_exit'] == 0, 'baseline actual proof differs')
    output = ROOT / 'validation/performance/slice431'
    require(not output.exists(), 'refuse replacing an archive')
    isolated = output / 'isolated'; isolated.mkdir(parents=True)
    excluded = {'source', 'test-tmp', 'target-checks', '__pycache__'}
    for path in JOB.rglob('*'):
        relative = path.relative_to(JOB)
        if any(part in excluded for part in relative.parts):
            continue
        if path.is_file():
            copy_regular(path, isolated / relative)
    for relative in source['changed_files']:
        copy_regular(JOB / 'source' / relative, isolated / 'changed-source' / relative)
    for path in baseline_job.rglob('*'):
        relative = path.relative_to(baseline_job)
        if any(part in excluded for part in relative.parts):
            continue
        if path.is_file():
            copy_regular(path, isolated / 'baseline430-pressure' / relative)
    copy_regular(terminal_path, isolated / 'ACTUAL_TERMINALS.json')
    copy_regular(promotion_path, isolated / 'ROOT_PROMOTION.json')
    qualification = {
        'source_files': 1427, 'changed_files': source['changed_files'], 'new_files': source['new_files'], 'patch_sha256': source['patch_sha256'],
        'actual_terminals': terminal, 'successful_stage_statuses': statuses, 'actual_test_summaries': coverage,
        'owner_coverage': '201 passing cases from original96474 plus separate same-source hundred-compacted13664 and controlled pressure47583 passes; never a whole203-case broad pass',
        'baseline_pressure': baseline, 'controlled_candidate_pressure': candidate,
        'initial_failures': [
            {'attempt': 1, 'terminal': 101, 'cause': 'typed LogRevision has no AddAssign; fixture constructor correction'},
            {'attempt': 2, 'terminal': 101, 'passed': 5, 'failed': 1, 'cause': 'public config gate correctly rejects; fixture explicitly asserts InvalidMessage then tests internal receive fallback'},
            {'attempt': 3, 'original_handle': 96474, 'terminal': 101, 'passed_lib': 392, 'passed_owner': 201, 'failed_owner': 2, 'cause': 'two unchanged-deadline native progress timeouts; causal mechanism unestablished'},
            {'original_handle': 13664, 'terminal': 101, 'hundred_compacted': 'pass57.83s', 'pressure': 'fail46.14s; same initial campaign/catch-up point, cause unestablished'}],
        'dependency_cache': 'root431 debug/check targets reuse terminal429 dependencies; actual current root compilation captured in original96474 Cargo JSON; fresh default-debug430 contrast uses a distinct target. Framework performance artifacts remain separate postpublication.',
        'configuration_fixture_scope': 'public step rejects unadmitted configuration; internal membership fixture tests fallback, not public protocol admission',
        'durability_scope': 'unit tickets model completion; separate actual TCP/QUIC retry/recovery controls passed',
        'ambient_limitations': 'baseline complete ambient environment was not captured; known direct arguments/RUST_BACKTRACE1/default debug/compiler/temporary filesystem matched. Candidate whole-host proc counters are not causal or per-test CPU/IO measurements.',
        'performance_gain_measured': False, 'publication': 'pending enabled root hook/publication receipt',
    }
    (output / 'QUALIFICATION.json').write_text(json.dumps(qualification, indent=2) + '\n')
    rows = ['| ' + name + ' | ' + ', '.join(str(r['passed']) + '/' + str(r['failed']) for r in results) + ' |' for name, results in coverage.items()]
    (output / 'README.md').write_text("""# Slice431 scoped qualification

The owned incoming Append vector transfers only after original validation and reply construction, at suffix offset0 with tight outer capacity and Noop/tight Command payloads. Configuration, overlap and spare-allocation cases preserve the clone fallback. Eligibility uses the existing validation traversal. Independent pending copies, provider ownership, capacities/budgets, ticket/barrier correlation, errors, formats and original outcomes remain intact.

All1,427 selected frozen/promoted source hashes and modes match ROOT_PROMOTION; three paths change and one test module is new. Final and failed changed-source snapshots, PATCH, schemas, actual commands/logs/statuses/Cargo compiler records and original handle receipts are preserved. Debug dependency caches are reused only for checks; these are not fresh framework performance artifacts.

Actual test summaries follow. Each comma-separated result is a separately printed suite; QUALIFICATION.json retains its raw execution line:

| Stage | Passed/failed |
| --- | --- |
""" + '\n'.join(rows) + """

Native owner coverage is201 passing cases from broad original96474, plus the individual unchanged-source hundred-compacted pass13664 and controlled pressure pass47583. The broad203-case owner run did not pass: its two deadline failures and actual101 remain, as does the second pressure failure in13664. No original status was replaced, no source/deadline was changed, and no whole203-case broad pass is claimed.

Initial attempts remain: typed LogRevision arithmetic compile101, followed by five passing new controls and one public unadmitted-configuration gate rejection101. Focused fixture fixes use the revision constructor and assert the public InvalidMessage gate before exercising internal configured fallback. No lint threshold, public gate or production behavior was weakened. Internal modeled tickets are not disk durability proof.

A fresh distinct published430 debug baseline passed the exact pressure case8.68s (34328). Controlled431 direct invocation of the same verified all-feature executable passed9.49s (47583), with before/after source/binary guards, unchanged15s deadline and known arguments/RUST_BACKTRACE1/fresh same-filesystem TMPDIR matching. No Cargo rebuild was performed. Historical complete baseline ambient environment was not captured. Candidate proc/diskstats/pressure observations are whole-host snapshots, not per-test blocked time or causal evidence. The earlier timeout cause remains unestablished; all raw errors and comparison limitations remain.

Remaining original96016 integrations/core/services/matched, formatting, four strict root profiles and standalone strict checks passed in their documented scopes. Exact individual-case qualification does not claim native performance improvement, sustained acceptance or completion of P9. Enabled-hook publication, fresh both-host artifacts and paired rates/latency remain pending.
""")
    files = {p.relative_to(output).as_posix(): digest(p) for p in output.rglob('*') if p.is_file()}
    require(all(not p.is_symlink() for p in output.rglob('*')), 'archive symlink found')
    (output / 'EVIDENCE.json').write_text(json.dumps({'regular_files_before_manifest': len(files), 'sha256': files, 'actual_terminals': terminal['attempts'], 'source_files': 1427, 'excluded': 'full source, generated native/test states, binary artifacts and debug/release build targets'}, indent=2) + '\n')
    files['EVIDENCE.json'] = digest(output / 'EVIDENCE.json')
    (output / 'evidence.sha256').write_text(''.join(f'{value}  {name}\n' for name, value in sorted(files.items())))
    for name, expected in files.items():
        require(digest(output / name) == expected, 'archive hash mismatch: ' + name)
    print(json.dumps({'archive': str(output), 'regular_files': len(files) + 1, 'bytes': sum(p.stat().st_size for p in output.rglob('*') if p.is_file()), 'hashes_verified': True, 'coverage': coverage}, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--archive', action='store_true')
    parser.add_argument('--terminal-receipt', type=Path)
    parser.add_argument('--promotion', type=Path)
    args = parser.parse_args()
    if args.archive:
        require(args.terminal_receipt is not None and args.promotion is not None, 'actual root terminal and promotion required')
        archive(args.terminal_receipt.resolve(), args.promotion.resolve())
    else:
        print('Collector prepared; require honest actual original/resume/pressure receipts and exact promotion, no pending coverage claim.')
