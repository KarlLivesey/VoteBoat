#!/usr/bin/env python3
"""Frozen diagnostic source/executable guards and one original listener lifecycle test and coordinated startup regression."""
import hashlib
import json
import pathlib
import subprocess
import sys

TASK = pathlib.Path(__file__).resolve().parent
SOURCE = json.loads((TASK / 'SOURCE.json').read_text())


def sha(file):
    return hashlib.sha256(pathlib.Path(file).read_bytes()).hexdigest()


def artifacts():
    build = TASK / 'BUILD.jsonl'
    if not build.exists():
        return {}
    records = [json.loads(line) for line in build.read_text().splitlines()]
    records = [r for r in records if r.get('reason') == 'compiler-artifact']
    tests = [r for r in records if r['target']['name'] == 'logical_activation_nested' and r.get('executable')]
    libraries = [r for r in records if r['target']['name'] == 'voteboat' and 'lib' in r['target']['kind']]
    assert len(tests) == 1 and libraries, 'actual executable/library missing'
    selected = [tests[0], libraries[-1]]
    assert all(not r['fresh'] for r in selected), 'unique changed library/test must actually build'
    assert tests[0]['profile']['debug_assertions'] and tests[0]['profile']['overflow_checks'], 'assertions and checked arithmetic required'
    files = [tests[0]['executable']] + [f for f in libraries[-1]['filenames'] if f.endswith('.rlib')]
    assert len(files) == 2, 'one original compiled library required'
    return {'executable': tests[0]['executable'], 'profile': tests[0]['profile'], 'sha256': {f: sha(f) for f in files}}


def guard(label):
    wrong = [rel for rel, digest in SOURCE['sha256'].items() if sha(TASK / 'repo' / rel) != digest]
    result = {'inputs': SOURCE['inputs'], 'mismatches': wrong, 'artifacts': artifacts()}
    (TASK / (label + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    assert not wrong, wrong
    return result


if sys.argv[1] == 'guard':
    print(json.dumps(guard(sys.argv[2])))
else:
    assert len(sys.argv) == 3 and sys.argv[1] == 'run' and sys.argv[2] in ['FULL']
    label = sys.argv[2]
    assert not (TASK / (label + '_RESULT.json')).exists(), 'approved invocation already consumed'
    assert (TASK / 'CHECK_EXITS.txt').read_text().splitlines() == ['fmt=0', 'all=0', 'default=0', 'core=0', 'native=0', 'build=0']
    before = guard('BEFORE_' + label)
    test = 'discovery::abandoned_prepared_startup_closes_workers_and_releases_listener'
    listed = subprocess.run([before['artifacts']['executable'], '--list'], cwd=TASK / 'repo', capture_output=True, text=True, check=True)
    (TASK / 'ACTUAL_TEST_LIST.txt').write_text(listed.stdout)
    assert len([line for line in listed.stdout.splitlines() if line.endswith(': test')]) == 14, 'original full14 target required'
    command = [before['artifacts']['executable']]
    if label == 'FOCUSED':
        command.extend(['--exact', test])
    command.extend(['--nocapture', '--test-threads=1'])
    (TASK / (label + '.command.json')).write_text(json.dumps(command, indent=2) + '\n')
    with (TASK / (label + '.log')).open('w') as output:
        run = subprocess.run(command, cwd=TASK / 'repo', stdout=output, stderr=subprocess.STDOUT, check=False)
    after = guard('AFTER_' + label)
    same = before['artifacts'] == after['artifacts']
    result = {'test': test if label == 'FOCUSED' else 'logical_activation_nested all-feature full14 target', 'command': command, 'status': run.returncode, 'sourceMismatches': after['mismatches'], 'binaryAndLibraryUnchanged': same}
    (TASK / (label + '_RESULT.json')).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
    print((TASK / (label + '.log')).read_text()[-12000:])
    sys.exit(run.returncode if same else 2)
