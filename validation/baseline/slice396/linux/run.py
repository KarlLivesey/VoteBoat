#!/usr/bin/env python3
"""Original full startup profiles, immutable source/artifacts, no runtime changes."""
import hashlib
import json
import pathlib
import subprocess
import sys

TASK = pathlib.Path(__file__).resolve().parent
SOURCE = json.loads((TASK / 'SOURCE.json').read_text())

def sha(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()

def artifacts(profile):
    if not profile:
        return {}
    path = TASK / ('BUILD_' + profile + '.jsonl')
    if not path.exists():
        return {}
    records = [json.loads(line) for line in path.read_text().splitlines()]
    records = [r for r in records if r.get('reason') == 'compiler-artifact']
    tests = [r for r in records if r['target']['name'] == 'startup' and r.get('executable')]
    libraries = [r for r in records if r['target']['name'] == 'voteboat' and 'lib' in r['target']['kind']]
    assert len(tests) == 1 and len(libraries) == 1
    selected = [tests[0], libraries[0]]
    expected_features = {'default', 'native', 'tls'} | ({'quic'} if profile == 'ALL' else set())
    for record in selected:
        assert not record['fresh'], 'actual fresh original compile required'
        assert record['profile']['opt_level'] == '3'
        assert record['profile']['debug_assertions'] and record['profile']['overflow_checks']
        assert set(record['features']) == expected_features
    files = [tests[0]['executable']] + [f for f in libraries[0]['filenames'] if f.endswith('.rlib')]
    assert len(files) == 2
    return {'executable': tests[0]['executable'], 'records': selected, 'sha256': {f: sha(f) for f in files}}

def guard(label, profile=''):
    wrong = [r for r, h in SOURCE['sha256'].items() if sha(TASK / 'repo' / r) != h]
    result = {'inputs': SOURCE['inputs'], 'mismatches': wrong, 'artifacts': artifacts(profile)}
    if profile == 'ALL' and (TASK / 'AFTER_ALL.json').exists():
        assert result['artifacts'] == json.loads((TASK / 'AFTER_ALL.json').read_text())['artifacts']
    (TASK / (label + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    assert not wrong
    return result

if sys.argv[1] == 'guard':
    print(json.dumps(guard(sys.argv[2], sys.argv[3] if len(sys.argv) > 3 else '')))
else:
    assert len(sys.argv) == 3 and sys.argv[1] == 'run' and sys.argv[2] in ['ALL', 'DEFAULT']
    profile = sys.argv[2]
    assert not (TASK / (profile + '_RESULT.json')).exists(), 'one invocation per original full profile'
    assert (TASK / 'CHECK_EXITS.txt').read_text().splitlines() == ['fmt=0', 'all=0', 'default=0', 'core=0', 'native=0']
    if profile == 'DEFAULT':
        prior = json.loads((TASK / 'ALL_RESULT.json').read_text())
        assert prior['binaryAndLibraryUnchanged'] and not prior['sourceMismatches']
    before = guard('BEFORE_' + profile, profile)
    executable = before['artifacts']['executable']
    listed = subprocess.run([executable, '--list'], cwd=TASK / 'repo', text=True, capture_output=True, check=True)
    (TASK / (profile + '_ACTUAL_TEST_LIST.txt')).write_text(listed.stdout)
    tests = [line[:-6] for line in listed.stdout.splitlines() if line.endswith(': test')]
    assert tests and len(tests) == len(set(tests))
    command = [executable, '--nocapture', '--test-threads=1']
    (TASK / (profile + '.command.json')).write_text(json.dumps(command, indent=2) + '\n')
    with (TASK / (profile + '.log')).open('w') as output:
        run = subprocess.run(command, cwd=TASK / 'repo', stdout=output, stderr=subprocess.STDOUT, check=False)
    after = guard('AFTER_' + profile, profile)
    same = before['artifacts'] == after['artifacts']
    result = {'profile': profile, 'command': command, 'actualSelectedCount': len(tests), 'actualTests': tests, 'status': run.returncode, 'sourceMismatches': after['mismatches'], 'binaryAndLibraryUnchanged': same}
    (TASK / (profile + '_RESULT.json')).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
    print((TASK / (profile + '.log')).read_text()[-16000:])
    sys.exit(run.returncode if same else 2)
