#!/usr/bin/env python3
"""Frozen diagnostic source/executable guards and two sequential original native histories."""
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
    assert tests[0]['profile']['debug_assertions'], 'original assertions required'
    files = [tests[0]['executable']] + [f for f in libraries[-1]['filenames'] if f.endswith('.rlib')]
    assert len(files) == 2, 'one original compiled library required'
    return {'executable': tests[0]['executable'], 'sha256': {f: sha(f) for f in files}}


def guard(label):
    wrong = [rel for rel, digest in SOURCE['sha256'].items() if sha(TASK / 'repo' / rel) != digest]
    result = {'inputs': SOURCE['inputs'], 'mismatches': wrong, 'artifacts': artifacts()}
    (TASK / (label + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    assert not wrong, wrong
    return result


if sys.argv[1] == 'guard':
    print(json.dumps(guard(sys.argv[2])))
else:
    assert len(sys.argv) == 3 and sys.argv[1] == 'run' and sys.argv[2] in ['TCP', 'QUIC']
    label = sys.argv[2]
    assert not (TASK / (label + '_RESULT.json')).exists(), 'approved invocation already consumed'
    if label == 'QUIC':
        assert (TASK / 'TCP_RESULT.json').exists(), 'original TCP must run first'
    before = guard('BEFORE_' + label)
    test = 'history::' + label.lower() + '_fifteen_native_owners_two_original_logical_replacements'
    command = [before['artifacts']['executable'], '--exact', test, '--nocapture', '--test-threads=1']
    with (TASK / (label + '.log')).open('w') as output:
        run = subprocess.run(command, cwd=TASK / 'repo', stdout=output, stderr=subprocess.STDOUT, check=False)
    after = guard('AFTER_' + label)
    same = before['artifacts'] == after['artifacts']
    result = {'test': test, 'command': command, 'status': run.returncode, 'sourceMismatches': after['mismatches'], 'binaryAndLibraryUnchanged': same}
    (TASK / (label + '_RESULT.json')).write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))
    print((TASK / (label + '.log')).read_text()[-26000:])
    sys.exit(run.returncode if same else 2)
