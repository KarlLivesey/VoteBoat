import datetime, hashlib, json, os, subprocess, time
from pathlib import Path

job = Path(__file__).resolve().parent
records = json.loads((job / 'ARTIFACTS.json').read_text())
artifact = next(r for r in records if r['target'] == 'native_benchmark')
binary = Path(artifact['copy'])
digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
assert digest(binary) == artifact['sha256']
roots = job / 'control-wal'
roots.mkdir()
result = {'scope': 'Two original release functional wide Create/Recover/readback histories; no capacity-rate claim', 'pid': os.getpid(), 'start': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'artifact_sha256': artifact['sha256'], 'cases': []}
def run(name, args):
    before = time.monotonic()
    with (job / 'logs' / (name + '.stdout')).open('wb') as out, (job / 'logs' / (name + '.stderr')).open('wb') as err:
        completed = subprocess.run(args, stdout=out, stderr=err)
    receipt = {'command': args, 'exit': completed.returncode, 'elapsed_s': time.monotonic() - before}
    (job / 'logs' / (name + '.exit.json')).write_text(json.dumps(receipt, indent=2) + '\n')
    assert completed.returncode == 0, receipt
    return receipt
for protocol in ['tcp', 'quic']:
    name = protocol + '-w4096'
    root = roots / name
    assert not root.exists()
    print('START ' + name, flush=True)
    process = run(name, [str(binary), str(root), protocol, '512', '4096', '1', '--offered', '128', '--maintenance', '2', '--ready-items', '16', '--offered-capacity', '4096'])
    validator = run(name + '-validator', ['node', str(job / 'source/validation/check-native-benchmark.mjs'), '--storage', str(root)])
    summary = dict(token.split('=', 1) for token in (root / 'summary.txt').read_text().split())
    assert summary['retry_verified'] == summary['workers_joined'] == 'true'
    assert int(summary['recovered_value']) == 64 + int(summary['applied'])
    assert summary['offered_capacity_effective'] == '4096'
    assert 'offered_capacity_requested=4096' in (root / 'run.txt').read_text()
    lines = [line for line in (job / 'logs' / (name + '.stderr')).read_text().splitlines() if line.startswith('capacity_readback ')]
    assert len(lines) == 6
    assert sum('mode=Create ' in line for line in lines) == 3
    assert sum('mode=Recover ' in line for line in lines) == 3
    assert all('offered_capacity_effective=4096 ' in line and 'client_group_requests=4096 ' in line and 'shard_group_items=4112 ' in line for line in lines)
    result['cases'].append({'name': name, 'process': process, 'validator': validator, 'summary': summary, 'actual_readbacks': lines})
    (job / 'CONTROLS.json').write_text(json.dumps(result, indent=2) + '\n')
    print('END ' + name + ' 0', flush=True)
assert digest(binary) == artifact['sha256']
result['end'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
result['artifact_unchanged'] = True
(job / 'CONTROLS.json').write_text(json.dumps(result, indent=2) + '\n')
