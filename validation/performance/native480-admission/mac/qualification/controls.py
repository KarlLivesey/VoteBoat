import csv, datetime, hashlib, json, os, subprocess, time
from pathlib import Path

job = Path(__file__).resolve().parent
records = json.loads((job / 'ARTIFACTS.json').read_text())
artifact = next(r for r in records if r['target'] == 'native_benchmark')
binary = Path(artifact['copy'])
digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
assert digest(binary) == artifact['sha256']
roots = job / 'control-wal'
roots.mkdir()
result = {'scope': 'Four selected Atomic/Witness x TCP/QUIC original release q64/two-group31:1 functional Create/Recover/readback histories; no capacity-rate or improvement claim', 'pid': os.getpid(), 'start': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'artifact_sha256': artifact['sha256'], 'cases': []}
def run(name, args):
    before = time.monotonic()
    with (job / 'logs' / (name + '.stdout')).open('wb') as out, (job / 'logs' / (name + '.stderr')).open('wb') as err:
        completed = subprocess.run(args, stdout=out, stderr=err)
    receipt = {'command': args, 'exit': completed.returncode, 'elapsed_s': time.monotonic() - before}
    (job / 'logs' / (name + '.exit.json')).write_text(json.dumps(receipt, indent=2) + '\n')
    assert completed.returncode == 0, receipt
    return receipt
for provider in ['atomic', 'witness']:
 for protocol in ['tcp', 'quic']:
    name = provider + '-' + protocol + '-q64-g2-w4096'
    root = roots / name
    assert not root.exists()
    print('START ' + name, flush=True)
    process = run(name, [str(binary), str(root), protocol, '512', '4096', '2', '--offered', '128', '--maintenance', '2', '--ready-items', '64', '--offered-capacity', '4096', '--publication', provider, '--group-weights', '31,1'])
    validator = run(name + '-validator', ['node', str(job / 'source/validation/check-native-benchmark.mjs'), '--storage', str(root)])
    summary = dict(token.split('=', 1) for token in (root / 'summary.txt').read_text().split())
    assert summary['ready_visit_items_requested'] == summary['ready_visit_items_effective'] == '64'
    assert summary['groups'] == '2' and summary['workload_group_weights'] == '31,1'
    assert summary['applied'] == '512' and all(summary[key] == '0' for key in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])
    assert int(summary['checkpoints']) > 0 and int(summary['reclaims']) > 0
    assert summary['retry_verified'] == summary['workers_joined'] == 'true'
    assert int(summary['recovered_value']) == 64 + int(summary['applied'])
    assert summary['offered_capacity_effective'] == '4096'
    assert summary['publication_requested'] == summary['publication_effective'] == provider
    assert summary['publication_provider'] == ('FileLogIo' if provider == 'atomic' else 'WitnessLogIo')
    assert summary['publication_timings'] == 'native' and summary['publication_scope'] == 'ordinary_barrier'
    assert summary['storage_error_scope'] == 'overlapping_observations'
    run_metadata = (root / 'run.txt').read_text()
    assert 'offered_capacity_requested=4096' in run_metadata
    assert 'publication_requested=' + provider in run_metadata
    all_lines = (job / 'logs' / (name + '.stderr')).read_text().splitlines()
    capacity = [line for line in all_lines if line.startswith('capacity_readback ')]
    publication = [line for line in all_lines if line.startswith('publication_readback ')]
    for lines in [capacity, publication]:
        assert len(lines) == 6
        for mode in ['Create', 'Recover']:
            for replica in [1, 2, 3]:
                assert sum('mode=' + mode + ' ' in line and 'replica=' + str(replica) + ' ' in line for line in lines) == 1
    assert all('offered_capacity_effective=4096 ' in line and 'client_group_requests=4096 ' in line and 'shard_group_items=4112 ' in line for line in capacity)
    assert all('publication_effective=' + provider + ' ' in line and 'publication_scope=ordinary_barrier ' in line for line in publication)
    with (root / 'storage.csv').open() as stream:
        storage = list(csv.DictReader(stream))
    joined = [row for row in storage if row['stage'].endswith('_join')]
    assert len(joined) == 6 and all(int(row['encoded_bytes']) > 0 for row in joined)
    assert all(int(row['errors']) == 0 for row in storage)
    selected_files = []
    for replica in [1, 2, 3]:
        selector = root / ('replica' + str(replica)) / 'WITNESS-CURRENT'
        assert selector.is_file() == (provider == 'witness')
        selected_files.append({'replica': replica, 'witness_selector_present': selector.is_file()})
    result['cases'].append({'name': name, 'process': process, 'validator': validator, 'summary': summary,
        'actual_capacity_readbacks': capacity, 'actual_publication_readbacks': publication, 'selected_files': selected_files})
    (job / 'CONTROLS.json').write_text(json.dumps(result, indent=2) + '\n')
    print('END ' + name + ' 0', flush=True)
assert digest(binary) == artifact['sha256']
result['end'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
result['artifact_unchanged'] = True
(job / 'CONTROLS.json').write_text(json.dumps(result, indent=2) + '\n')
