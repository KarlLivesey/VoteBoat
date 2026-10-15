"""Independent read-only484 frozen-config review; no setup/workload invocation."""
import ast
import hashlib
import json
from pathlib import Path

base = Path('target/p9/native484-ingress')
review = base / 'review'
load = lambda p: json.loads(Path(p).read_text())
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
paths = {'linux': base / 'comparison/linux/revision2/campaign', 'mac': base / 'comparison/mac/prepared/campaign'}
prior_paths = {'linux': Path('target/p9/native481-admission/linux'), 'mac': Path('target/p9/native481-admission/mac-collection/raw')}
expected = {'baseline': 'f7b0ef12709bb7e7aad4f0279851aa035b508e9e', 'candidate': 'efd383dc36b4b9edf8f66021b90115824440b468'}
config = {host: load(p / 'CAMPAIGN_CONFIG.json') for host, p in paths.items()}
assert config['linux']['scripts'] == config['mac']['scripts']
assert config['linux']['validators'] == config['mac']['validators']
assert config['linux']['schema_sha256'] == config['mac']['schema_sha256']
checked = {}
for host, p in paths.items():
    c = config[host]
    old = load(prior_paths[host] / 'CAMPAIGN_CONFIG.json')
    ready = load(p / 'READY.json')
    assert sha(p / 'CAMPAIGN_CONFIG.json') == ready['campaign_config_sha256']
    assert ready['cases'] == 48 and ready['pairs'] == 24 and c['source_commits'] == expected
    assert c['existing_native_drain_guard_seconds'] == old['existing_native_drain_guard_seconds'] == 120
    assert c['all_workloads_end_before_validation'] is True
    assert c['python'] == old['python'] and c['node'] == old['node']
    assert c['runtime_tools'] == old['runtime_tools']
    assert len(c['cases']) == 48 and len({case['name'] for case in c['cases']}) == 48
    assert len({case['pair'] for case in c['cases']}) == 24
    for index in range(0, 48, 2):
        first, second = c['cases'][index:index + 2]
        assert first['pair'] == second['pair'] and {first['arm'], second['arm']} == {'baseline', 'candidate'}
    for case, previous in zip(c['cases'], old['cases']):
        assert {k: v for k, v in case.items() if k not in ['command', 'resource_command', 'wal_root', 'metadata']} == {k: v for k, v in previous.items() if k not in ['command', 'resource_command', 'wal_root', 'metadata']}
        assert case['command'][2:] == previous['command'][2:]
        binary = next(a for a in c['candidates'][case['arm']]['artifacts'] if a['target'] == 'native_benchmark')
        assert case['command'][0] == binary['copy'] and case['command'][1] == case['wal_root']
        assert case['resource_command'] == [c['python'], c['job'] + '/resources.py', case['metadata'], *case['command']]
        assert case['wal_root'] == c['wal_parent'] + '/' + case['name']
        assert case['metadata'] == c['job'] + '/campaign-metadata/' + case['name']
        assert case['retained_bound'] <= 16384 and case['publication'] == 'witness'
        assert case['group_pending_ceiling'] == (case['window'] + case['groups'] - 1) // case['groups']
    assert sum(case['offers'] for case in c['cases']) == 391680
    assert sum(case['horizon_seconds'] for case in c['cases']) == 1344
    assert (p / 'ORDER.txt').read_text().splitlines() == [case['name'] for case in c['cases']]
    for name, digest in c['scripts'].items():
        assert sha(p / name) == digest
        if name.endswith('.py'):
            ast.parse((p / name).read_text())
    assert (p / 'REQUIRED_COMMIT').read_text().strip() == expected['candidate']
    for name, digest in c['validators'].items():
        assert sha(p / 'control-tools' / name) == digest == old['validators'][name]
        for arm in ['baseline', 'candidate']:
            assert c['candidates'][arm]['source']['files']['validation/' + name] == digest
    assert len(c['validators']) == 8
    assert sha(p / 'SCHEMA.md') == c['schema_sha256']
    assert sha(p / 'resources.py') == sha(prior_paths[host] / 'resources.py')
    # The controller differs from481 only in its leading slice-label docstring.
    assert (p / 'campaign.py').read_text().splitlines()[1:] == (prior_paths[host] / 'campaign.py').read_text().splitlines()[1:]
    setup = load(p / 'setup-host.json')
    assert setup['device_identity'] == c['device_identity'] and setup['cpu_policy'] == c['cpu_policy']
    assert not (p / 'STARTED.json').exists() and not (p / 'campaign').exists()
    baseline, candidate = [c['candidates'][arm] for arm in ['baseline', 'candidate']]
    assert sorted(t['sha256'] for t in baseline['compiler_tools']) == sorted(t['sha256'] for t in candidate['compiler_tools'])
    for name in ['Cargo.toml', 'Cargo.lock', 'clippy.toml', 'build.rs', 'LICENSE', 'benchmarks/matched/Cargo.toml', 'benchmarks/matched/Cargo.lock']:
        assert baseline['source']['files'][name] == candidate['source']['files'][name]
    fixtures = [name for name in baseline['source']['files'] if name.startswith('tests/fixtures/')]
    for name in fixtures:
        assert baseline['source']['files'][name] == candidate['source']['files'][name]
        assert baseline['source']['modes'][name] == candidate['source']['modes'][name]
    keys = [name for name in fixtures if name.endswith('key.der')]
    assert len(keys) == 7 and all(candidate['source']['modes'][key] == '0o600' for key in keys)
    profiles = []
    actual_artifacts = {}
    for arm in ['baseline', 'candidate']:
        r = c['candidates'][arm]
        assert r['source']['commit'] == expected[arm]
        for artifact in r['artifacts']:
            record = artifact['compiler_record']
            assert record['fresh'] is False
            assert record['profile'] == {'opt_level': '3', 'debuginfo': 0, 'debug_assertions': True, 'overflow_checks': True, 'test': False}
            assert record['features'] == ['default', 'native', 'quic', 'tls']
            assert record['manifest_path'] == r['source']['root'] + '/Cargo.toml'
            if host == 'linux':
                assert sha(artifact['copy']) == sha(artifact['original']) == artifact['sha256']
        actual_artifacts[arm] = {a['target']: a['sha256'] for a in r['artifacts']}
        assert ready['artifacts_sha256'][arm] == actual_artifacts[arm]['native_benchmark']
        if host == 'linux':
            for item in r['proofs'] + r['compiler_tools'] + c['runtime_tools']:
                assert sha(item['path']) == item['sha256']
            symbols = load(Path(r['source']['path']).parent / 'SYMBOLS.json')
            assert symbols['root_buildscript_fresh_record']['fresh'] is False
    checked[host] = {'config_sha256': sha(p / 'CAMPAIGN_CONFIG.json'), 'ready_sha256': sha(p / 'READY.json'), 'job': c['job'], 'cases': 48, 'pairs': 24, 'offers': 391680, 'horizon_seconds': 1344, 'same481_native_argv_order_except_binary_and_root_paths': True, 'artifacts': actual_artifacts, 'withinhost_compiler_protoc_hashes_equal': True, 'seven_private_fixtures_0600': True, 'fixtures_count': len(fixtures), 'scripts': c['scripts'], 'validators': c['validators'], 'setup_host_sha256': sha(p / 'setup-host.json')}
# Remote Mac actual guards ran before preparation; independently verify exact
# remote/local27-file hash/length/mode mapping, then bind actual fresh metadata.
remote = load(base / 'comparison/mac/REMOTE_PREPARED_FILES.json')
collection = load(base / 'comparison/mac/PREPARED_COLLECTION.json')
assert collection['actual_exit'] == 0 and len(remote) == 27
for name, meta in remote.items():
    p = base / 'comparison/mac/prepared' / name
    assert sha(p) == meta['sha256'] == collection['files'][name]['sha256']
    assert p.stat().st_size == meta['bytes'] and oct(p.stat().st_mode & 0o777) == meta['mode']
mac_guard = load(base / 'comparison/mac/prepared/ACTUAL_MAC_GUARD.json')
assert mac_guard['exit'] == 0 and mac_guard['source_commits'] == expected
assert mac_guard['artifacts_sha256'] == checked['mac']['artifacts']
mac_raw = base / 'mac/collection/raw'
readiness, symbols = load(mac_raw / 'READINESS.json'), load(mac_raw / 'SYMBOLS.json')
assert readiness['exit'] == 0 and readiness['source_exact'] and readiness['source_commit'] == expected['candidate']
assert readiness['artifact_sha256'] == checked['mac']['artifacts']['candidate']
assert readiness['native_uuid'] == symbols['uuid'] and symbols['root_buildscript_fresh_record']['fresh'] is False
assert readiness['fresh_root_executable_buildscript'] and readiness['controls'] == 4
assert sha(mac_raw / 'SOURCE.json') == config['mac']['candidates']['candidate']['source']['sha256']
assert load(base / 'mac/INDEPENDENT_ROOT_REVIEW.json')['exit'] == 0
receipt = {'exit': 0, 'hosts': checked, 'mac_remote27_exact_original_metadata': True, 'no_blockers': True, 'review_script_sha256': sha(__file__), 'boundaries': ['Both config native argv/order match actual481 except binary/WAL/resource paths; four cells, three rotating adjacent pairs, q16/q64/group ceilings unchanged.', 'Controller body and resources are unchanged from481. One serial inherited environment per host; no per-arm environment overrides. No environment fingerprint was added or claimed.', 'Author common.py strengthening binds Mac ROOT_FINAL_INPUTS1502 source/modes; published source and both compiled artifacts are untouched.', 'Committed offered.rs diff only extracts the same at-most64 per-original clock/window/leader/admit loop into three nested synchronous ingress scopes. Sorting, maintenance, warmup64, drain120 and completion timer boundaries unchanged; every outcome remains.', 'Actual Mac binaries stay remote; actual remote guards and27-file exact collection bind their hashes, fresh native profile/UUID/compiler/source/qualification proof. No binary was copied or build/guard/workload repeated remotely.', 'All setup/readout/copy/build tools must stop before separate root quiet GO. No timing authorization from this review; raw readout waits BOTHEND.'], 'raw_reader_sha256': sha(review / 'raw_readout.py')}
assert config['linux']['scripts'] == config['mac']['scripts']
(review / 'INDEPENDENT_CONFIG_REVIEW.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({'exit': 0, 'linux_config': checked['linux']['config_sha256'], 'mac_config': checked['mac']['config_sha256'], 'cases_per_host': 48, 'mac_remote_files_verified': len(remote), 'no_blockers': True}, indent=2))
