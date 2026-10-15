#!/usr/bin/env python3
"""Seal twelve SAME464-artifact native OFF/ON commands; never run workloads.

Root reviews this setup, performs socket/quiet preflight, then writes READY.json
for the unchanged461 campaign controller. No READY or quiet grant is made here.
"""
from pathlib import Path
import hashlib
import json
import shutil
import stat

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).resolve().parent
JOB = HERE / 'linux'
BUILD = ROOT / 'target/p9/native464-linux'
OLD = ROOT / 'target/p9/comparison461-native/linux'
COMMIT = '6fce8989e2c57afa033b5ee982aaa201eae04ac6'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_text())


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def main():
    assert not JOB.exists(), 'No overwrite/restart of prior or partial setup'
    assert (BUILD / 'STATUS').read_text().strip() == '0'
    assert (BUILD / 'STAGE').read_text().strip() == 'DONE'
    assert (BUILD / 'PILOT_ORIGINAL_EXIT').read_text().strip() == '0'
    publication = load(BUILD / 'PUBLICATION.json')
    assert publication['commit'] == COMMIT
    assert publication['actual_push_exit'] == publication['qualified_main_exit'] == 0
    source = load(BUILD / 'SOURCE.json')
    assert source['commit'] == COMMIT and source['inputs'] == len(source['sha256'])
    for name, digest in source['sha256'].items():
        path = BUILD / 'source' / name
        assert sha(path) == digest and oct(stat.S_IMODE(path.stat().st_mode)) == source['physical_modes'][name]
    artifacts = load(BUILD / 'ARTIFACTS.json')
    assert len(artifacts) == 2
    executable = next(a for a in artifacts if a['target'] == 'native_benchmark')
    library = next(a for a in artifacts if a['target'] == 'voteboat')
    for artifact in artifacts:
        assert sha(Path(artifact['original'])) == sha(Path(artifact['copy'])) == artifact['sha256']
        record = artifact['compiler_record']
        assert record['fresh'] is False
        assert Path(record['manifest_path']).resolve() == BUILD / 'source/Cargo.toml'
        assert record['features'] == ['default', 'native', 'quic', 'tls']
        p = record['profile']
        assert p['opt_level'] == '3' and p['debuginfo'] == 0
        assert p['debug_assertions'] is True and p['overflow_checks'] is True and p['test'] is False
    proof = load(BUILD / 'PROFILE_PROOF.json')
    assert proof['source_commit'] == COMMIT and proof['actual_exit'] == 0
    assert proof['root_example_fresh_false'] and proof['root_buildscript_fresh_false']
    assert proof['release_oracle_proof']['reference_oracle_cfg_absent_PROFILErelease']
    assert proof['release_oracle_proof']['executable_sha256'] == executable['sha256']
    assert proof['source_inventory_sha256'] == sha(BUILD / 'SOURCE.json')
    assert proof['actual_rustc_lines_sha256'] == sha(BUILD / 'logs/native-root-rustc-lines.txt')
    assert proof['cargo_json_sha256'] == sha(BUILD / 'logs/native-release.jsonl')
    native_tools = load(BUILD / 'control-tools/SOURCE.json')
    assert native_tools['source_commit'] == COMMIT and len(native_tools['files']) == 8

    JOB.mkdir()
    for directory in ('control-tools', 'campaign-metadata', 'campaign-wal'):
        (JOB / directory).mkdir()
    for name, row in native_tools['files'].items():
        original = BUILD / 'control-tools' / name
        assert sha(original) == source['sha256'][row['committed_path']] == row['sha256']
        assert oct(stat.S_IMODE(original.stat().st_mode)) == row['mode']
        shutil.copy2(original, JOB / 'control-tools' / name)
    shutil.copy2(BUILD / 'control-tools/SOURCE.json', JOB / 'control-tools/SOURCE.json')
    shutil.copy2(OLD / 'campaign.py', JOB / 'campaign.py')
    assert sha(JOB / 'campaign.py') == sha(OLD / 'campaign.py')
    collector = BUILD / 'controls/tcp-r0-metadata/resources.py'
    compiler = {name: {'path': str(BUILD / 'logs' / name), 'sha256': sha(BUILD / 'logs' / name)}
                for name in ('rustc.txt', 'cargo.txt', 'protoc.txt', 'tool-binaries.sha256',
                             'native-root-rustc-lines.txt', 'native-release.jsonl')}
    common = {
        'source_commit': COMMIT, 'source_inventory': str(BUILD / 'SOURCE.json'),
        'source_inventory_sha256': sha(BUILD / 'SOURCE.json'),
        'artifact': executable, 'root_artifact': library, 'compiler': compiler,
        'profile_proof': {'path': str(BUILD / 'PROFILE_PROOF.json'), 'sha256': sha(BUILD / 'PROFILE_PROOF.json')},
    }
    cases = []
    for protocol in ('tcp', 'quic'):
        for trial in (1, 2, 3):
            order = [('baseline', 0), ('candidate', 2)] if trial != 2 else [('candidate', 2), ('baseline', 0)]
            for variant, replies in order:
                label = 'off' if replies == 0 else 'on'
                name = f'g1-{protocol}-r500-w64-q16-t{trial}-{label}'
                metadata = JOB / 'campaign-metadata' / name
                metadata.mkdir()
                shutil.copy2(collector, metadata / 'resources.py')
                wal = JOB / 'campaign-wal' / name
                assert wal.parent.is_dir() and not wal.exists()
                command = [executable['copy'], str(wal), protocol, '15000', '64', '1',
                           '--offered', '500', '--maintenance', '2',
                           '--ready-items', '16', '--ready-replies', str(replies)]
                cases.append({'name': name, 'variant': variant, 'producer': label, 'trial': trial,
                              'protocol': protocol, 'ready_reply_inputs': replies,
                              'metadata': str(metadata), 'wal_root': str(wal), 'command': command,
                              'resource_collector_sha256': sha(metadata / 'resources.py'),
                              'resource_command': ['python3', str(metadata / 'resources.py'), *command]})
    scope = ('SAME published464 source/binary producerOFF0 versus ON2;12 serial paired trials '
             '(3 per TCP/TLS+QUIC),15000offers/rate500/window64/ready16/g1/3voters/Counter8bytes/'
             'warm64/active2smaintenance/noPause. Finite durable application diagnostic; joins are '
             'actual returned-step observations, not a hit-rate or causal proof. No framework floor '
             'or full sustained/headroom/fairness acceptance claim.')
    config = {'control_tools': {'directory': str(JOB / 'control-tools'),
                               'inventory_sha256': sha(JOB / 'control-tools/SOURCE.json'),
                               'files': native_tools['files']},
              'profile': 'nativeopt3/debug0/assert+overflow/defaultLTO/defaultcodegen; notframeworkthin/cgu1',
              'scope': scope, 'resources': 'Unchanged Linux whole-process peakRSS collector; includes warmup/recovery/cleanup, not measurement-only or continuous RSS. Block counts are not bytes.',
              'timing_authorized': False, 'root_quiet_grant_required': True,
              'baseline': {**common, 'producer': 'off', 'ready_reply_inputs': 0},
              'candidate': {**common, 'producer': 'on', 'ready_reply_inputs': 2}, 'cases': cases}
    assert len(cases) == 12
    write(JOB / 'CAMPAIGN_CONFIG.json', config)
    write(JOB / 'COMMANDS.json', [case['command'] for case in cases])
    write(JOB / 'SETUP_GUARD.json', {
        'source_commit': COMMIT, 'source_inputs': source['inputs'],
        'source_artifact_compiler_validator_guards': 0,
        'same_artifact_both_arms': True, 'same_source_profile_payload_settings': True,
        'exact_runner_sha256': sha(JOB / 'campaign.py'), 'exact_collector_sha256': sha(collector),
        'campaign_config_sha256': sha(JOB / 'CAMPAIGN_CONFIG.json'),
        'all_12_absent_roots_have_existing_parent': True,
        'socket_preflight_performed': False, 'quiet_grant': False,
        'source_build_script': str(BUILD / 'run.sh'),
        'preparation_script_sha256': sha(Path(__file__)),
        'scope': 'Preparation only. Root must perform actual socket/process preflight and create READY.json before explicit --authorized-quiet launch.'})
    print(json.dumps({'job': str(JOB), 'cases': 12, 'timing_started': False}))


if __name__ == '__main__':
    main()
