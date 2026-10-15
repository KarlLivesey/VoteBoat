#!/usr/bin/env python3
"""Prepare existing four native functional controls after the actual464 build.

No subprocess, socket preflight, benchmark, validator or build is executed here.
Usage: prepare-pilots.py --build-handle ORIGINAL_ROOT_HANDLE
Root must separately review preflight/quiet state before invoking run-pilots.py.
"""
from pathlib import Path
import argparse
import hashlib
import json
import shutil
import stat
import tomllib

ROOT = Path(__file__).resolve().parents[3]
JOB = ROOT / 'target/p9/native464-linux'
TEMPLATE = ROOT / 'target/p9/native463-linux'
COMMIT = '6fce8989e2c57afa033b5ee982aaa201eae04ac6'
TOOLS = (
    'check-benchmark-storage.mjs', 'check-lanes.mjs', 'check-maintenance.mjs',
    'check-native-benchmark.mjs', 'check-offered.mjs',
    'check-publication-timings.mjs', 'check-rounded-rate.mjs', 'check-joined.mjs',
)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(path):
    return json.loads(path.read_text())


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def source_proof():
    publication = load(JOB / 'PUBLICATION.json')
    assert publication['commit'] == COMMIT
    for key in ('actual_push_exit', 'qualified_main_exit'):
        assert type(publication[key]) is int and publication[key] == 0, key
    assert (JOB / 'STATUS').read_text().strip() == '0', 'Build must actually terminate0'
    assert (JOB / 'STAGE').read_text().strip() == 'DONE'
    source = load(JOB / 'SOURCE.json')
    assert source['commit'] == COMMIT and source['inputs'] == len(source['sha256'])
    directory = JOB / 'source'
    actual = {p.relative_to(directory).as_posix() for p in directory.rglob('*') if p.is_file()}
    assert actual == set(source['sha256'])
    for name, value in source['sha256'].items():
        path = directory / name
        assert not path.is_symlink() and sha(path) == value, name
        assert oct(stat.S_IMODE(path.stat().st_mode)) == source['physical_modes'][name], name
    assert 'rust-toolchain.toml' in source['sha256']
    for phase in ('before', 'after'):
        assert f"exact {source['inputs']} committed source hashes and physical modes passed" in (
            JOB / f'logs/source-{phase}.log').read_text()
        lines = (JOB / f'logs/source-{phase}-full.log').read_text().splitlines()
        assert len(lines) == source['inputs'] and all(line.endswith(': OK') for line in lines)
    pin = tomllib.loads((directory / 'rust-toolchain.toml').read_text())['toolchain']['channel']
    assert f'release: {pin}\n' in (JOB / 'logs/rustc.txt').read_text()
    for row in (JOB / 'logs/tool-binaries.sha256').read_text().splitlines():
        expected, path = row.split(maxsplit=1)
        assert sha(Path(path.strip())) == expected, path
    return source


def artifact_proof(source, handle):
    events = [json.loads(line) for line in (JOB / 'logs/native-release.jsonl').read_text().splitlines()
              if line.startswith('{')]
    artifacts = load(JOB / 'ARTIFACTS.json')
    assert {record['target'] for record in artifacts} == {'voteboat', 'native_benchmark'}
    assert len(artifacts) == 2
    for artifact in artifacts:
        record = artifact['compiler_record']
        assert record in events and record['fresh'] is False
        assert Path(record['manifest_path']).resolve() == JOB / 'source/Cargo.toml'
        assert record['features'] == ['default', 'native', 'quic', 'tls']
        profile = record['profile']
        assert profile['opt_level'] == '3' and profile['debuginfo'] == 0
        assert profile['debug_assertions'] is True and profile['overflow_checks'] is True
        assert profile['test'] is False
        original, copied = Path(artifact['original']), Path(artifact['copy'])
        assert original.resolve().is_relative_to(JOB / 'target-native-release')
        assert copied.resolve().is_relative_to(JOB / 'artifacts/native')
        assert sha(original) == sha(copied) == artifact['sha256']
        rel = Path(record['target']['src_path']).relative_to(JOB / 'source').as_posix()
        assert sha(JOB / 'source' / rel) == source['sha256'][rel]
    rustc_lines = (JOB / 'logs/native-root-rustc-lines.txt').read_text().splitlines()
    assert len(rustc_lines) == 2
    verbose = (JOB / 'logs/native-release.stderr').read_text()
    for line in rustc_lines:
        assert line in verbose
        assert '-C opt-level=3' in line and '-C debug-assertions=on' in line
        for forbidden in ('-C lto=', '-C codegen-units=', '-C linker-plugin-lto',
                          '-C overflow-checks=off', '--cfg voteboat_checkpoint_debug_oracle'):
            assert forbidden not in line, forbidden
    symbols = load(JOB / 'SYMBOLS.json')
    scripts = [event for event in events if event.get('reason') == 'build-script-executed'
               and event.get('package_id', '').endswith('#voteboat@0.1.0')]
    builds = [event for event in events if event.get('reason') == 'compiler-artifact'
              and event.get('target', {}).get('name') == 'build-script-build'
              and event.get('package_id', '').endswith('#voteboat@0.1.0')]
    assert len(scripts) == len(builds) == 1 and builds[0]['fresh'] is False
    assert 'voteboat_checkpoint_debug_oracle' not in scripts[0]['cfgs']
    assert symbols['root_buildscript_record'] == scripts[0]
    assert symbols['root_buildscript_fresh_record'] == builds[0]
    assert symbols['reference_oracle_cfg_absent_PROFILErelease'] is True
    for stem, key in [('elf-notes', 'elf_notes_sha256'), ('elf-sections', 'elf_sections_sha256'),
                      ('symbol-table', 'symbol_table_sha256')]:
        assert (JOB / f'logs/{stem}.exit').read_text().strip() == '0'
        assert sha(JOB / f'logs/{stem}.log') == symbols[key]
    assert '.debug_info' not in (JOB / 'logs/elf-sections.log').read_text()
    executable = next(record for record in artifacts if record['target'] == 'native_benchmark')
    assert symbols['executable_sha256'] == executable['sha256']
    tools = ('rustc.txt', 'cargo.txt', 'protoc.txt', 'tool-binaries.sha256')
    same_tools = all((JOB / 'logs' / name).read_bytes() == (TEMPLATE / 'logs' / name).read_bytes()
                     for name in tools)
    assert same_tools, 'Disclose and review any within-host compiler/tool difference before pilots'
    return executable, {
        'original_build_handle': handle, 'actual_exit': 0, 'source_commit': COMMIT,
        'source_inputs': source['inputs'], 'root_example_fresh_false': True,
        'root_buildscript_fresh_false': True,
        'profile': 'opt3/debug0/assert+overflow/nativeDEFAULTLTO+cgu',
        'withinhost_compiler_tools_exact_native463': same_tools,
        'source_before_after_guards0': True,
        'actual_rustc_lines_sha256': sha(JOB / 'logs/native-root-rustc-lines.txt'),
        'cargo_json_sha256': sha(JOB / 'logs/native-release.jsonl'),
        'source_inventory_sha256': sha(JOB / 'SOURCE.json'),
        'release_oracle_proof': symbols,
        'handle_provenance': 'Original handle supplied by root; terminal STATUS0 and DONE read from original build job.',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-handle', type=int, required=True)
    args = parser.parse_args()
    assert args.build_handle > 0
    for name in ('PROFILE_PROOF.json', 'PILOT_CONFIG.json', 'control-tools', 'controls', 'run-pilots.py'):
        assert not (JOB / name).exists(), ('Refuse overwrite of prior/partial pilot preparation', name)
    source = source_proof()
    executable, proof = artifact_proof(source, args.build_handle)
    write(JOB / 'PROFILE_PROOF.json', proof)
    tools = JOB / 'control-tools'
    tools.mkdir()
    inventory = {}
    for name in TOOLS:
        original = JOB / 'source/validation' / name
        copied = tools / name
        shutil.copy2(original, copied)
        assert sha(copied) == source['sha256'][f'validation/{name}']
        inventory[name] = {'sha256': sha(copied), 'mode': oct(stat.S_IMODE(copied.stat().st_mode)),
                           'committed_path': f'validation/{name}'}
    write(tools / 'SOURCE.json', {'source_commit': COMMIT, 'files': inventory})
    (JOB / 'controls').mkdir()
    resources = TEMPLATE / 'controls/tcp-r0-metadata/resources.py'
    cases = []
    for protocol, replies in [('tcp', 0), ('tcp', 2), ('quic', 0), ('quic', 2)]:
        name = f'{protocol}-r{replies}'
        metadata = JOB / f'controls/{name}-metadata'
        metadata.mkdir()
        shutil.copy2(resources, metadata / 'resources.py')
        assert sha(metadata / 'resources.py') == sha(resources)
        wal = JOB / f'controls/{name}-storage'
        assert not wal.exists()
        command = [executable['copy'], str(wal), protocol, '128', '64', '1', '--offered', '500',
                   '--ready-items', '16', '--ready-replies', str(replies)]
        cases.append({
            'name': name, 'metadata': str(metadata), 'wal_root': str(wal), 'command': command,
            'resource_command': ['python3', str(metadata / 'resources.py'), *command],
            'check_command': ['node', str(tools / 'check-native-benchmark.mjs'), '--storage', str(wal)],
            'scope': 'Functional CLI controls only;128offers/rate500/window64/onegroup/warm64/noMaintenance/pause; exactoriginals/recovery/newcounterchecker. No rate/capacity claim.',
        })
    artifact = dict(executable)
    artifact['compiled_source_sha256'] = source['sha256']['examples/native_benchmark.rs']
    artifact['manifest_sha256'] = source['sha256']['Cargo.toml']
    write(JOB / 'PILOT_CONFIG.json', {
        'candidate_source_commit': COMMIT, 'artifact': artifact,
        'profile_proof_sha256': sha(JOB / 'PROFILE_PROOF.json'),
        'control_tools': {'directory': str(tools), 'inventory_sha256': sha(tools / 'SOURCE.json'),
                          'files': inventory},
        'cases': cases, 'no_long_campaign': True,
        'quiet_coordination': 'HELD: root must separately perform actual socket/process preflight and grant quiet. This setup does not authorize launch.',
    })
    shutil.copy2(TEMPLATE / 'run-pilots.py', JOB / 'run-pilots.py')
    assert sha(JOB / 'run-pilots.py') == sha(TEMPLATE / 'run-pilots.py')
    write(JOB / 'PILOT_PREPARATION.json', {
        'commit': COMMIT, 'script_sha256': sha(Path(__file__)),
        'profile_proof_sha256': sha(JOB / 'PROFILE_PROOF.json'),
        'pilot_config_sha256': sha(JOB / 'PILOT_CONFIG.json'),
        'controller_sha256': sha(JOB / 'run-pilots.py'), 'resources_sha256': sha(resources),
        'actual_build_terminal_exit': 0, 'pilot_started': False,
        'socket_preflight_performed': False, 'quiet_granted': False,
    })
    print(json.dumps({'prepared': str(JOB), 'cases': 4, 'pilot_started': False}))


if __name__ == '__main__':
    main()
