"""Seal collected Mac476 evidence; preserve original bytes via PACKING.json."""
from pathlib import Path
import gzip
import hashlib
import json
import re

job = Path(__file__).resolve().parent
raw = job/'collection/raw'
destination = Path('validation/performance/native476-publication/mac')
sha = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
source = json.loads((raw/'SOURCE.json').read_text())
qualified = json.loads((job/'ROOT_FINAL_INPUTS.json').read_text())
assert source['commit'] == qualified['published_commit'] == '7e9068296ce00dde5bf3e03bf4d852ed2833d185'
assert len(source['sha256']) == len(qualified['files']) == 1491
assert set(source['sha256']) == set(qualified['files'])
for name, record in qualified['files'].items():
    assert source['sha256'][name] == record['sha256'] and source['modes'][name] == record['mode'], name
assert len(source['directory_modes']) == 323
assert len(source['protected_fixture_keys']) == 7
assert all(source['modes'][name] == '0o600' for name in source['protected_fixture_keys'])
assert (raw/'STATUS').read_text().strip() == '0' and (raw/'STAGE').read_text().strip() == 'DONE'
assert json.loads((job/'RUN.exit.json').read_text())['exit'] == 0
assert all(path.read_text().strip() == '0' for path in (raw/'logs').glob('*.exit'))
for path in (raw/'logs').glob('*.exit.json'):
    assert json.loads(path.read_text())['exit'] == 0, path

def counts(name):
    return [int(value) for value in re.findall(r'test result: ok\. (\d+) passed; 0 failed;', (raw/'logs'/name).read_text())]
integration = counts('selected_tests.stdout')
library = counts('native_library.stdout')
example = counts('publication_example.stdout')
assert len(integration) == 7 and min(integration) > 0 and sum(integration) == 197
assert library == [35]
assert example == [5]
root_selected = Path('target/p9/native476-publication/core/selected.stdout')
root_counts = [int(value) for value in re.findall(r'test result: ok\. (\d+) passed; 0 failed;', root_selected.read_text())]
assert integration == root_counts, (integration, root_counts)
artifacts = json.loads((raw/'ARTIFACTS.json').read_text())
symbols = json.loads((raw/'SYMBOLS.json').read_text())
assert symbols['reference_oracle_cfg_absent_PROFILErelease']
assert symbols['root_buildscript_fresh_record']['fresh'] is False
for artifact in artifacts:
    record = artifact['compiler_record']
    assert record['fresh'] is False
    assert record['profile'] == {'opt_level': '3', 'debuginfo': 0, 'debug_assertions': True, 'overflow_checks': True, 'test': False}
controls = json.loads((raw/'CONTROLS.json').read_text())
assert len(controls['cases']) == 4 and controls['artifact_unchanged']
assert all(case['process']['exit'] == case['validator']['exit'] == 0 for case in controls['cases'])
case_summary = [{key: case['summary'][key] for key in
    ['protocol', 'publication_effective', 'offers', 'applied', 'recovered_value', 'retry_verified', 'workers_joined']}
    for case in controls['cases']]
proof = {'scope': 'Published476 Mac qualification and fresh native artifact. Functional controls only, not throughput improvement or physical power-loss proof.',
    'source_commit': source['commit'], 'source_files': 1491, 'source_directories': 323,
    'source_sha256_and_file_directory_modes_exact': True, 'source_root_mode': source['source_root_mode'], 'private_keys at 0600': 7,
    'original_qualification_handle': 91880, 'original_source_transfer_handle': 20500,
    'actual_qualification_exit': 0, 'actual_remote_status': 0, 'actual_remote_stage': 'DONE', 'restarted': False,
    'integration_counts_in_cargo_order': integration, 'integration_total': sum(integration), 'integration_counts_match_exact_linux_source_command': True,
    'native_log_library_count': library[0], 'publication_example_count': 5,
    'format_four_strict_profiles_and_matched_format_strict_exit': 0,
    'fresh_root_executable_buildscript': True, 'root_artifact_sha256': next(a['sha256'] for a in artifacts if a['target'] == 'voteboat'),
    'executable_sha256': next(a['sha256'] for a in artifacts if a['target'] == 'native_benchmark'),
    'native_profile': 'opt3/debug0/assertions+overflow; native defaultLTO/codegen/split; actualCargo JSON and verbose rustc output; rootbuildscript PROFILE=release; no debugcheckpointoracle',
    'compiler': 'stable1.98.0 aarch64-apple-darwin LLVM22.1.8; protoc3.21.12; byte-identical retainedMac455 tools; Linux 1.98.1 distinct',
    'macho_uuid': symbols['uuid'], 'cases': case_summary,
    'diagnostic_scope': 'Ordinary publication primitives only; cold repair/initial selector/reclamation excluded. Upper+primitive errors are overlapping observations.',
    'transfer_scope': 'Initial source-only transfer and later post-terminal metadata/plainresults collection are separate. No binaries/buildtrees/fullsource/WAL in this archive.',
    'open_acceptance': 'Paired native gains on Btrfs/APFS/real ext4-or-XFS, all fullP9 requirements and default promotion remain open.'}
(job/'collection/QUALIFICATION_REVIEW.json').write_text(json.dumps(proof, indent=2)+'\n')

assert not destination.exists(), 'refuse replacing sealed evidence'
destination.mkdir(parents=True)
payloads = {str(path.relative_to(raw)): path for path in raw.rglob('*') if path.is_file()}
for name in ['PREFLIGHT.json', 'PREFLIGHT.stdout', 'PREFLIGHT.stderr', 'PREFLIGHT.command.json', 'PREFLIGHT.exit.json',
             'PREPARATION.json', 'PREPARATION_CHECKS.json', 'PREPARATION_CHECKS2.json', 'ROOT_FINAL_INPUTS.json',
             'SOURCE_EXPORT.json', 'TRANSFER.json', 'RUN.command.json', 'RUN.stdout', 'RUN.stderr', 'RUN.exit.json',
             'REMOTE_JOB', 'prepare-source.py', 'preflight.py', 'collect-evidence.py', 'archive-evidence.py']:
    payloads['preparation/'+name] = job/name
for prefix in ['prepare-sentinel', 'prepare-sentinel2', 'run-sentinel', 'run-sentinel2', 'create-job', 'source-transfer', 'extract-verify', 'native-library-live-tail']:
    for path in job.glob(prefix+'.*'):
        payloads['preparation/'+path.name] = path
for name in ['LOCAL_TRANSFER_PROOF.json', 'QUALIFICATION_REVIEW.json', 'remote-export.command.json', 'remote-export.stdout', 'remote-export.stderr']:
    payloads['collection/'+name] = job/'collection'/name
for path in (job/'collection').glob('*'):
    if path.is_file() and (path.name.endswith('.stdout') or path.name.endswith('.stderr') or path.name.endswith('.command.json')):
        payloads['collection/'+path.name] = path
packing = []
for relative, original in sorted(payloads.items()):
    content = original.read_bytes()
    saved = relative+'.gz' if len(content) > 65536 else relative
    target = destination/saved
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(gzip.compress(content, mtime=0) if saved != relative else content)
    assert (gzip.decompress(target.read_bytes()) if saved != relative else target.read_bytes()) == content
    if saved != relative:
        packing.append({'original_path': relative, 'archived_path': saved, 'original_bytes': len(content), 'original_sha256': sha(original)})
(destination/'PACKING.json').write_text(json.dumps(packing, indent=2)+'\n')
lines = ['#476 Mac qualification and fresh native artifact', '',
    'Published source 7e9068296ce00dde5bf3e03bf4d852ed2833d185. Original SSH handle 91880 actually terminated with exit 0 and remote STATUS 0 / STAGE DONE; it was never restarted. Commands, raw outputs and exits retain the affected integration suites, native log-store library controls, five focused publication/diagnostic example tests, JavaScript validators, provider metadata/negative controls, formatting/four strict profiles and separate matched formatting/strict. No broad unchanged P8 suite or framework performance build is claimed.', '',
    f'All 1491 selected files and 323 source directories preserve exact hashes/modes, source root 0755 and seven private fixture keys at 0600. Integration test counts match the exact Linux source command: {integration}; native log-store library controls: {library[0]}. Source guards run before, after and at terminal cleanup.', '',
    'Cargo JSON and verbose rustc output prove freshly compiled root library, executable and root buildscript with native opt3/debug0/assertions and overflow checks/native default LTO and codegen. The actual root buildscript invocation uses PROFILE=release. Mac stable 1.98.0 / LLVM 22.1.8 / protoc 3.21.12 bytes match the retained within-host baseline; Linux 1.98.1 remains distinct. Original/copied Mach-O hashes and UUID/symbol evidence are retained, without a framework debug2 profile or invented dSYM requirement.', '',
    '| Protocol | Selected provider | Offers | Applied originals | Recovered including 64 warmups |',
    '|---|---|---:|---:|---:|']
for case in case_summary:
    lines.append(f"| {case['protocol']} | {case['publication_effective']} | {case['offers']} | {case['applied']} | {case['recovered_value']} |")
lines += ['', 'Four actual release functional controls use a 4096 window, the same selected 4096 admission profile, 128 offers/s, ready 16 and 2 s maintenance. Each retains six capacity and six provider Create/Recover readbacks, exact original-operation history, retries and worker joins. These controls do not establish a throughput gain. All three voters share the same APFS Data volume, container and internal SSD, not independent placement.', '',
    'Publication primitive timing covers ordinary barriers; cold repair, initial selector and reclamation namespace work are excluded. Witness zero ordinary namespace calls does not mean zero whole-process namespace operations. Error counts are explicitly overlapping upper and primitive observations. Raw byte counts and zero-error checks are retained.', '',
    'Unpublished guards intentionally refused preparation/runner startup before publication; their original exits/stdout/stderr are preserved. Actual Mac qualification and four controls have no failed/replaced workload. Post-terminal collection excludes source trees, binaries, buildtrees and generated WAL; those remain on the Mac. Large originals are losslessly gzip-packed with original hashes in PACKING.json. SHA256SUMS seals this archive. All three filesystem gain acceptance and full P9 remain open.']
(destination/'README.md').write_text('\n'.join(lines)+'\n')
manifest = ''.join(f'{sha(path)}  {path.relative_to(destination)}\n' for path in sorted(destination.rglob('*')) if path.is_file())
(destination/'SHA256SUMS').write_text(manifest)
print(json.dumps({'archive': str(destination), 'payloads': len(list(destination.rglob('*'))) - len([p for p in destination.rglob('*') if p.is_dir()]) - 1,
    'packed': len(packing), 'manifest_sha256': sha(destination/'SHA256SUMS'), 'proof': proof}, indent=2))
