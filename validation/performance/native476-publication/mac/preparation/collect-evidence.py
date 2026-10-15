"""Post-terminal qualification collection, adapted from Mac475; no workloads."""
import hashlib
import json
import stat
import tarfile
import time
from pathlib import Path

job = Path(__file__).resolve().parent
sha = lambda path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
assert (job/'STATUS').read_text().strip() == '0'
assert (job/'STAGE').read_text().strip() == 'DONE'
source = json.loads((job/'SOURCE.json').read_text())
assert source['commit'] == (job/'REQUIRED_COMMIT').read_text().strip()
assert len(source['sha256']) == 1491
assert oct(stat.S_IMODE((job/'source').stat().st_mode)) == source['source_root_mode']
for name, digest in source['sha256'].items():
    path = job/'source'/name
    assert sha(path) == digest and oct(stat.S_IMODE(path.stat().st_mode)) == source['modes'][name], name
for name, mode in source['directory_modes'].items():
    assert oct(stat.S_IMODE((job/'source'/name).stat().st_mode)) == mode, name
assert len(source['protected_fixture_keys']) == 7
assert all(source['modes'][name] == '0o600' for name in source['protected_fixture_keys'])
artifacts = json.loads((job/'ARTIFACTS.json').read_text())
for artifact in artifacts:
    assert sha(Path(artifact['copy'])) == sha(Path(artifact['original'])) == artifact['sha256']
    assert artifact['compiler_record']['fresh'] is False
controls = json.loads((job/'CONTROLS.json').read_text())
assert len(controls['cases']) == 4 and controls['artifact_unchanged'] is True
for case in controls['cases']:
    assert case['process']['exit'] == case['validator']['exit'] == 0
    assert len(case['actual_capacity_readbacks']) == len(case['actual_publication_readbacks']) == 6

selected = []
for path in job.rglob('*'):
    if not path.is_file():
        continue
    relative = path.relative_to(job)
    if relative.parts[0] in ('source', 'target-checks', 'target-matched-checks', 'target-native-release', 'test-tmp', 'artifacts'):
        continue
    if path.name in ('source.tar', 'COLLECTED.tar.gz', 'COLLECTED.json', 'collect-evidence.py'):
        continue
    if relative.parts[0] == 'control-wal' and path.suffix not in ('.csv', '.txt'):
        continue
    selected.append(path)
selected.sort()
assert all(path.suffix not in ('.rlib', '.rmeta', '.o') for path in selected)
inventory = {str(path.relative_to(job)): {'sha256': sha(path), 'bytes': path.stat().st_size,
    'mode': oct(stat.S_IMODE(path.stat().st_mode))} for path in selected}
receipt = {'scope': 'Collection after original qualification SSH91880 actually terminated. No source/build/binary/WAL transfer or rerun.',
    'collected_unix': time.time(), 'qualification_status': 0, 'qualification_stage': 'DONE',
    'original_qualification_handle': 91880, 'cases': 4, 'commit': source['commit'],
    'source_files': len(source['sha256']), 'source_directories': len(source['directory_modes']), 'files': inventory}
(job/'COLLECTED.json').write_text(json.dumps(receipt, indent=2)+'\n')
with tarfile.open(job/'COLLECTED.tar.gz', 'w:gz') as archive:
    for path in selected+[job/'COLLECTED.json']:
        archive.add(path, arcname=str(path.relative_to(job)), recursive=False)
assert all(sha(job/name) == record['sha256'] for name, record in inventory.items())
print(json.dumps({'files': len(selected), 'raw_bytes': sum(record['bytes'] for record in inventory.values()),
    'archive_sha256': sha(job/'COLLECTED.tar.gz'), 'archive_bytes': (job/'COLLECTED.tar.gz').stat().st_size,
    'manifest_sha256': sha(job/'COLLECTED.json')}))
