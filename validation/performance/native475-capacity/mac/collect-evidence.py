import hashlib, json, tarfile, time
from pathlib import Path

job = Path('/tmp/voteboat-mac475-dfc195c-Fz9Ivq').resolve()
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
assert (job/'STATUS').read_text().strip() == '0'
assert (job/'STAGE').read_text().strip() == 'DONE'
assert (job/'sweep/CAMPAIGN_STATUS').read_text().strip() == '0'
assert (job/'sweep/CAMPAIGN_STAGE').read_text().strip() == 'ALL_TIMING_END'
manifest = json.loads((job/'SOURCE.json').read_text())
assert manifest['commit'] == 'dfc195cd39196fc6f16e69ea74de34dabd359ef2'
assert len(manifest['sha256']) == 1474 and len(manifest['directory_modes']) == 318
for name, digest in manifest['sha256'].items():
    p = job/'source'/name
    assert sha(p) == digest and oct(p.stat().st_mode&0o777) == manifest['modes'][name], name
for name, mode in manifest['directory_modes'].items():
    assert oct((job/'source'/name).stat().st_mode&0o777) == mode, name
artifacts = json.loads((job/'ARTIFACTS.json').read_text())
for a in artifacts:
    assert sha(Path(a['copy'])) == sha(Path(a['original'])) == a['sha256']
    assert a['compiler_record']['fresh'] is False
campaign = json.loads((job/'sweep/campaign/CAMPAIGN.json').read_text())
assert len(campaign['cases']) == 54
for case in campaign['cases']:
    assert case['exit'] == case['validator_exit'] == 0
    assert (job/'sweep/campaign'/(case['name']+'.exit')).read_text().strip() == '0'
    assert (job/'sweep/campaign'/(case['name']+'-check.exit')).read_text().strip() == '0'

selected = []
for p in job.rglob('*'):
    if not p.is_file(): continue
    rel = p.relative_to(job)
    parts = rel.parts
    if parts[0] in ('source', 'target-checks', 'target-matched-checks', 'target-native-release', 'test-tmp', 'artifacts'): continue
    if p.name in ('source.tar', 'COLLECTED.tar.gz', 'COLLECTED.json', 'collect-evidence.py'): continue
    if 'campaign-wal' in parts or parts[0] == 'control-wal':
        if p.suffix not in ('.csv', '.txt'): continue
    # Controls root is named by controls.py; only plain result ledgers transfer.
    if p.name.startswith(('WAL', 'MANIFEST', 'CURRENT')): continue
    selected.append(p)
selected.sort()
assert all(p.suffix not in ('.rlib', '.rmeta', '.o') for p in selected)
inventory = {str(p.relative_to(job)): {'sha256': sha(p), 'bytes': p.stat().st_size, 'mode': oct(p.stat().st_mode&0o777)} for p in selected}
receipt = {'scope': 'Post-terminal read-only evidence collection, no source/build/binary/WAL transfer or rerun. Original interactive handles unavailable; actual remote terminal markers and all54 exits retained.', 'collected_unix': time.time(), 'qualification_status': 0, 'qualification_stage': 'DONE', 'campaign_status': 0, 'campaign_stage': 'ALL_TIMING_END', 'cases': 54, 'source_files': 1474, 'source_directories': 318, 'original_qualification_handle': 8849, 'original_campaign_handle': 29711, 'files': inventory}
(job/'COLLECTED.json').write_text(json.dumps(receipt, indent=2)+'\n')
with tarfile.open(job/'COLLECTED.tar.gz', 'w:gz') as archive:
    for p in selected+[job/'COLLECTED.json']:
        archive.add(p, arcname=str(p.relative_to(job)), recursive=False)
assert all(sha(job/name) == r['sha256'] for name,r in inventory.items())
print(json.dumps({'files': len(selected), 'raw_bytes': sum(r['bytes'] for r in inventory.values()), 'archive_sha256': sha(job/'COLLECTED.tar.gz'), 'archive_bytes': (job/'COLLECTED.tar.gz').stat().st_size, 'manifest_sha256': sha(job/'COLLECTED.json')}))
