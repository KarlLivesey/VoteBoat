import ast, hashlib, json, shutil, subprocess
from pathlib import Path

job = Path(__file__).resolve().parent
candidate = job.parent
assert (candidate / 'STATUS').read_text().strip() == '0'
assert (candidate / 'STAGE').read_text().strip() == 'DONE'
assert not (job / 'CAMPAIGN_CONFIG.json').exists()
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
subprocess.run(['/usr/bin/python3', str(candidate / 'verify-source.py')], check=True)
source = json.loads((candidate / 'SOURCE.json').read_text())
assert source['commit'] == 'dfc195cd39196fc6f16e69ea74de34dabd359ef2'
artifacts = json.loads((candidate / 'ARTIFACTS.json').read_text())
controls = json.loads((candidate / 'CONTROLS.json').read_text())
assert len(controls['cases']) == 2 and controls['artifact_unchanged']
for artifact in artifacts:
    assert artifact['compiler_record']['fresh'] is False
    assert sha(artifact['copy']) == sha(artifact['original']) == artifact['sha256']
config = json.loads((job / 'LINUX_CAMPAIGN_CONFIG.template.json').read_text())
config['scope'] = '475 finite offered-load capacity sweep on Mac APFS only, same immutable native475 artifact/commoncapacity4096 all54cases; not paired speed acceptance, sustained20M or three-filesystem proof. Three voters share one APFS volume/internal SSD.'
config['candidate'] = {
    'source_commit': source['commit'], 'source_root': str(candidate / 'source'),
    'source_inventory': str(candidate / 'SOURCE.json'), 'source_inventory_sha256': sha(candidate / 'SOURCE.json'),
    'artifact': next(a for a in artifacts if a['target'] == 'native_benchmark'),
    'root_artifact': next(a for a in artifacts if a['target'] == 'voteboat'),
    'compiler': {name: {'path': str(candidate / 'logs' / name), 'sha256': sha(candidate / 'logs' / name)} for name in ['rustc.txt', 'cargo.txt', 'tool-sha256.txt', 'protoc.txt', 'native-release-profile.txt']},
}
config['setup_dependencies'] = [{'path': str(candidate / name), 'sha256': sha(candidate / name)} for name in ['ARTIFACTS.json', 'SYMBOLS.json', 'CONTROLS.json', 'collect-artifacts.py', 'verify-source.py']]
tools = candidate / 'source/validation'
config['control_tools'] = {'directory': str(tools), 'files': {p.name: {'sha256': sha(p), 'mode': oct(p.stat().st_mode & 0o777)} for p in tools.glob('*.mjs')}}
metadata = job / 'campaign-metadata'
wal = job / 'campaign-wal'
assert not metadata.exists() and not wal.exists() and not (job / 'campaign').exists()
metadata.mkdir(); wal.mkdir()
config['wal_parent'] = str(wal)
for case in config['cases']:
    root = wal / case['name']
    meta = metadata / case['name']
    meta.mkdir()
    shutil.copyfile(job / 'resources.py', meta / 'resources.py')
    case.update(wal_root=str(root), metadata=str(meta), resource_collector_sha256=sha(meta / 'resources.py'))
    case['command'][:2] = [config['candidate']['artifact']['copy'], str(root)]
    case['resource_command'] = ['/usr/bin/python3', str(meta / 'resources.py'), *case['command']]
assert len(config['cases']) == 54
(job / 'CAMPAIGN_CONFIG.json').write_text(json.dumps(config, indent=2) + '\n')
(job / 'ORDER.txt').write_text('\n'.join(c['name'] for c in config['cases']) + '\n')
ast.parse((job / 'campaign.py').read_text())
ast.parse((job / 'resources.py').read_text())
ready = {'scope': 'SETUP_ONLY no workloads launched; rootquietgrant required', 'cases': 54, 'campaign_config_sha256': sha(job / 'CAMPAIGN_CONFIG.json'), 'campaign_driver_sha256': sha(job / 'campaign.py'), 'artifact_sha256': config['candidate']['artifact']['sha256'], 'source_files': len(source['sha256']), 'source_directories': len(source['directory_modes']), 'mac_ru_maxrss_unit': 'bytes', 'all_setup_ended': True}
(job / 'READY.json').write_text(json.dumps(ready, indent=2) + '\n')
print(json.dumps(ready, sort_keys=True))
