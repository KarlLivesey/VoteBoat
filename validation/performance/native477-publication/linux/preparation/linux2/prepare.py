"""After actual476 readiness only; prepares data and guards, never runs a workload."""
import argparse, ast, os, platform, re, shutil, sys
from pathlib import Path
from common import candidate_record, guards, host_snapshot, load, proof, save, sha
from plan import cases

parser = argparse.ArgumentParser()
parser.add_argument('--candidate', required=True)
parser.add_argument('--host', choices=['linux','mac'], required=True)
parser.add_argument('--job', required=True)
parser.add_argument('--wal-parent', required=True)
parser.add_argument('--source-commit', required=True)
parser.add_argument('--publication-authority', required=True)
parser.add_argument('--schema', required=True)
args = parser.parse_args()
assert (platform.system() == 'Darwin') == (args.host == 'mac')
candidate = Path(args.candidate).resolve()
# Check actual publication/readiness/source proof before creating any campaign path.
record = candidate_record(candidate, args.host, args.source_commit, Path(args.publication_authority).resolve())
job = Path(args.job).resolve(); wal = Path(args.wal_parent).resolve()
assert not job.exists() and not wal.exists(), 'No overwrite/restart: choose fresh campaign paths'
assert wal.parent.is_dir(), 'Select an existing volume directory; never create a mount/partition'
job.mkdir(parents=True)
save(job/'PREPARING.json', {'candidate':str(candidate),'source_commit':args.source_commit,'host':args.host,
                          'scope':'Setup only; no timing authorization, workload, build or dependency update.'})
scripts = ['common.py','plan.py','prepare.py','campaign.py','resources.py']
for name in scripts:
    source = Path(__file__).resolve().parent/name
    ast.parse(source.read_text())
    shutil.copy2(source, job/name)
shutil.copy2(args.schema,job/'SCHEMA.md')
tools = job/'control-tools'; tools.mkdir()
source_root = Path(record['source']['root'])
pending = [source_root/'validation/check-native-benchmark.mjs']; closure = set()
while pending:
    source = pending.pop().resolve()
    if source in closure: continue
    assert source.parent == source_root/'validation', source
    closure.add(source)
    for imported in re.findall(r"from\s+['\"](\./[^'\"]+)['\"]",source.read_text()):
        pending.append(source.parent/imported)
assert {'check-publication-profile.mjs','check-publication-timings.mjs','check-benchmark-storage.mjs','check-offered.mjs'} <= {p.name for p in closure}
for source in sorted(closure):
    assert sha(source) == record['source']['files'][source.relative_to(source_root).as_posix()]
    shutil.copy2(source,tools/source.name)
metadata = job/'campaign-metadata'; metadata.mkdir(); wal.mkdir()
exe = next(a['copy'] for a in record['artifacts'] if a['target']=='native_benchmark')
order = cases(exe,wal,metadata,sys.executable,job/'resources.py')
for case in order: Path(case['metadata']).mkdir()
node = shutil.which('node'); assert node
config = {'scope':'477 finite paired publication diagnostic, same476 executable/native opt3 debug0 assertions/overflow/defaultLTO+codegen; three voters share one volume/device. No fullP9 or three-filesystem acceptance claim.',
          'source_commit':args.source_commit,'host':args.host,'job':str(job),'wal_parent':str(wal),
          'candidate':record,'runtime_tools':[proof(Path(sys.executable).resolve()),proof(Path(node).resolve())],
          'python':str(Path(sys.executable).resolve()),'node':str(Path(node).resolve()),
          'scripts':{name:sha(job/name) for name in scripts},
          'schema_sha256':sha(job/'SCHEMA.md'),'validators':{p.name:sha(p) for p in tools.iterdir()},
          'cases':order,'failure_policy':'Continue all fixed cases after a child nonzero exit, never retry or replace. Controller interruption stops with partial ledger. Guard failure refuses launch; postguard/readback/validator failure marks entire campaign failed.',
          'existing_native_drain_guard_seconds':120,'all_workloads_end_before_validation':True}
guards(config)
snapshot = host_snapshot(args.host,wal); save(job/'setup-host.json',snapshot)
config['device_identity'] = snapshot['device_identity']; config['cpu_policy'] = snapshot['cpu_policy']
save(job/'CAMPAIGN_CONFIG.json',config)
(job/'ORDER.txt').write_text('\n'.join(c['name'] for c in order)+'\n')
save(job/'READY.json',{'campaign_config_sha256':sha(job/'CAMPAIGN_CONFIG.json'),
                       'source_commit':args.source_commit,'cases':48,'pairs':24,
                       'artifact_sha256':next(a['sha256'] for a in record['artifacts'] if a['target']=='native_benchmark'),
                       'scope':'PREPARED ONLY: no workload launched. Requires root review, completed all-host setup/publication and separate explicit quiet-window grant.'})
print('PREPARED48_NO_TIMING',job)
