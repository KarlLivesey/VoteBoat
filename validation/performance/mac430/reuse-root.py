"""Certify exact source/flags scope before reusing passed Mac429 root checks."""
import hashlib
import json
import stat
from pathlib import Path

root = Path(__file__).resolve().parent
baseline = Path('/tmp/voteboat-mac429-832479d-4o73Bl')
source = json.loads((root/'SOURCE.json').read_text())
old = json.loads((baseline/'SOURCE.json').read_text())
assert source['commit'] == '9d2b7852862a633e188afb38de8e2d3578efb5ca'
assert old['commit'] == '832479d7dc11db176549baea422b98b00b3e01dc'
assert (baseline/'STATUS').read_text().strip() == '0' and (baseline/'STAGE').read_text().strip() == 'DONE'
h = lambda path: hashlib.sha256(path.read_bytes()).hexdigest()
keys = {p for p in old['sha256'] if not p.startswith('benchmarks/matched/')}
new_keys = {p for p in source['sha256'] if not p.startswith('benchmarks/matched/')}
assert keys == new_keys and len(keys) == 1404
for rel in sorted(keys):
    assert old['sha256'][rel] == source['sha256'][rel], rel
    assert old['modes'][rel] == source['modes'][rel], rel
    for job, inventory in [(root, source), (baseline, old)]:
        path = job/'source'/rel
        assert h(path) == inventory['sha256'][rel], rel
        assert oct(stat.S_IMODE(path.stat().st_mode)) == inventory['modes'][rel], rel
# The isolated production root check inventory excludes three contextual files.
root_checked = keys - {'.gitignore', 'README.md', 'rust-toolchain.toml'}
assert len(root_checked) == 1401
steps = ['affected_all', 'affected_core', 'effect_owner_all', 'effect_owner_core',
         'services', 'root_fmt', 'strict_default', 'strict_all', 'strict_core', 'strict_native']
proofs = []
for step in steps:
    log = baseline/'logs'/(step+'.log')
    exit_file = baseline/'logs'/(step+'.exit')
    assert exit_file.read_text().strip() == '0', step
    proofs.append({'step':step, 'exit':0, 'log':str(log), 'log_sha256':h(log),
                   'exit_file':str(exit_file), 'exit_sha256':h(exit_file)})
assert (root/'logs/rustc.txt').read_bytes() == (baseline/'logs/rustc.txt').read_bytes()
qualification = json.loads((baseline/'QUALIFICATION.json').read_text())
record = {'baseline_commit':old['commit'], 'candidate_commit':source['commit'],
          'root_checked_inputs':len(root_checked), 'nonmatched_inventory_inputs':len(keys),
          'exact_sha256_modes':True, 'source_sha256':{p:source['sha256'][p] for p in sorted(keys)},
          'source_modes':{p:source['modes'][p] for p in sorted(keys)},
          'additional_context_inputs':['.gitignore','README.md','rust-toolchain.toml'],
          'baseline_qualification':{'path':str(baseline/'QUALIFICATION.json'), 'sha256':h(baseline/'QUALIFICATION.json')},
          'reused_steps':proofs, 'original_test_counts':qualification['test_counts'],
          'same_actual_rustc_version':True,
          'scope':'Mac429 root tests, formatting and four strict profiles passed on exact unchanged1401 checked root inputs; plus three contextual/config files exact. These scopes were not rerun on430. Matched input changes are independently tested/built.'}
(root/'ROOT_REUSE.json').write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps({'root_checked_inputs':len(root_checked), 'nonmatched_inventory_inputs':len(keys),
                  'all_reused_steps_exit':0, 'same_actual_rustc_version':True},sort_keys=True))
