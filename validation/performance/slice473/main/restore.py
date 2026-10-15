from pathlib import Path
import hashlib,json,subprocess,stat,datetime
repo=Path.cwd();job=repo/'target/p9/slice473';plan=json.loads((job/'PLAN.json').read_text());base=plan['baseline']
sha=lambda b:hashlib.sha256(b).hexdigest()
def git(*args):return subprocess.check_output(['git',*args])
assert not (job/'RESTORED.json').exists()
assert len(plan['paths'])==84 and sum(r['action']=='remove_experiment_added_module' for r in plan['paths'])==16
# Validate every allowlisted current byte and restore blob before any mutation.
blobs={}
for r in plan['paths']:
 p=repo/r['path'];assert p.is_file() and not p.is_symlink()
 assert sha(p.read_bytes())==r['before_sha256'] and oct(stat.S_IMODE(p.stat().st_mode))==r['before_mode'],r['path']
 if r['action']=='restore_baseline_blob':
  b=git('show',base+':'+r['path']);assert sha(b)==r['baseline_sha256'];blobs[r['path']]=b
 else:assert r['action']=='remove_experiment_added_module'
preserved=['validation/check-benchmark-storage.mjs','validation/check-benchmark-storage.test.mjs']
before_preserve={p:sha((repo/p).read_bytes()) for p in preserved}
rows=[]
for r in plan['paths']:
 p=repo/r['path']
 if r['action']=='restore_baseline_blob':p.write_bytes(blobs[r['path']])
 else:p.unlink()
 rows.append({'path':r['path'],'action':r['action'],'after_sha256':sha(p.read_bytes()) if p.exists() else None})
assert all(sha((repo/p).read_bytes())==h for p,h in before_preserve.items())
# All tracked production/test/example/build/lock/framework/contract inputs, not just changed paths.
prefixes=('src/','tests/','examples/','benchmarks/matched/src/','.cargo/','.githooks/')
fixed={'Cargo.toml','Cargo.lock','clippy.toml','build.rs','README.md','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock','benchmarks/matched/README.md','benchmarks/matched/SCHEMA.md','benchmarks/matched/COMPARISON.md','docs/component-contracts.json','docs/provider-conformance.json','validation/check-provider-conformance.test.mjs'}
entries=[]
for line in git('ls-tree','-r',base).decode().splitlines():
 meta,name=line.split('\t');mode,kind,oid=meta.split()
 if name not in fixed and not name.startswith(prefixes):continue
 p=repo/name;assert p.is_file() and not p.is_symlink(),name
 b=git('cat-file','blob',oid);assert p.read_bytes()==b,name
 actual=stat.S_IMODE(p.stat().st_mode);expected=0o755 if mode=='100755' else (0o600 if actual==0o600 else 0o644)
 assert actual==expected,(name,oct(actual),oct(expected))
 entries.append({'path':name,'sha256':sha(b),'git_mode':mode,'physical_mode':oct(actual)})
expected={r['path'] for r in entries}
for prefix in prefixes:
 for p in (repo/prefix).rglob('*'):
  if p.is_file():assert str(p.relative_to(repo)) in expected, str(p)
assert not git('diff',base,'--','src','benchmarks/matched/src')==b'' # index still priorHEAD, worktree diff includes restored test-only controls.
# Production differs from455 only in retained test-only additions; executable bodies require explicit review separately.
report={'baseline':base,'prior_head':plan['head'],'evidence_commit':git('rev-parse','HEAD').decode().strip(),'completed':datetime.datetime.now(datetime.timezone.utc).isoformat(),'restored_paths':rows,'restored':68,'removed':16,'baseline_projection_entries':len(entries),'all_baseline_projection_bytes_modes':'PASS','preserved471':before_preserve,'files':entries}
(job/'RESTORED.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({k:v for k,v in report.items() if k not in ['files','restored_paths','preserved471']}))
