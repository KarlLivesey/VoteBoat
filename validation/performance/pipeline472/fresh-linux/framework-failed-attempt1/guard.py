import json,hashlib,shutil,sys,subprocess,re
from pathlib import Path
h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
ESSENTIALS={'src/log/command_bytes.rs', 'tests/startup/append_window.rs', 'src/raft/append_window.rs', 'src/format_crc.rs', 'src/log/command_bytes/tests.rs', 'examples/benchmark/observe_barriers.rs', 'src/runtime/ready_batch/tests/append_window.rs', 'src/runtime/effects/append_window.rs', 'src/format_crc/tests.rs', 'src/native/startup/append_window.rs', 'examples/benchmark/append_window.rs', 'tests/provider_conformance/append_window.rs', 'tests/worker/append_window.rs', 'src/raft/connections/tests.rs', 'src/runtime/connections/tests.rs', 'rust-toolchain.toml', 'src/runtime/replica/pump.rs', 'src/runtime/ready_batch/tests/advance_budget.rs', 'tests/support/replica_driver/append_pump.rs', 'src/raft/append_window/tests.rs', 'src/runtime/append_window.rs'}
def qualification(job):
 commit=(job/'REQUIRED_COMMIT.txt').read_text().strip()
 assert re.fullmatch('[0-9a-f]{40}',commit), '472 unpublished: source export/build/setup refused'
 publication=json.loads((job/'PUBLICATION.json').read_text());assert publication['commit']==commit and publication['exit']==0
 proof=publication['proof'];assert h(proof['path'])==proof['sha256']
 published=json.loads(Path(proof['path']).read_text())
 assert published['commit']==commit and published['exit']==0 and published['remote_exit']==0 and published['remote_main']==commit
 hook=publication['hook'];assert subprocess.check_output(['git','config','--local','core.hooksPath'],text=True).strip()=='.githooks';assert h(hook['path'])==hook['sha256']
 q=json.loads((job/'FUNCTIONAL_QUALIFICATION.json').read_text());assert q['source_commit']==commit
 inv=q['source_inventory'];assert h(inv['path'])==inv['sha256'];normalized=json.loads(Path(inv['path']).read_text());qualified=normalized['files']
 assert (ESSENTIALS-{'rust-toolchain.toml'})<=set(qualified)
 raw=normalized['derived_from'];assert h(raw['path'])==raw['sha256'];assert json.loads(Path(raw['path']).read_text())==qualified
 evidence={}
 for name,p in q['proofs'].items():
  assert h(p['path'])==p['sha256'],name;evidence[name]=json.loads(Path(p['path']).read_text())
 for name in ['focused','validation','selected','core_matched_metadata','strict']:
  assert evidence[name]['exit']==0,name
  if 'commands' in evidence[name]:assert all(c['exit']==0 for c in evidence[name]['commands']),name
 for name in ['focused','validation','core_matched_metadata','strict']:assert evidence[name]['source_unchanged'],name
 assert evidence['selected']['same_selected_inputs']
 for name in ['attempt1','attempt2','baseline24','baseline27','baseline28']:assert evidence[name]['exit']==101,name
 for pair in q['final_source_guards']:
  for p in pair.values():assert h(p['path'])==p['sha256']
  assert json.loads(Path(pair['before']['path']).read_text())==json.loads(Path(pair['after']['path']).read_text())
 return commit,qualified
job=Path(sys.argv[1]).resolve();required_commit,qualified=qualification(job)
if sys.argv[2]=='qualification':
 print(json.dumps({'commit':required_commit,'qualified_inputs':len(qualified),'scope':'Changed472 driver pump qualified by actual focused/broad/core/matched/strict scopes; three baseline-reproduced failures remain failures.'}));sys.exit(0)
source=(job/'source').resolve();m=json.loads((job/'SOURCE.json').read_text());expected=m['sha256'];assert m['commit']==required_commit
actual={p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()};assert actual==set(expected),(actual-set(expected),set(expected)-actual)
assert ESSENTIALS<=set(expected)
for rel,digest in expected.items():assert h(source/rel)==digest,rel
for rel,mode in m['physical_modes'].items():assert oct((source/rel).stat().st_mode & 0o777)==mode,rel
for rel,item in qualified.items():assert expected[rel]==item['sha256'] and m['physical_modes'][rel]==item['mode'],rel
if sys.argv[2]=='source':
 print(f'exact{len(expected)} committed inputs +{len(qualified)} qualified input projection hashes/physical modes passed');sys.exit(0)
events=[json.loads(line) for line in (job/'logs/matched-release.jsonl').read_text().splitlines() if line.startswith('{')];artifacts=[]
for name,kind in [('voteboat','lib'),('voteboat-matched-framework','bin'),('openraft','lib'),('bench_minimal','lib')]:
 selected=[r for r in events if r.get('reason')=='compiler-artifact' and r.get('target',{}).get('name')==name and kind in r['target']['kind']]
 assert len(selected)==1,(name,len(selected));r=selected[0];p=r['profile'];assert r['fresh'] is False and p['opt_level']=='3' and p['debuginfo']==2 and p['debug_assertions'] and p['overflow_checks'] and not p['test'],r
 src=Path(r['target']['src_path']).resolve();manifest=Path(r['manifest_path']).resolve()
 if name in ['voteboat','voteboat-matched-framework']:
  rel=src.relative_to(source).as_posix();assert expected[rel]==h(src);assert manifest.is_relative_to(source)
  assert r['features']==[],r
 else:
  assert 'e88bb40707f2e511d8ecc44d31edcb1d814a11a8' in r['package_id'],r
  checkout=next(parent for parent in src.parents if (parent/'.git').exists());assert subprocess.check_output(['git','-C',str(checkout),'rev-parse','HEAD'],text=True).strip()=='e88bb40707f2e511d8ecc44d31edcb1d814a11a8'
  status=subprocess.check_output(['git','-C',str(checkout),'status','--porcelain'],text=True).splitlines()
  assert all(line=='?? .cargo-ok' for line in status),status
  # Cargo's checkout-completion marker is metadata, not a tracked source edit.
  (job/'logs/upstream-checkout-status.txt').write_text('\n'.join(status)+'\n')
  relative=src.relative_to(checkout).as_posix()
  assert h(src)==hashlib.sha256(subprocess.check_output(['git','-C',str(checkout),'show','HEAD:'+relative])).hexdigest()
 original=Path(next(path for path in r['filenames'] if path.endswith('.rlib'))) if kind=='lib' else Path(r['executable']);assert original.resolve().is_relative_to(job/'target-matched-release')
 digest=h(original);copy=job/'artifacts/matched'/original.name;copy.parent.mkdir(parents=True,exist_ok=True);assert not copy.exists() or h(copy)==digest
 if not copy.exists():shutil.copyfile(original,copy)
 assert h(copy)==digest;copy.chmod(0o555 if kind=='bin' else 0o444)
 artifacts.append({'kind':'matched','target':name,'original':str(original),'copy':str(copy),'sha256':digest,'compiler_record':r,'compiled_source_sha256':h(src),'manifest_sha256':h(manifest)})
lines=[line for line in (job/'logs/matched-release.stderr').read_text().splitlines() if '--crate-name voteboat ' in line or '--crate-name voteboat_matched_framework ' in line or '--crate-name openraft ' in line or '--crate-name bench_minimal ' in line];assert len(lines)==4
for line in lines:
 for flag in ['-C opt-level=3','-C debuginfo=2','-C debug-assertions=on','-C codegen-units=1','-C split-debuginfo=packed']:assert flag in line,(flag,line)
 assert '-C overflow-checks=off' not in line
assert any('-C lto=thin' in line for line in lines)
(job/'logs/matched-root-upstream-rustc-lines.txt').write_text('\n'.join(lines)+'\n');(job/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
print('freshroot/upstream/compiler/source/assertedthinLTO profile and original/copied artifact guards passed')

# PROFILE release omits only redundant full-reference cfg; ordinary assertions remain on.
root_scripts=[r for r in events if r.get('reason')=='build-script-executed' and r.get('package_id','').endswith('#voteboat@0.1.0')]
assert len(root_scripts)==1 and 'voteboat_checkpoint_debug_oracle' not in root_scripts[0]['cfgs'],root_scripts
root_lines=[line for line in lines if '--crate-name voteboat ' in line];assert len(root_lines)==1
assert '--cfg voteboat_checkpoint_debug_oracle' not in root_lines[0]
assert "--check-cfg 'cfg(voteboat_checkpoint_debug_oracle)'" in root_lines[0]
script_invocations=[line for line in (job/'logs/matched-release.stderr').read_text().splitlines() if 'PROFILE=release' in line and 'build-script-build' in line and 'CARGO_PKG_NAME=voteboat ' in line and f'CARGO_MANIFEST_PATH={source}/Cargo.toml ' in line]
assert len(script_invocations)==1,script_invocations
script_artifacts=[r for r in events if r.get('reason')=='compiler-artifact' and r.get('target',{}).get('name')=='build-script-build' and r.get('package_id','').endswith('#voteboat@0.1.0')]
assert len(script_artifacts)==1 and script_artifacts[0]['fresh'] is False
(job/'RELEASE_ORACLE.json').write_text(json.dumps({'build_rs_sha256':expected['build.rs'],'build_script_record':root_scripts[0],'build_script_compilation':script_artifacts[0],'actual_PROFILE_release_invocation':script_invocations[0],'actual_root_rustc':root_lines[0],'reference_cfg_absent':True,'ordinary_assertions_overflow_enabled':True,'profile':'release'},indent=2)+'\n')
# Copy packed debug sidecar alongside the immutable executable.
exe=next(r for r in artifacts if r['target']=='voteboat-matched-framework')
original=Path(exe['original']);dwp=Path(str(original)+'.dwp');assert dwp.is_file() and dwp.stat().st_size>0
copy=Path(str(exe['copy'])+'.dwp');assert not copy.exists();shutil.copyfile(dwp,copy);assert h(copy)==h(dwp);copy.chmod(0o444)
sidecar={'original':str(dwp),'copy':str(copy),'sha256':h(dwp),'bytes':dwp.stat().st_size,'executable_sha256':exe['sha256'],'cargo_filenames':exe['compiler_record']['filenames']}
(job/'SYMBOLS.json').write_text(json.dumps(sidecar,indent=2)+'\n')
for label,args in [('elf-notes',['readelf','-n',str(original)]),('elf-sections',['readelf','-S',str(original)]),('symbol-table',['nm','-anC',str(original)])]:
 result=subprocess.run(args,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=True)
 (job/'logs'/(label+'.log')).write_bytes(result.stdout);(job/'logs'/(label+'.stderr')).write_bytes(result.stderr)
print('release profile oracle gate and packed DWP original/copied provenance passed')
