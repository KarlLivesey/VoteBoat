import json,hashlib,shutil,sys,subprocess
from pathlib import Path
job=Path(sys.argv[1]).resolve();source=(job/'source').resolve();m=json.loads((job/'SOURCE.json').read_text());expected=m['sha256'];h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
actual={p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()};assert actual==set(expected),(actual-set(expected),set(expected)-actual)
for rel,digest in expected.items():assert h(source/rel)==digest,rel
for rel,mode in m['physical_modes'].items():assert oct((source/rel).stat().st_mode & 0o777)==mode,rel
required_commit='6c7f09b2c7d14606bc1d4308b83b5a5974f3ab19';assert required_commit!='UNPUBLISHED_CANDIDATE_NOT_AUTHORIZED_FOR_TIMING', '446 candidate not published; build/qualification refused';assert m['commit']==required_commit, 'wrong446 published source'
ready={name:digest for digest,name in (line.split(maxsplit=1) for line in (job/'slice446-source.sha256').read_text().splitlines())};assert set(ready)==set(m['changed_inputs']) and ready, 'exact declared446 changed-input scope differs'
for rel,digest in ready.items():assert expected[rel]==digest and h(source/rel)==digest,rel
reuse=json.loads((job/'FUNCTIONAL_REUSE.json').read_text());assert reuse['source_commit']==m['commit'] and reuse['final_source_sha256']==expected and reuse['exact_final_source_modes']==m['git_modes'];assert reuse['matched_functional_reuse']['fmt_strict_sourceguards_all_exit0'] and reuse['root_functional_reuse']['all_hashes_and_git_modes_equal'];assert all(h(Path(p['path']))==p['sha256'] for p in reuse['proofs']), 'changed functional/lint reuse receipt'
if sys.argv[2]=='source':print(f'exact{len(expected)} source ({m["compiled_inputs"]} compiled) and{len(ready)} slice446 hashes passed');sys.exit(0)
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
