import json,hashlib,shutil,sys,subprocess,re
from pathlib import Path
h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
ESSENTIALS={'Cargo.toml','Cargo.lock','build.rs','rust-toolchain.toml','src/log/command_bytes.rs','src/log/command_bytes/tests.rs','src/format_crc.rs','src/format_crc/tests.rs','examples/native_benchmark.rs','examples/benchmark/offered_capacity.rs','examples/benchmark/offered_capacity/tests.rs','validation/check-offered.mjs','validation/check-native-benchmark.mjs'}
def qualification(job):
 commit=(job/'REQUIRED_COMMIT.txt').read_text().strip()
 assert re.fullmatch('[0-9a-f]{40}',commit), '475 unpublished: source export/build/setup refused'
 publication=json.loads((job/'PUBLICATION.json').read_text());assert publication['commit']==commit and publication['exit']==0
 proof=publication['proof'];assert h(proof['path'])==proof['sha256']
 published=json.loads(Path(proof['path']).read_text())
 assert published['commit']==commit and published['exit']==0 and published['remote_exit']==0 and published['remote_main']==commit
 hook=publication['hook'];assert subprocess.check_output(['git','config','--local','core.hooksPath'],text=True).strip()=='.githooks';assert h(hook['path'])==hook['sha256']
 q=json.loads((job/'FUNCTIONAL_QUALIFICATION.json').read_text());assert q['source_commit']==commit
 assert q['reuse_base_commit']=='4f88a3f223b76936be63f73b43662e769ccecd15'
 assert not subprocess.check_output(['git','diff','--name-only',q['reuse_base_commit'],commit,'--','src','Cargo.toml','Cargo.lock','build.rs','clippy.toml','.cargo'],text=True).strip(), '475 production/build inputs changed beyond fixture scope'
 inv=q['source_inventory'];assert h(inv['path'])==inv['sha256'];normalized=json.loads(Path(inv['path']).read_text());qualified=normalized['files']
 assert (ESSENTIALS-{'rust-toolchain.toml'})<=set(qualified)
 evidence={}
 for name,p in q['proofs'].items():
  assert h(p['path'])==p['sha256'],name;evidence[name]=json.loads(Path(p['path']).read_text())
 for name in ['example','network','strict']:
  assert evidence[name]['exit']==0 and evidence[name]['source_exact'] is True,name
 assert evidence['network']['cases']==2 and evidence['network']['negative']==11
 for name in ['example_all','example_build']:assert evidence[name]['exit']==0,name
 assert q['scope']=='475 fixture-only capacity profile; unchanged production reuse explicitly scoped'
 assert len(q['final_source_guards'])>=1
 for pair in q['final_source_guards']:
  for p in pair.values():assert h(p['path'])==p['sha256']
  before=json.loads(Path(pair['before']['path']).read_text())['files']
  after=json.loads(Path(pair['after']['path']).read_text())['files']
  assert before==after==qualified
 return commit,qualified
job=Path(sys.argv[1]).resolve();required_commit,qualified=qualification(job)
if sys.argv[2]=='qualification':
 print(json.dumps({'commit':required_commit,'qualified_inputs':len(qualified),'scope':'475 fixture-only profile: actual final example/network/strict receipts; no native or framework performance claim.'}));sys.exit(0)
source=(job/'source').resolve();m=json.loads((job/'SOURCE.json').read_text());expected=m['sha256'];assert m['commit']==required_commit
actual={p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()};assert actual==set(expected),(actual-set(expected),set(expected)-actual)
assert ESSENTIALS<=set(expected)
for rel,digest in expected.items():assert h(source/rel)==digest,rel
for rel,mode in m['physical_modes'].items():assert oct((source/rel).stat().st_mode & 0o777)==mode,rel
for rel,item in qualified.items():assert expected[rel]==item['sha256'] and m['physical_modes'][rel]==item['mode'],rel
if sys.argv[2]=='source':
 print(f'exact{len(expected)} committed inputs +{len(qualified)} qualified input projection hashes/physical modes passed');sys.exit(0)
events=[json.loads(line) for line in (job/'logs/native-release.jsonl').read_text().splitlines() if line.startswith('{')]
records=[]
for name,kind in [('voteboat','lib'),('native_benchmark','example')]:
 selected=[x for x in events if x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')==name and kind in x['target']['kind']]
 assert len(selected)==1,(name,len(selected));record=selected[0]
 profile=record['profile'];assert record['fresh'] is False and profile['opt_level']=='3' and profile['debug_assertions'] and profile['overflow_checks'] and not profile['test'],record
 assert profile['debuginfo']==0,profile
 assert record['features']==['default','native','quic','tls'],record
 assert Path(record['manifest_path']).resolve()==source/'Cargo.toml',record
 rel=Path(record['target']['src_path']).resolve().relative_to(source).as_posix();assert rel in expected and h(source/rel)==expected[rel]
 original=Path(next(path for path in record['filenames'] if path.endswith('.rlib'))) if kind=='lib' else Path(record['executable'])
 assert original.resolve().is_relative_to((job/'target-native-release').resolve())
 digest=h(original);copy=job/'artifacts/native'/original.name;copy.parent.mkdir(parents=True,exist_ok=True);assert not copy.exists();shutil.copyfile(original,copy);assert h(copy)==digest
 copy.chmod(0o555 if kind=='example' else 0o444)
 records.append({'kind':'native','target':name,'original':str(original),'copy':str(copy),'sha256':digest,'compiler_record':record})
lines=[line for line in (job/'logs/native-release.stderr').read_text().splitlines() if '--crate-name voteboat ' in line or '--crate-name native_benchmark ' in line]
assert len(lines)==2,len(lines)
for line in lines:
 assert '-C lto=' not in line and '-C codegen-units=' not in line and '-C linker-plugin-lto' not in line,line
 for flag in ['-C opt-level=3','-C debug-assertions=on']:assert flag in line,(flag,line)
 assert '-C overflow-checks=off' not in line,line
 # Cargo can omit an explicit overflow switch when it agrees with enabled
 # debug assertions; the actual compiler-artifact profile must still be true.
(job/'logs/native-root-rustc-lines.txt').write_text('\n'.join(lines)+'\n')
(job/'ARTIFACTS.json').write_text(json.dumps(records,indent=2)+'\n')
print('fresh source/compiler/native profile and original/copied artifact guards passed')

import subprocess
exe=next(r for r in records if r['target']=='native_benchmark');binary=Path(exe['copy'])
for name,args in [('elf-notes',['readelf','-n',str(binary)]),('elf-sections',['readelf','-S',str(binary)]),('symbol-table',['nm','-anC',str(binary)])]:
 result=subprocess.run(args,capture_output=True);(job/'logs'/(name+'.log')).write_bytes(result.stdout);(job/'logs'/(name+'.stderr')).write_bytes(result.stderr);(job/'logs'/(name+'.exit')).write_text(str(result.returncode)+'\n');assert result.returncode==0
sections=(job/'logs/elf-sections.log').read_text();assert '.debug_info' not in sections, 'debug0 acceptance executable must not carry embedded DWARF'
root_scripts=[r for r in events if r.get('reason')=='build-script-executed' and r.get('package_id','').endswith('#voteboat@0.1.0')];assert len(root_scripts)==1 and 'voteboat_checkpoint_debug_oracle' not in root_scripts[0]['cfgs']
script_artifacts=[r for r in events if r.get('reason')=='compiler-artifact' and r.get('target',{}).get('name')=='build-script-build' and r.get('package_id','').endswith('#voteboat@0.1.0')];assert len(script_artifacts)==1 and script_artifacts[0]['fresh'] is False
(job/'SYMBOLS.json').write_text(json.dumps({'executable_sha256':exe['sha256'],'symbol_form':'NativeLinuxELF debug0 acceptance/symboltable; no DWARF/DWP requirement, native defaultLTO/codegen preserved.','elf_notes_sha256':h(job/'logs/elf-notes.log'),'elf_sections_sha256':h(job/'logs/elf-sections.log'),'symbol_table_sha256':h(job/'logs/symbol-table.log'),'reference_oracle_cfg_absent_PROFILErelease':True,'root_buildscript_record':root_scripts[0],'root_buildscript_fresh_record':script_artifacts[0]},indent=2)+'\n')
print('nativeacceptance debug0 ELF/release-profilecfg proven; CLI controls stillpending')

script_invocations=[line for line in (job/'logs/native-release.stderr').read_text().splitlines() if 'PROFILE=release' in line and 'build-script-build' in line and 'CARGO_PKG_NAME=voteboat ' in line and f'CARGO_MANIFEST_PATH={source}/Cargo.toml ' in line]
assert len(script_invocations)==1,script_invocations
root_line=next(line for line in lines if '--crate-name voteboat ' in line)
assert '--cfg voteboat_checkpoint_debug_oracle' not in root_line
assert "--check-cfg 'cfg(voteboat_checkpoint_debug_oracle)'" in root_line
(job/'PROFILE_PROOF.json').write_text(json.dumps({'source_commit':m['commit'],'profile':'opt3/debug0/assert+overflow/nativeDEFAULTLTO+cgu','actual_rustc_lines':lines,'root_buildscript_record':root_scripts[0],'root_buildscript_compilation':script_artifacts[0],'actual_PROFILE_release_invocation':script_invocations[0],'build_rs_sha256':expected['build.rs'],'compiler_tools_sha256_file':h(job/'logs/tool-binaries.sha256'),'native455_tool_preflight_sha256':h(job/'logs/baseline-tool-preflight.log'),'scope':'Actual CargoJSON/vv source/flags/artifact guards, not functional reruns or benchmark results.'},indent=2)+'\n')
