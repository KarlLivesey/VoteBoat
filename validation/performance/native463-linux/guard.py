import json,hashlib,shutil,sys
from pathlib import Path
job=Path(sys.argv[1]).resolve();source=(job/'source').resolve();manifest=json.loads((job/'SOURCE.json').read_text());expected=manifest['sha256'];h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
publication=json.loads((job/'PUBLICATION.json').read_text());assert publication['commit']==manifest['commit'] and publication['actual_push_exit']==0 and publication['qualified_main_exit']==0
actual={p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()}
assert actual==set(expected),(actual-set(expected),set(expected)-actual)
for rel,digest in expected.items():assert h(source/rel)==digest,rel
assert manifest['commit']=='85eaf3e41ffe9488f78b75b02521ea2bbbc80c3a'
for rel,mode in manifest['physical_modes'].items():assert oct((source/rel).stat().st_mode & 0o777)==mode,rel
if sys.argv[2]=='source':
 print(f'exact {len(expected)} committed source hashes and physical modes passed');sys.exit(0)
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
