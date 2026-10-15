import json,hashlib,shutil,subprocess,sys
from pathlib import Path
job=Path(__file__).resolve().parent;source=job/'source';manifest=json.loads((job/'SOURCE.json').read_text());expected=manifest['sha256'];h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
subprocess.run(['/usr/bin/python3',str(job/'verify-source.py')],check=True)
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
script_lines=[line for line in (job/'logs/native-release.stderr').read_text().splitlines() if 'CARGO_MANIFEST_DIR='+str(source)+' ' in line and 'PROFILE=release' in line and '/build-script-build' in line]
assert len(script_lines)==1,len(script_lines)
(job/'logs/native-buildscript-invocation.txt').write_text(script_lines[0]+'\n')
(job/'ARTIFACTS.json').write_text(json.dumps(records,indent=2)+'\n')
print('fresh source/compiler/native profile and original/copied artifact guards passed')

import subprocess
exe=next(r for r in records if r['target']=='native_benchmark');binary=Path(exe['copy'])
for name,args in [('macho-uuid',['/usr/bin/dwarfdump','--uuid',str(binary)]),('macho-sections',['/usr/bin/otool','-l',str(binary)]),('symbol-table',['/usr/bin/nm','-n',str(binary)])]:
 result=subprocess.run(args,capture_output=True);(job/'logs'/(name+'.log')).write_bytes(result.stdout);(job/'logs'/(name+'.stderr')).write_bytes(result.stderr);(job/'logs'/(name+'.exit')).write_text(str(result.returncode)+'\n');assert result.returncode==0
original_uuid=subprocess.check_output(['/usr/bin/dwarfdump','--uuid',exe['original']],text=True)
copy_uuid=(job/'logs/macho-uuid.log').read_text();assert original_uuid.split()[1]==copy_uuid.split()[1]
assert '__debug_info' not in (job/'logs/macho-sections.log').read_text(),'native debug0 must not carry embedded DWARF'
root_scripts=[r for r in events if r.get('reason')=='build-script-executed' and r.get('package_id','').endswith('#voteboat@0.1.0')];assert len(root_scripts)==1 and 'voteboat_checkpoint_debug_oracle' not in root_scripts[0]['cfgs']
script_artifacts=[r for r in events if r.get('reason')=='compiler-artifact' and r.get('target',{}).get('name')=='build-script-build' and r.get('package_id','').endswith('#voteboat@0.1.0')];assert len(script_artifacts)==1 and script_artifacts[0]['fresh'] is False
(job/'SYMBOLS.json').write_text(json.dumps({'executable_sha256':exe['sha256'],'uuid':copy_uuid.split()[1],'symbol_form':'NativeMacMachO debug0 acceptance/symbol table; no forced dSYM, native defaultLTO/codegen preserved.','uuid_output_sha256':h(job/'logs/macho-uuid.log'),'macho_sections_sha256':h(job/'logs/macho-sections.log'),'symbol_table_sha256':h(job/'logs/symbol-table.log'),'reference_oracle_cfg_absent_PROFILErelease':True,'root_buildscript_record':root_scripts[0],'root_buildscript_fresh_record':script_artifacts[0]},indent=2)+'\n')
print('native debug0 MachO/UUID/release-profile guards passed')
