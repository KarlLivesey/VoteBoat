import json,hashlib,shutil,sys
from pathlib import Path
job=Path(sys.argv[1]).resolve();source=(job/'source').resolve();manifest=json.loads((job/'SOURCE.json').read_text());expected=manifest['sha256'];h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
actual={p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()}
assert actual==set(expected),(actual-set(expected),set(expected)-actual)
for rel,digest in expected.items():assert h(source/rel)==digest,rel
ready={name:digest for digest,name in (line.split(maxsplit=1) for line in (job/'ready-source.sha256').read_text().splitlines())}
assert len(ready)==37
for rel,digest in ready.items():assert expected[rel]==digest and h(source/rel)==digest,rel
if sys.argv[2]=='source':
 print(f'exact {len(expected)} source and {len(ready)} ready hashes passed');sys.exit(0)
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
 assert original.resolve().is_relative_to((job/'target-native-release-qualified').resolve())
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
