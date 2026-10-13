import json,hashlib,shutil,sys,subprocess
from pathlib import Path
job=Path(sys.argv[1]).resolve();source=(job/'source').resolve();m=json.loads((job/'SOURCE.json').read_text());expected=m['sha256'];h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
actual={p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()};assert actual==set(expected),(actual-set(expected),set(expected)-actual)
for rel,digest in expected.items():assert h(source/rel)==digest,rel
ready={name:digest for digest,name in (line.split(maxsplit=1) for line in (job/'slice424-source.sha256').read_text().splitlines())};assert len(ready)==9
for rel,digest in ready.items():assert expected[rel]==digest and h(source/rel)==digest,rel
if sys.argv[2]=='source':print(f'exact{len(expected)} source ({m["compiled_inputs"]} compiled) and{len(ready)} slice424 hashes passed');sys.exit(0)
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
 digest=h(original);copy=job/'artifacts/matched'/original.name;copy.parent.mkdir(parents=True,exist_ok=True);assert not copy.exists();shutil.copyfile(original,copy);assert h(copy)==digest;copy.chmod(0o555 if kind=='bin' else 0o444)
 artifacts.append({'kind':'matched','target':name,'original':str(original),'copy':str(copy),'sha256':digest,'compiler_record':r,'compiled_source_sha256':h(src),'manifest_sha256':h(manifest)})
lines=[line for line in (job/'logs/matched-release.stderr').read_text().splitlines() if '--crate-name voteboat ' in line or '--crate-name voteboat_matched_framework ' in line];assert len(lines)==2
for line in lines:
 for flag in ['-C opt-level=3','-C debuginfo=2','-C debug-assertions=on','-C codegen-units=1']:assert flag in line,(flag,line)
 assert '-C overflow-checks=off' not in line
assert any('-C lto=thin' in line for line in lines)
(job/'logs/matched-root-rustc-lines.txt').write_text('\n'.join(lines)+'\n');(job/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
print('freshroot/upstream/compiler/source/assertedthinLTO profile and original/copied artifact guards passed')
