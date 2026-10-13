import hashlib,json,shutil
from pathlib import Path
root=Path('/tmp/voteboat-mac417-baseline-355cfa6');h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text());assert source['inputs']==1378
for name,digest in source['sha256'].items():assert h(root/'source'/name)==digest,name
records=[json.loads(s) for s in (root/'logs/build.jsonl').read_text().splitlines() if s.startswith('{')];artifacts=[]
for name in ['voteboat','voteboat-matched-framework']:
 chosen=[x for x in records if x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')==name];assert len(chosen)==1
 r=chosen[0];p=r['profile'];assert r['fresh'] is False and p['opt_level']=='3' and p['debug_assertions'] and p['overflow_checks']
 compiled=Path(r['target']['src_path']);relative=str(compiled.resolve().relative_to((root/'source').resolve()));assert h(compiled)==source['sha256'][relative]
 original=Path(next(x for x in r['filenames'] if x.endswith('.rlib'))) if name=='voteboat' else Path(r['executable'])
 copy=root/'artifacts'/original.name;copy.parent.mkdir(exist_ok=True);shutil.copyfile(original,copy);digest=h(original);assert h(copy)==digest
 if name!='voteboat':copy.chmod(0o555)
 artifacts.append({'target':name,'original':str(original),'copy':str(copy),'sha256':digest,'compiler_record':r})
lines=[s for s in (root/'logs/build.stderr').read_text().splitlines() if '--crate-name voteboat ' in s or '--crate-name voteboat_matched_framework ' in s];assert len(lines)==2 and all('-C codegen-units=1' in s for s in lines) and any('-C lto=thin' in s for s in lines)
(root/'logs/root-rustc-lines.txt').write_text('\n'.join(lines)+'\n')
(root/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
(root/'RESULTS.json').write_text(json.dumps({'scope':'Mac matched-only1378-input355 baseline; no native/tests/timed run','source_unchanged':True,'compiler_source_and_falsefresh_profile_guards':True,'commit':source['commit'],'artifacts':[{k:v for k,v in a.items() if k!='compiler_record'} for a in artifacts]},indent=2)+'\n')
print('fresh matched baseline library/executable and1378-input guards pass')
