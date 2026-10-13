"""Prepare guarded immutable variants only after frozen qualification succeeds."""
import hashlib,json,shutil
from pathlib import Path
root=Path('/tmp/voteboat-mac415416-8a5840d');old=Path('/tmp/voteboat-matched414-mac');upstream=Path('/tmp/voteboat-matched230-mac')
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert (root/'STATUS').read_text().strip()=='0'
qualification=json.loads((root/'QUALIFICATION.json').read_text());assert qualification['source_unchanged']
new_source=json.loads((root/'SOURCE.json').read_text())
artifacts=json.loads((root/'ARTIFACTS.json').read_text())
def parsed(p):
 return {s.split('  ',1)[1]:s.split('  ',1)[0] for s in p.read_text().splitlines()}
def copied(name,original,digest):
 assert h(original)==digest,(name,original)
 q=root/'measurement-artifacts'/name/original.name;q.parent.mkdir(parents=True,exist_ok=True)
 shutil.copyfile(original,q);assert h(q)==digest;q.chmod(0o555);return str(q)
# Matching-profile414 native variant is the previous campaign's candidate.
native_ref=json.loads(Path('/tmp/voteboat-native414-mac/candidate/ARTIFACTS.json').read_text())
ref_source=json.loads(Path('/tmp/voteboat-native414-mac/candidate/SOURCE.json').read_text())
for item in native_ref:item['copy']=copied('native-reference',Path(item['copy']),item['sha256'])
native_new=[]
for item in artifacts:
 if item['kind']=='native':
  c=item['compiler_record'];native_new.append({'target':item['target'],'compiler_source':c['target']['src_path'],'compiler_fresh':c['fresh'],'profile':c['profile'],'features':c['features'],'original':item['original'],'copy':copied('native-candidate',Path(item['copy']),item['sha256']),'sha256':item['sha256']})
for variant,source,records in [('reference',ref_source,native_ref),('candidate',{**new_source,'sourceRoot':str(root/'source')},native_new)]:
 q=root/f'native-{variant}';q.mkdir();(q/'SOURCE.json').write_text(json.dumps(source,indent=2)+'\n');(q/'ARTIFACTS.json').write_text(json.dumps(records,indent=2)+'\n')
for k,v in ref_source['sha256'].items():
 if k=='examples/native_benchmark.rs' or k.startswith('examples/benchmark/'):assert new_source['sha256'][k]==v,k
# Prior matched414 and untouched pinned upstream remain exact recorded controls.
base_exe=old/'target-harness/release/voteboat-matched-framework';base_sha='df6f1fc5c122a30034ec5d6ba0fd42ca1099121f1558392700698072ba9ee9fd'
base_lib=old/'target-harness/release/deps/libvoteboat-6ada661ba50f865f.rlib'
new_records=[x for x in artifacts if x['kind']=='matched'];new_exe=next(x for x in new_records if x['target']=='voteboat-matched-framework')
assert new_exe['sha256']!=base_sha
assert next(x['sha256'] for x in native_new if x['target']=='native_benchmark')!=next(x['sha256'] for x in native_ref if x['target']=='native_benchmark')
upstream_exe=upstream/'target-upstream/release/bench';upstream_sha='b3f2878f4146b4cf03174617a82c979bb9aec8558f3ab8fb2e45918691be8a6f'
identity={'scope':'Mac incremental414 to frozen8a5840d/Core415 plus pinned upstream; no wrapper416 benchmark or publishedfloor claim','variants':{
 'baseline':{'executable':copied('matched-reference',base_exe,base_sha),'source_root':str(old/'source-final'),'source_sha256':parsed(old/'source-final.sha256'),'artifact_sha256':{str(base_lib):h(base_lib)}},
 'candidate':{'executable':copied('matched-candidate',Path(new_exe['copy']),new_exe['sha256']),'source_root':str(root/'source'),'source_sha256':new_source['sha256'],'artifact_sha256':{str(Path(x['copy'])):x['sha256'] for x in new_records}},
 'untouched':{'executable':copied('upstream',upstream_exe,upstream_sha),'source_root':'/tmp/voteboat-openraft230-mac','source_sha256':parsed(upstream/'upstream-source.sha256'),'artifact_sha256':{}},
}}
for name,item in identity['variants'].items():
 item['artifact_sha256'][item['executable']]=h(Path(item['executable']))
 for p,d in item['source_sha256'].items():assert h(Path(item['source_root'])/p)==d,(name,p)
 for p,d in item['artifact_sha256'].items():assert h(Path(p))==d
(root/'FRAMEWORK_IDENTITY.json').write_text(json.dumps(identity,indent=2)+'\n')
print('immutable paired variants ready; no timings')
