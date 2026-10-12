from pathlib import Path
import json,sys,hashlib
root=Path(sys.argv[1]);records=[]
for line in (root/'results/build.log').read_text().splitlines():
    try:r=json.loads(line)
    except json.JSONDecodeError:continue
    if r.get('reason')=='compiler-artifact':records.append(r)
selectors=[('runtime',True),('voteboat',True),('voteboat',False)];selected=[]
for name,is_test in selectors:
    found=[r for r in records if r['target']['name']==name and r['profile']['test']==is_test and (r['executable'] is not None if is_test else any(x.endswith('.rlib') for x in r['filenames']))]
    assert len(found)==1,(name,is_test,len(found));r=found[0];p=r['profile'];assert p['opt_level']=='0' and p['debug_assertions'] and p['overflow_checks'],p
    assert sorted(r['features'])==['default','native','quic','tls'],r['features'];selected.append(r)
files=[selected[0]['executable'],selected[1]['executable']]+[x for x in selected[2]['filenames'] if x.endswith('.rlib')];assert len(files)==3 and all(Path(x).is_file() for x in files)
(root/'results/artifact_paths.txt').write_text('\n'.join(sorted(files))+'\n')
(root/'results/compiler_artifacts.json').write_text(json.dumps({'compiler_records':selected,'launch_files':files,'before_sha256':{x:hashlib.sha256(Path(x).read_bytes()).hexdigest() for x in files}},indent=2)+'\n')
print('selected original runtime/lib-test executables + library; actual defaultdebug/assertions/overflow/allfeatures profile verified')
