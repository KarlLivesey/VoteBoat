"""Actual existing native-example fresh build/symbol proof; no tests or timing."""
import hashlib,json,shutil,subprocess
from pathlib import Path
j=Path(__file__).resolve().parent;h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
subprocess.run(['/usr/bin/python3',str(j/'verify-source.py')],check=True)
s=json.loads((j/'SOURCE.json').read_text());assert s['commit']==(j/'REQUIRED_COMMIT').read_text().strip()
assert (j/'logs/native-release.exit').read_text().strip()=='0'
events=[json.loads(line)for line in(j/'logs/native-release.jsonl').read_text().splitlines()if line.startswith('{')]
rows=[]
for name in ['voteboat','native_benchmark']:
 records=[e for e in events if e.get('reason')=='compiler-artifact'and e.get('target',{}).get('name')==name];assert len(records)==1,(name,len(records));r=records[0];profile=r['profile']
 assert r['fresh'] is False and profile['opt_level']=='3'and profile['debuginfo']==0 and profile['debug_assertions']and profile['overflow_checks']and not profile['test'],r
 assert set(r['features'])=={'default','native','quic','tls'},r['features']
 for field in ['src_path']:
  path=Path(r['target'][field]);rel=path.resolve().relative_to((j/'source').resolve()).as_posix();assert h(path)==s['sha256'][rel]
 original=Path(r['executable'])if r.get('executable')else Path(next(f for f in r['filenames']if f.endswith('.rlib')))
 copy=j/'artifacts/native'/original.name;copy.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(original,copy);assert h(original)==h(copy)
 rows.append({'target':name,'original':str(original),'copy':str(copy),'sha256':h(copy),'compiler_record':r})
lines=[line for line in(j/'logs/native-release.stderr').read_text().splitlines()if any('--crate-name '+n+' 'in line for n in ['voteboat','native_benchmark'])];assert len(lines)==2,lines
for line in lines:
 assert all(x in line for x in ['-C opt-level=3','-C debug-assertions=on']),line
 assert '-C debuginfo=2' not in line and '-C split-debuginfo=packed' not in line,line
 assert '-C lto=' not in line and '-C codegen-units='not in line and '-C overflow-checks=off'not in line,line
(j/'ROOT_NATIVE_RUSTC.txt').write_text('\n'.join(lines)+'\n')
root=next(r['compiler_record']for r in rows if r['target']=='voteboat');rid=root['package_id']
scripts=[e for e in events if e.get('reason')=='compiler-artifact'and e.get('target',{}).get('name')=='build-script-build'and e['package_id']==rid];assert len(scripts)==1 and scripts[0]['fresh']is False
executed=[e for e in events if e.get('reason')=='build-script-executed'and e['package_id']==rid];assert len(executed)==1 and executed[0]['cfgs']==[]
assert '--cfg voteboat_checkpoint_debug_oracle'not in '\n'.join(lines)
exe=Path(next(r['original']for r in rows if r['target']=='native_benchmark'))
u=subprocess.check_output(['/usr/bin/dwarfdump','--uuid',str(exe)],text=True);ids=[line.split()[1]for line in u.splitlines()if line.startswith('UUID:')];assert len(ids)==1;(j/'UUID.txt').write_text(u)
b=Path('/tmp/voteboat-mac435-5c2390c-2oTYYa')
for name in ['rustc.txt','cargo.txt','tool-sha256.txt']:assert(j/'logs'/name).read_bytes()==(b/'logs'/name).read_bytes(),name
proof={'source_commit':s['commit'],'source_inputs':s['inputs'],'artifacts':rows,'symbols':'DEBUG0 acceptance; no debug symbols requested','UUID':ids[0],'root_build_script_fresh':scripts[0],'root_build_script_executed':executed[0],'compiler_lines':'ROOT_NATIVE_RUSTC.txt','profile':'native defaultLTO/codegen; opt3/debug0/assertions+overflow; no framework override','source_modes':'SOURCE.json physical modes; seven existing fixture keys0600','tool_identities_equal435':True,'scope':'fresh root/example native acceptance artifact; no timing yet'}
(j/'BUILD_PROOF.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps({'fresh_root_example':True,'source_inputs':s['inputs'],'UUID':ids[0],'exe_sha256':rows[1]['sha256']}))
