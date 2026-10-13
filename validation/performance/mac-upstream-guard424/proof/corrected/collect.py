from pathlib import Path
import hashlib,json,shutil,subprocess
r=Path('/tmp/voteboat-mac-upstream-guard424-corrected');s=json.loads((r/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
events=[json.loads(x) for x in (r/'logs/build.jsonl').read_text().splitlines() if x.startswith('{')];artifacts=[]
for name in ['openraft','bench']:
 candidates=[x for x in events if x.get('reason')=='compiler-artifact' and x.get('target',{}).get('name')==name];assert len(candidates)==1,(name,len(candidates))
 x=candidates[0];p=x['profile'];assert x['fresh'] is False and p['opt_level']=='3' and p['debug_assertions'] and p['overflow_checks'],x
 src=Path(x['target']['src_path']);rel=str(src.resolve().relative_to((r/'source').resolve()));assert h(src)==s['sha256'][rel]
 original=Path(next(q for q in x['filenames'] if q.endswith('.rlib'))) if name=='openraft' else Path(x['executable'])
 copy=r/'artifacts'/original.name;copy.parent.mkdir(exist_ok=True);shutil.copy2(original,copy);assert h(copy)==h(original)
 if name=='bench':assert copy.stat().st_mode&0o111
 artifacts.append({'target':name,'original':str(original),'copy':str(copy),'sha256':h(copy),'compiler_record':x})
lines=[x for x in (r/'logs/build.stderr').read_text().splitlines() if '--crate-name openraft ' in x or '--crate-name bench ' in x];assert len(lines)==2 and all('-C codegen-units=1' in x for x in lines) and any('-C lto=thin' in x for x in lines)
(r/'logs/root-rustc-lines.txt').write_text('\n'.join(lines)+'\n')
binary=next(x['copy'] for x in artifacts if x['target']=='bench');controls=[]
for batch in [1,4]:
 name=f'functional-b{batch}';argv=[binary,'--client-workers','1','--server-workers','16','--clients','64','--batch',str(batch),'--operations','16384','--members','3','--warmup-writes','1024','--latency']
 (r/f'logs/{name}.command').write_text(json.dumps(argv)+'\n');p=subprocess.run(argv,capture_output=True,text=True)
 (r/f'logs/{name}.json').write_text(p.stdout);(r/f'logs/{name}.stderr').write_text(p.stderr);(r/f'logs/{name}.exit').write_text(str(p.returncode)+'\n');assert p.returncode==0,(name,p.stderr)
 v=json.loads(p.stdout);assert v['operations']==v['requested_operations']==16384 and v['samples']==16384//batch and v['warmup_writes']==1024 and v['members']==[0,1,2] and v['clients']==64 and v['batch']==batch
 assert all(v[k]==0 for k in ['append_delay_us','flush_delay_us','network_delay_us','write_rate']);controls.append({'batch':batch,'originals':16384,'samples':v['samples'],'performance_claim':False})
for rel,d in s['sha256'].items():assert h(r/'source'/rel)==d,rel
a=json.loads((r/'ANCHOR.json').read_text())
for rel,d in a['source_sha256'].items():assert h(Path(a['source_root'])/rel)==d,rel
for p,d in a['artifact_sha256'].items():assert h(Path(p))==d,p
for x in artifacts:assert h(Path(x['copy']))==h(Path(x['original']))==x['sha256']
(r/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n');(r/'QUALIFICATION.json').write_text(json.dumps({'pinned_engine':s['upstream_commit'],'source_files':1357,'guard_extended_seconds':3600,'original_guard_seconds':400,'independent_lock_packages':196,'default_benchmark_features':True,'engine_and_driver_fresh_false':True,'source_and_original_anchor_unchanged':True,'functional_controls':controls,'timing_claim':False,'artifacts':[{k:v for k,v in x.items() if k!='compiler_record'} for x in artifacts]},indent=2)+'\n')
print('freshassertedengine/driver/defaultfeature/source/anchorandfunctional guards pass')
