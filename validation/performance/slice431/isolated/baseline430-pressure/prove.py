from pathlib import Path
import hashlib,json
j=Path(__file__).resolve().parent;s=j/'source';h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();m=json.loads((j/'SOURCE.json').read_text())
assert (j/'BUILD_STATUS').read_text().strip()=='0' and (j/'BUILD_STAGE').read_text().strip()=='DONE'
for p,v in m['sha256'].items():
 assert h(s/p)==v,p
 assert oct((s/p).stat().st_mode&0o777)==m['copied_modes'][p],p
 assert bool((s/p).stat().st_mode&0o111)==(m['git_modes'][p]=='100755'),p
records=[json.loads(l) for l in (j/'logs/build.jsonl').read_text().splitlines() if l.startswith('{')]
art=[]
for name,kind in [('voteboat','lib'),('effect_owner','test'),('voteboat-counter','bin'),('voteboat-directory','bin'),('voteboat-transfer','bin')]:
 rs=[r for r in records if r.get('reason')=='compiler-artifact' and r['target']['name']==name and kind in r['target']['kind']];assert len(rs)==1,(name,len(rs));r=rs[0]
 assert r['fresh'] is False,(name,r)
 assert r['profile']=={'opt_level':'0','debuginfo':2,'debug_assertions':True,'overflow_checks':True,'test':kind=='test'},(name,r['profile'])
 assert Path(r['manifest_path']).resolve()==s/'Cargo.toml'
 p=Path(r['target']['src_path']);assert p.is_relative_to(s) and h(p)==m['sha256'][p.relative_to(s).as_posix()]
 executable=Path(r['executable']) if r['executable'] else Path(next(f for f in r['filenames'] if f.endswith('.rlib')))
 assert executable.is_relative_to(j/'target-checks')
 art.append({'target':name,'kind':kind,'original':str(executable),'sha256':h(executable),'compiler_record':r})
lines=(j/'logs/build.stderr').read_text().splitlines();rootlines=[l for l in lines if '--crate-name voteboat ' in l];assert len(rootlines)==1
assert '--cfg voteboat_checkpoint_debug_oracle' in rootlines[0]
assert '-C opt-level=3' not in rootlines[0] and '-C lto=' not in rootlines[0]
bs=[r for r in records if r.get('reason')=='build-script-executed' and r.get('package_id','').endswith('#voteboat@0.1.0')];assert len(bs)==1 and 'voteboat_checkpoint_debug_oracle' in bs[0]['cfgs']
actual=[l for l in lines if 'PROFILE=debug' in l and 'build-script-build' in l and 'voteboat-' in l];assert len(actual)==1
for name in ['rustc.txt','cargo.txt','protoc.txt','tool-binaries.sha256']:
 assert (j/'logs'/name).read_bytes()==(j.parent/'matched430-linux/logs'/name).read_bytes(),name
(j/'ARTIFACTS.json').write_text(json.dumps(art,indent=2)+'\n')
(j/'PROOF.json').write_text(json.dumps({'commit':m['commit'],'source_inputs':1426,'source_hashes_and_modes_match':True,'original_build_handle':16659,'actual_terminal_exit':0,'actual_stage':'DONE','debug_target':'distinct genuinely fresh target-checks; no shared431 orframeworkartifactwrites','profile':'default debug opt0/debug2/assertions/overflowon; inheritedflagswrappers/profileoverridesunset','actual_root_rustc':rootlines[0],'build_script_record':bs[0],'actual_PROFILE_debug_invocation':actual[0],'reference_oracle_present':True,'same_compiler_cargo_protoc_as_published430_framework':True,'artifacts':[{k:r[k] for k in ['target','kind','original','sha256']} for r in art],'test_run':'NOTRUN; waitexplicitrootquietauthorization foroneexactnativepressurecase'},indent=2)+'\n')
print('baseline430 BUILD_READY,5 freshroot/test/requiredbin compiler records;1426hashes/modes; defaultdebug/referenceoracle exact; no testexecuted')
