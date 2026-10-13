from pathlib import Path
import hashlib,json,re
j=Path(__file__).resolve().parent
source=json.loads((j/'SOURCE.json').read_text())
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
required=('focused-owned','focused-core-full','drain-focus','remaining-all','affected-core','actual-services','fmt-final','clippy-all','clippy-default','clippy-core','clippy-native','matched-tests','matched-focused-actor','matched-final-fmt','matched-final-clippy','source-after')
scopes={}
for name in required:
 status=int((j/('DRAIN_STATUS' if name=='drain-focus' else name+'.status')).read_text())
 assert status==0,(name,status)
 log=j/(name+'.log')
 scopes[name]={'status':status,'command':(j/(name+'.command')).read_text().strip(),'log':str(log.relative_to(j)),'log_sha256':sha(log),'test_results':re.findall(r'test result: (.+)',log.read_text())}
assert int((j/'FINAL_MATCHED_STATUS').read_text())==0
assert (j/'CHECK_STAGE').read_text().strip()=='DONE'
for p,h in source['sha256'].items():assert sha(j/'source'/p)==h,p
reused={}
for a,allowed in (('attempt2',{'src/raft/checkpoint_tests.rs','src/log/owned/tests/checkpoint.rs','benchmarks/matched/src/voteboat_engine/actor/tests.rs'}),('attempt3',{'benchmarks/matched/src/voteboat_engine/actor/tests.rs'}),('attempt4',{'benchmarks/matched/src/voteboat_engine/actor/tests.rs'})):
 old=json.loads((j/a/'SOURCE.json').read_text())
 delta={p for p,h in source['sha256'].items() if old['sha256'].get(p)!=h}
 assert delta==allowed,(a,delta)
 assert old['sha256'].keys()==source['sha256'].keys()
 reused[a]={'source_manifest_sha256':sha(j/a/'SOURCE.json'),'only_changed_paths':sorted(delta)}
lines=(j/'affected-all.log').read_text().splitlines()
script=[];invocations=[]
for line in lines:
 try:v=json.loads(line)
 except ValueError:
  if 'rustc --crate-name voteboat ' in line and '--cfg voteboat_checkpoint_debug_oracle' in line:invocations.append(line)
  continue
 if v.get('reason')=='build-script-executed' and v.get('package_id','').endswith('#voteboat@0.1.0'):script.append(v)
assert len(script)==1 and script[0]['cfgs']==['voteboat_checkpoint_debug_oracle']
assert len(invocations)==2
(j/'DEV_ORACLE.json').write_text(json.dumps({'build_script_executed':script[0],'actual_root_rustc_invocations':invocations,'build_rs_sha256':source['sha256']['build.rs'],'source_log':'affected-all.log','source_log_sha256':sha(j/'affected-all.log'),'release_profile_proof':'pending; no artifact build or timing'},indent=2)+'\n')
packet={'isolated_only':True,'final_scopes_clean':True,'full_repeated_suite_claim':False,'base_commit':source['base_commit'],'source_manifest_sha256':sha(j/'SOURCE.json'),'source_files':source['source_files'],'changed_files':source['changed_files'],'new_files':source['new_files'],'patch_sha256':sha(j/'PATCH.diff'),'final_original_handle':36555,'final_original_exit':0,'scopes':scopes,'exact_source_reuse':reused,'coverage':{'all_feature_library':{'total':386,'reuse_passed':384,'corrected_fixture_controls':'focused-owned + focused-core-full (4 passes, includes the two earlier failures)'},'all_feature_effect_owner':{'total':203,'reuse_passed':202,'focused_remaining':1,'original_timeout_preserved':True},'all_feature_log_store':19,'all_feature_provider':32,'all_feature_raft':79,'all_feature_snapshot':27,'core_library':306,'core_effect_owner':195,'core_log_store':0,'core_provider':17,'core_raft':29,'core_snapshot':12,'actual_tcp_quic_services':2,'matched':35,'matched_edited_fixture_rerun':1},'history':[{'original_handle':14940,'exit':101,'reason':'Native private module import; focused pubcrate reexport import fix before tests','archive':'attempt1'},{'original_handle':95111,'exit':101,'reason':'384 lib passed, two new fixture assumptions failed; corrected actual preserved campaign lineage and atomic one-Full higher-term replacement; production unchanged','archive':'attempt2'},{'original_handle':47105,'exit':101,'reason':'202 effect_owner passed, existing hundred-group transport control exceeded initial 10s drive deadline; preserved raw failure','archive':'attempt3'},{'original_handle':85819,'exit':0,'reason':'exact same control quiet rerun passed 37.11s; no source/limit changes'},{'original_handle':67028,'exit':101,'reason':'standalone new fixture cognitive complexity26/25; extraction of unchanged report assertions, no weakened lint','archive':'attempt4'},{'original_handle':36555,'exit':0,'reason':'affected matched fixture, standalone fmt/strict Clippy, frozen inventory pass'}],'historical_check_status_kept':(j/'CHECK_STATUS').read_text().strip(),'dev_oracle_proof':'DEV_ORACLE.json','release_profile_proof':'pending','production_main_changed':False,'performance_claim':'none; no428 measurements'}
(j/'QUALIFICATION.json').write_text(json.dumps(packet,indent=2)+'\n')
print('QUALIFICATION: all scoped checks clean, with exact-source reuse and historical failures retained')
print('DEV_ORACLE: Cargo emitted cfg and both actual root rustc invocations contain it')
