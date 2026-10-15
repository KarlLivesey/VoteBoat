"""Independent collected Mac487 actual config review; no SSH or workload."""
from pathlib import Path
import hashlib,json
b=Path('target/p9/native487-capacity-bound');m=b/'comparison/mac';p=m/'prepared/campaign';l=b/'comparison/linux/campaign';raw=Path('target/p9/native486-capacity-bound/mac/collection/raw');qual=raw.parent.parent
load=lambda p:json.loads(Path(p).read_text());sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
c=load(p/'CAMPAIGN_CONFIG.json');linux=load(l/'CAMPAIGN_CONFIG.json');old=load(Path('target/p9/native484-ingress/comparison/mac/prepared/campaign/CAMPAIGN_CONFIG.json'));ready=load(p/'READY.json');expected={'baseline':'f7b0ef12709bb7e7aad4f0279851aa035b508e9e','candidate':'dcdcafe3f0c0a56d01e3c3095d3c276113cd7d69'}
assert c['source_commits']==expected and c['source_commit']==expected['candidate']
assert sha(p/'CAMPAIGN_CONFIG.json')==ready['campaign_config_sha256']=='d62834423aad8430f997f1566b315b9c10a6d62420e7b2122c1d3723260a5a61'
assert c['scripts']==linux['scripts']and c['validators']==linux['validators']==old['validators']and c['schema_sha256']==linux['schema_sha256']
assert c['runtime_tools']==old['runtime_tools']and c['python']==old['python']and c['node']==old['node']
assert ready['cases']==48 and ready['pairs']==24 and len(c['cases'])==48
assert c['existing_native_drain_guard_seconds']==old['existing_native_drain_guard_seconds']==120
assert c['all_workloads_end_before_validation']is True and c['failure_policy']==old['failure_policy']
assert sum(x['offers']for x in c['cases'])==391680 and sum(x['horizon_seconds']for x in c['cases'])==1344
for a,z,linux_case in zip(c['cases'],old['cases'],linux['cases']):
 fields=lambda x:{k:v for k,v in x.items()if k not in ['command','resource_command','metadata','wal_root']}
 assert fields(a)==fields(z)==fields(linux_case)and a['command'][2:]==z['command'][2:]==linux_case['command'][2:]
 binary=next(x['copy']for x in c['candidates'][a['arm']]['artifacts']if x['target']=='native_benchmark')
 assert a['command'][:2]==[binary,a['wal_root']]and a['wal_root']==c['wal_parent']+'/'+a['name']
 assert a['metadata']==c['job']+'/campaign-metadata/'+a['name']
 assert a['resource_command']==[c['python'],c['job']+'/resources.py',a['metadata'],*a['command']]
for n in range(0,48,2):assert c['cases'][n]['pair']==c['cases'][n+1]['pair']and{c['cases'][n]['arm'],c['cases'][n+1]['arm']}=={'baseline','candidate'}
assert(p/'ORDER.txt').read_text().splitlines()==[x['name']for x in c['cases']]
for n,d in c['scripts'].items():assert sha(p/n)==d==sha(l/n)
for n,d in c['validators'].items():assert sha(p/'control-tools'/n)==d and all(r['source']['files']['validation/'+n]==d for r in c['candidates'].values())
assert sha(p/'SCHEMA.md')==c['schema_sha256']and not(p/'STARTED.json').exists()and not(p/'campaign').exists()
setup=load(p/'setup-host.json');assert setup['device_identity']==c['device_identity']and setup['cpu_policy']==c['cpu_policy']
remote=load(m/'prepared/PREPARED_FILES.json');collection=load(m/'PREPARED_COLLECTION.json');assert len(remote)==collection['files']==28 and collection['actual_exit']==0 and collection['exact_remote_local_hashes_bytes_modes']
assert sha(m/'prepared/PREPARED_FILES.json')==collection['remote_inventory_sha256']
for name,v in remote.items():
 q=m/'prepared'/name;assert sha(q)==v['sha256']and q.stat().st_size==v['bytes']and oct(q.stat().st_mode&0o777)==v['mode']
guard=load(m/'prepared/ACTUAL_MAC_GUARD.json');assert guard['exit']==0 and guard['source_commits']==expected
power=(m/'prepared/actual-power-source.txt').read_text();assert 'AC Power'in power
baseline,candidate=[c['candidates'][a]for a in ['baseline','candidate']]
assert baseline['source']==old['candidates']['baseline']['source']
assert sorted(x['sha256']for x in baseline['compiler_tools'])==sorted(x['sha256']for x in candidate['compiler_tools'])
for name in ['Cargo.toml','Cargo.lock','clippy.toml','build.rs','LICENSE','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock']:
 assert baseline['source']['files'][name]==candidate['source']['files'][name]
fixtures=[n for n in baseline['source']['files']if n.startswith('tests/fixtures/')];keys=[n for n in fixtures if n.endswith('key.der')];assert len(keys)==7
for name in fixtures:assert baseline['source']['files'][name]==candidate['source']['files'][name]and baseline['source']['modes'][name]==candidate['source']['modes'][name]
assert all(candidate['source']['modes'][n]=='0o600'for n in keys)
changed=[n for n,d in candidate['source']['files'].items()if n.startswith(('src/','tests/','examples/'))and baseline['source']['files'].get(n)!=d]
assert set(changed)=={'src/application.rs','tests/application.rs','tests/application/capacity_bound.rs'}
artifact_ids={}
for arm,r in c['candidates'].items():
 assert r['source']['commit']==expected[arm]
 artifact_ids[arm]={}
 for a in r['artifacts']:
  record=a['compiler_record'];assert record['fresh']is False and record['profile']=={'opt_level':'3','debuginfo':0,'debug_assertions':True,'overflow_checks':True,'test':False}
  assert record['features']==['default','native','quic','tls']and record['manifest_path']==r['source']['root']+'/Cargo.toml';artifact_ids[arm][a['target']]=a['sha256']
 assert ready['artifacts_sha256'][arm]==artifact_ids[arm]['native_benchmark']
assert guard['artifacts_sha256']==artifact_ids
readiness=load(raw/'READINESS.json');symbols=load(raw/'SYMBOLS.json');original=load(raw/'ORIGINAL_TERMINAL.json');source=load(raw/'SOURCE.json');canonical=load(raw/'ROOT_FINAL_INPUTS.json')
assert readiness['exit']==0 and readiness['source_exact']is True and readiness['source_commit']==expected['candidate']
assert readiness['source_inventory_sha256']==sha(raw/'SOURCE.json')==candidate['source']['sha256']
assert readiness['inputs']==len(candidate['source']['files'])==len(canonical['files'])==1496
assert canonical['published_commit']==expected['candidate']
for name,item in canonical['files'].items():assert candidate['source']['files'][name]==item['sha256']and candidate['source']['modes'][name]==item['mode']
assert readiness['artifact_sha256']==artifact_ids['candidate']and readiness['native_uuid']==symbols['uuid']and readiness['fresh_root_executable_buildscript']
assert symbols['root_buildscript_fresh_record']['fresh']is False and symbols['reference_oracle_cfg_absent_PROFILErelease']
assert original['actual_exit']==0 and original['original_handle']==60577 and readiness['original_terminal']==original
required=['application_all','application_core','client_all','client_core','unit_application_all','unit_application_core','provider_all','provider_core','counter_service','matched_tests','provider_metadata','provider_metadata_controls','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_fmt','matched_strict','native-release','artifact_guard','positive_controls','source_before','source_after','tool_rustc_identity','tool_cargo_identity','tool_hash_identity','tool_protoc_identity']
for n in required:assert readiness['checks'][n]['exit']==0 and(raw/'logs'/(n+'.exit')).read_text().strip()=='0'
for n,count in {'application_all':23,'application_core':23,'unit_application_all':39,'unit_application_core':39,'client_all':28,'client_core':28,'provider_all':42,'provider_core':27,'counter_service':2,'matched_tests':70}.items():assert readiness['checks'][n]['actual_rust_passed_counts']==[count]
controls=load(raw/'CONTROLS.json');assert controls['exit']==0 and controls['artifact_unchanged']and readiness['controls']==len(controls['cases'])==4
assert{(x['protocol'],x['selection'])for x in controls['cases']}=={(p,s)for p in ['tcp','quic']for s in ['default','witness']}
for x in controls['cases']:
 s=x['summary'];assert x['process']['exit']==x['validator']['exit']==0 and s['applied']=='512'and s['recovered_value']=='576'and s['retry_verified']==s['workers_joined']=='true'
 assert s['groups']=='2'and s['workload_group_weights']=='31,1'and s['ready_visit_items_effective']=='64'
 assert all(s[k]=='0'for k in ['window_refused','no_leader','admission_refused','not_proposed','unknown','pending'])and len(x['capacity_readbacks'])==6
 if x['selection']=='default':assert not any(k.startswith('publication_')for k in s)and not x['publication_readbacks']and'--publication'not in x['process']['command']
 else:assert s['publication_effective']=='witness'and s['publication_provider']=='WitnessLogIo'and len(x['publication_readbacks'])==6
collected=load(raw/'COLLECTED.json');local_proof=load(qual/'LOCAL_TRANSFER_PROOF.json');assert local_proof['exit']==0 and local_proof['all_sha256_lengths_modes_exact']
for name,item in collected['files'].items():
 q=raw/name;assert sha(q)==item['sha256']and q.stat().st_size==item['bytes']and oct(q.stat().st_mode&0o777)==item['mode']
rootreview=load(qual/'INDEPENDENT_ROOT_REVIEW.json');assert rootreview['exit']==0
linuxreview=load(b/'review/INDEPENDENT_LINUX_CONFIG_REVIEW.json');assert linuxreview['exit']==0 and linuxreview['config_sha256']==sha(l/'CAMPAIGN_CONFIG.json')
receipt={'exit':0,'hosts':{'linux':{'config_sha256':sha(l/'CAMPAIGN_CONFIG.json'),'ready_sha256':sha(l/'READY.json'),'prior_review_sha256':sha(b/'review/INDEPENDENT_LINUX_CONFIG_REVIEW.json')},'mac':{'config_sha256':sha(p/'CAMPAIGN_CONFIG.json'),'ready_sha256':sha(p/'READY.json'),'job':c['job'],'actual_remote_prepared_files':28,'source_inputs':1496,'artifact_sha256':artifact_ids,'qualification_original':original,'readiness_sha256':sha(raw/'READINESS.json'),'native_uuid':symbols['uuid'],'prepared_collection_sha256':sha(m/'PREPARED_COLLECTION.json'),'prepared_inventory_sha256':sha(m/'prepared/PREPARED_FILES.json'),'AC_power_source_sha256':sha(m/'prepared/actual-power-source.txt')}},'source_commits':expected,'cases_per_host':48,'pairs_per_host':24,'offers_per_host':391680,'scheduled_horizon_seconds':1344,'same484_matrix_order_native_arguments_except_binary_and_roots':True,'same_linux_mac_frozen_scripts_and_validators':True,'scripts':c['scripts'],'validators':c['validators'],'withinhost_compiler_protoc_hashes_equal':True,'fixture_bytes_modes_equal_seven_private0600':True,'all_actual486_scope_counts_checked':True,'no_blockers':True,'review_script_sha256':sha(__file__),'limits':['Actual Mac binary/root/source remain remote; original remote guards passed before setup,28 prepared metadata copies and212 raw qualification originals bind their identities. No SSH/new remote checks or binary/source transfer.','Whole environment fingerprint is not claimed. One serial inherited controller environment, no per-arm overrides.','Read-only actual preparation review only. No benchmark/build/check suite; root quiet GO still required.']}
(b/'review/INDEPENDENT_BOTH_CONFIG_REVIEW.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps({'exit':0,'linux_config':receipt['hosts']['linux']['config_sha256'],'mac_config':receipt['hosts']['mac']['config_sha256'],'mac_ready':receipt['hosts']['mac']['ready_sha256'],'no_blockers':True},indent=2))
