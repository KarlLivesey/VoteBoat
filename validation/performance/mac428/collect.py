import hashlib,json,re,shutil,subprocess,stat
from pathlib import Path
root=Path('/tmp/voteboat-mac428-a46da2b-xGe40Y');h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text())
actual={p.relative_to(root/'source').as_posix() for p in (root/'source').rglob('*') if p.is_file()}
assert actual==set(source['sha256']),(actual-set(source['sha256']),set(source['sha256'])-actual)
assert source['commit']=='a46da2b0a4a13166974601ef0752341aecf9c1c6'
for rel,digest in source['sha256'].items():
 assert h(root/'source'/rel)==digest,rel
 assert oct(stat.S_IMODE((root/'source'/rel).stat().st_mode))==source['modes'][rel],rel
assert all(rel in source['sha256'] for rel in ['clippy.toml','rust-toolchain.toml','build.rs','src/log/comparison.rs','src/log/comparison/tests.rs','src/log/owned/tests/checkpoint.rs','src/native/log_store/accepted/tests/checkpoint.rs','src/raft/checkpoint_tests.rs','tests/log_store/matches.rs','tests/provider_conformance/log_store/default_errors.rs','tests/provider_conformance/log_store/matches.rs','tests/snapshot/log_match.rs','benchmarks/matched/src/store/tests.rs','docs/provider-conformance.json'])
expected={'affected_all':[386,19,32,79,27],'affected_core':[306,0,17,29,12],'services':[2],'matched_tests':[35]}
counts={}
for name,expected_counts in expected.items():
 values=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;',(root/f'logs/{name}.log').read_text())
 assert values==[(str(n),'0') for n in expected_counts],(name,values)
 counts[name]=expected_counts
steps=['affected_all','affected_core','services','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_fmt','matched_tests','matched_strict','matched_cli_build','cli-off','cli-on','cli-openraft','matched-release']
for name in steps:assert (root/f'logs/{name}.exit').read_text().strip()=='0',name
controls=[]
for mode in ['off','on','openraft']:
 d=json.loads((root/f'logs/cli-{mode}.log').read_text());on=mode=='on';engine='openraft' if mode=='openraft' else 'voteboat';s=d['settings']
 assert d['engine']==engine and s['attribution']==on and s['operations']==s['requested_operations']==16384 and s['warmup']==1024 and s['clients']==4096 and s['batch']==4
 assert d['completed_originals']==16384 and d['completed_requests']==4096 and d['applied_frontiers']==[17409]*3
 assert s['maintenance']=='default_retention' and s['envelopes']=='upstream_default' and s['campaign_timeout_secs']==3600
 assert d['durability']=='volatile_no_disk' and d['completion']=='every_original_locally_applied_after_quorum_commit'
 cap=4096 if engine=='openraft' else 16384
 assert [d[k] for k in ['api_entry_batch_capacity','append_entry_batch_capacity','replication_entry_capacity']]==[cap,cap,1024]
 assert d['capacity_contract']['api_merge_original_bound_for_fixed_request_batch']==16384
 if engine=='voteboat':
  assert all(x['checkpoint_count']==3 for x in d['diagnostics'])
  assert sum(x['proposed_originals'] for x in d['diagnostics'])==17408
  assert sum(sum(w['original_entries']*w['batches'] for w in x['proposal_widths']) for x in d['diagnostics'])==17408
 else:
  assert d['diagnostics'] is None and d['upstream_commit']=='e88bb40707f2e511d8ecc44d31edcb1d814a11a8'
 if not on:assert d.get('attribution') is None
 else:
  assert len(d['attribution'])==3
  for a in d['attribution']:
   assert len(a['phases'])==29 and len({p['phase'] for p in a['phases']})==29 and len(a['checkpoint_provider_scopes'])==4
   assert 'not_cpu_or_critical_path' in a['clock'] and 'reported_compact_gaps' in a['residual'] and 'state_to_pin_starts_after_protected_checkpoint_verification' in a['checkpoint_compact_gap_scope']
   assert 'false_or_error_zero_not_physical_traversal' in a['item_units']
   assert [g['gap'] for g in a['checkpoint_compact_gaps']]==['state_to_pin','pin_to_append','barrier_to_reconcile']
   for g in a['checkpoint_compact_gaps']:
    assert g['calls']==3 and g['failures']==0 and g['items']==0 and 0<=g['maximum_elapsed_ns']<=g['total_elapsed_ns']
   phases={p['phase']:p for p in a['phases']}
   assert 'store_matches_state_exact_nested' in phases and 'store_state_clone_nested' in phases
   for q in a['phases']:assert q['failures']==0 and 0<=q['maximum_elapsed_ns']<=q['total_elapsed_ns']
   for scope in a['checkpoint_provider_scopes']:
    assert len(scope['methods'])==15 and len({m['method'] for m in scope['methods']})==15
    for m in scope['methods']:
     assert m['failures']==0 and all(0<=m[k]<=phases[m['method']][k] for k in ['calls','total_elapsed_ns','items'])
   compact=next(scope for scope in a['checkpoint_provider_scopes'] if scope['scope']=='checkpoint_compact_inclusive')
   methods={m['method']:m for m in compact['methods']}
   assert methods['store_checkpoint_state_protected_inclusive_nested']['calls']==3
   assert methods['store_matches_state_exact_nested']['calls']==0 and methods['store_state_clone_nested']['calls']==0
   assert 'not_actual_compared_or_skipped_entries' in a['item_units']
   assert sum(g['total_elapsed_ns'] for g in a['checkpoint_compact_gaps'])<=phases['checkpoint_compact_inclusive']['total_elapsed_ns']
 controls.append({'name':mode,'engine':engine,'originals':16384,'requests':4096,'frontiers':[17409]*3,'checkpoint_counts':[3]*3 if engine=='voteboat' else None,'performance_claim':False})
events=[json.loads(s) for s in (root/'logs/matched-release.jsonl').read_text().splitlines() if s.startswith('{')]
artifacts=[]
for name in ['voteboat','voteboat-matched-framework','openraft','bench_minimal']:
 records=[e for e in events if e.get('reason')=='compiler-artifact' and e.get('target',{}).get('name')==name]
 assert len(records)==1,(name,len(records));r=records[0];p=r['profile']
 assert r['fresh'] is False and p['opt_level']=='3' and p['debug_assertions'] and p['overflow_checks'] and p['debuginfo']==2,r
 assert p['test'] is False
 src=Path(r['target']['src_path'])
 manifest=Path(r['manifest_path'])
 if name in ['voteboat','voteboat-matched-framework']:
  rel=str(src.resolve().relative_to((root/'source').resolve()));assert h(src)==source['sha256'][rel]
  assert r['features']==[]
 else:
  checkout=next(a for a in src.parents if (a/'.git').exists());assert subprocess.check_output(['/usr/bin/git','-C',str(checkout),'rev-parse','HEAD']).decode().strip()=='e88bb40707f2e511d8ecc44d31edcb1d814a11a8'
  assert 'e88bb40707f2e511d8ecc44d31edcb1d814a11a8' in r['package_id']
  status=subprocess.check_output(['/usr/bin/git','-C',str(checkout),'status','--porcelain'],text=True).splitlines()
  assert all(line=='?? .cargo-ok' for line in status),status
  for file in [src,manifest]:
   committed=subprocess.check_output(['/usr/bin/git','-C',str(checkout),'show','HEAD:'+file.relative_to(checkout).as_posix()])
   assert hashlib.sha256(committed).hexdigest()==h(file)
 original=Path(r['executable']) if r.get('executable') else Path(next(f for f in r['filenames'] if f.endswith('.rlib')))
 copy=root/'artifacts'/'matched'/original.name;copy.parent.mkdir(parents=True,exist_ok=True)
 if copy.exists():assert h(copy)==h(original)
 else:shutil.copyfile(original,copy)
 assert h(copy)==h(original)
 if r.get('executable'):copy.chmod(0o555)
 artifacts.append({'target':name,'original':str(original),'copy':str(copy),'sha256':h(copy),'compiler_record':r,'compiled_source_sha256':h(src),'manifest_sha256':h(manifest)})
lines=[s for s in (root/'logs/matched-release.stderr').read_text().splitlines() if any(f'--crate-name {n} ' in s for n in ['voteboat','voteboat_matched_framework','openraft','bench_minimal'])]
assert len(lines)==4 and all('-C codegen-units=1' in s for s in lines) and any('-C lto=thin' in s for s in lines),len(lines)
(root/'logs/matched-root-rustc-lines.txt').write_text('\n'.join(lines)+'\n')
(root/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
# The custom cfg follows the actual profile, not debug_assertions.
dev_events=[json.loads(line) for line in (root/'logs/affected_all.log').read_text().splitlines() if line.startswith('{')]
root_id=next(a['compiler_record']['package_id'] for a in artifacts if a['target']=='voteboat')
dev_roots=[e for e in dev_events if e.get('reason')=='compiler-artifact' and e.get('target',{}).get('name')=='voteboat' and Path(e['manifest_path']).resolve()==(root/'source/Cargo.toml').resolve()]
dev_root_ids={e['package_id'] for e in dev_roots}
assert len(dev_root_ids)==1 and len(dev_roots)==2,dev_roots
dev_script=[e for e in dev_events if e.get('reason')=='build-script-executed' and e.get('package_id') in dev_root_ids]
assert len(dev_script)==1 and dev_script[0]['cfgs']==['voteboat_checkpoint_debug_oracle'],dev_script
dev_lines=[line for line in (root/'logs/affected_all.log').read_text().splitlines() if '--crate-name voteboat ' in line and '--cfg voteboat_checkpoint_debug_oracle' in line]
assert len(dev_lines)==2,dev_lines
release_script=[e for e in events if e.get('reason')=='build-script-executed' and e.get('package_id')==root_id]
assert len(release_script)==1 and release_script[0]['cfgs']==[],release_script
root_lines=[line for line in lines if '--crate-name voteboat ' in line]
assert len(root_lines)==1 and '--cfg voteboat_checkpoint_debug_oracle' not in root_lines[0]
for line in lines:
 assert all(flag in line for flag in ['-C opt-level=3','-C debuginfo=2','-C debug-assertions=on','-C codegen-units=1']),line
 assert '-C overflow-checks=off' not in line
assert any('--crate-name voteboat_matched_framework ' in line and '-C lto=thin' in line for line in lines)
assert (root/'logs/rustc.txt').read_bytes()==Path('/tmp/voteboat-mac426-799f2cf-VRN5Xr/logs/rustc.txt').read_bytes()
(root/'DEV_ORACLE.json').write_text(json.dumps({'build_script_executed':dev_script[0],'build_rs_sha256':source['sha256']['build.rs'],'actual_verbose_source':'logs/affected_all.log','actual_root_rustc_lines':dev_lines,'canonical_dev_manifest':str((root/'source/Cargo.toml').resolve()),'scope':'actual Mac dev profile reference cfg; tests retain full Eq'},indent=2)+'\n')
(root/'RELEASE_ORACLE.json').write_text(json.dumps({'build_script_executed':release_script[0],'root_rustc_line':root_lines[0],'build_rs_sha256':source['sha256']['build.rs'],'debug_assertions':True,'overflow_checks':True,'oracle_cfg_absent':True,'scope':'asserted release exact protected algorithm; ordinary assertions retained'},indent=2)+'\n')
# Preserve every reported/generated external DWARF bundle/file; absence is explicit.
symbols=[]
executable=next(Path(a['original']) for a in artifacts if a['target']=='voteboat-matched-framework')
for symbol in [Path(str(executable)+'.dSYM'),Path(str(executable)+'.dwp')]:
 if symbol.exists():
  dest=root/'artifacts/symbols'/symbol.name;dest.parent.mkdir(parents=True,exist_ok=True)
  if symbol.is_dir():shutil.copytree(symbol,dest)
  else:shutil.copy2(symbol,dest)
  files=list(symbol.rglob('*')) if symbol.is_dir() else [symbol]
  for original in files:
   if original.is_file():
    copy=dest/original.relative_to(symbol) if symbol.is_dir() else dest
    assert h(original)==h(copy)
    symbols.append({'original':str(original),'copy':str(copy),'sha256':h(copy)})
(root/'SYMBOL_ARTIFACTS.json').write_text(json.dumps({'files':symbols,'external_dsym_dwp_present':bool(symbols),'scope':'all executable-associated dSYM/DWP files present; no invented external symbols when DWARF is embedded'},indent=2)+'\n')
(root/'QUALIFICATION.json').write_text(json.dumps({'commit':source['commit'],'history':{'original_remote_pid':96361,'original_terminal_status':1,'original_failure':'collector compared literal /private/tmp dev package identity against /tmp standalone release; source and compiled paths canonically identical','focused_fix':'canonical dev source manifest/package mapping; reuse identical existing copied artifacts','retry_scope':'collector guards only; no repeated tests/checks/builds'},'guarded_inputs':source['inputs'],'scope':'Full428 scoped library/cache/core/log/provider/raft/snapshot, real TCP/QUIC retry/replacement/native recovery, formatting four strict profiles, matched35 and functional off/on/OpenRaft maintenance histories; no timing or floor claim','controls':controls,'test_counts':counts,'four_artifacts_fresh_false':True,'profile':'opt3/debug2/assertions/overflow/thinLTO/codegen1','source_unchanged':True,'source_permissions_preserved':True,'dev_oracle_actual_cfg':True,'asserted_release_oracle_cfg_absent':True,'symbol_artifacts':'SYMBOL_ARTIFACTS.json','clippy_config_sha256':source['sha256']['clippy.toml']},indent=2)+'\n')
print('source/permissions, scoped tests, four strict profiles, matched35, CLI maintenance, observer29/4/15, and four fresh asserted artifact proofs pass')
