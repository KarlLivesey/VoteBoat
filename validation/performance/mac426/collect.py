import hashlib,json,re,shutil,subprocess,stat
from pathlib import Path
root=Path('/tmp/voteboat-mac426-799f2cf-VRN5Xr');h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text())
assert source['commit']=='799f2cf300a50e004e74d07b6a9237f59003892b'
for rel,digest in source['sha256'].items():
 assert h(root/'source'/rel)==digest,rel
 assert oct(stat.S_IMODE((root/'source'/rel).stat().st_mode))==source['modes'][rel],rel
assert all(rel in source['sha256'] for rel in ['clippy.toml','rust-toolchain.toml','tests/log_store/matches.rs','tests/provider_conformance/log_store/default_errors.rs','tests/provider_conformance/log_store/matches.rs','tests/snapshot/log_match.rs','benchmarks/matched/src/store/tests.rs','docs/provider-conformance.json'])
expected={'affected_all':[19,31,26],'affected_core':[16,11],'services':[2],'matched_tests':[34]}
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
   assert len(a['phases'])==28 and len({p['phase'] for p in a['phases']})==28 and len(a['checkpoint_provider_scopes'])==4
   assert 'not_cpu_or_critical_path' in a['clock'] and 'reported_compact_gaps' in a['residual'] and 'state_to_pin_starts_after_exact_comparison' in a['checkpoint_compact_gap_scope']
   assert 'false_or_error_zero_not_physical_traversal' in a['item_units']
   assert [g['gap'] for g in a['checkpoint_compact_gaps']]==['state_to_pin','pin_to_append','barrier_to_reconcile']
   for g in a['checkpoint_compact_gaps']:
    assert g['calls']==3 and g['failures']==0 and g['items']==0 and 0<=g['maximum_elapsed_ns']<=g['total_elapsed_ns']
   phases={p['phase']:p for p in a['phases']}
   assert 'store_matches_state_exact_nested' in phases and 'store_state_clone_nested' in phases
   for q in a['phases']:assert q['failures']==0 and 0<=q['maximum_elapsed_ns']<=q['total_elapsed_ns']
   for scope in a['checkpoint_provider_scopes']:
    assert len(scope['methods'])==14 and len({m['method'] for m in scope['methods']})==14
    for m in scope['methods']:
     assert m['failures']==0 and all(0<=m[k]<=phases[m['method']][k] for k in ['calls','total_elapsed_ns','items'])
   assert sum(g['total_elapsed_ns'] for g in a['checkpoint_compact_gaps'])<=phases['checkpoint_compact_inclusive']['total_elapsed_ns']
 controls.append({'name':mode,'engine':engine,'originals':16384,'requests':4096,'frontiers':[17409]*3,'checkpoint_counts':[3]*3 if engine=='voteboat' else None,'performance_claim':False})
events=[json.loads(s) for s in (root/'logs/matched-release.jsonl').read_text().splitlines() if s.startswith('{')]
artifacts=[]
for name in ['voteboat','voteboat-matched-framework','openraft','bench_minimal']:
 records=[e for e in events if e.get('reason')=='compiler-artifact' and e.get('target',{}).get('name')==name]
 assert len(records)==1,(name,len(records));r=records[0];p=r['profile']
 assert r['fresh'] is False and p['opt_level']=='3' and p['debug_assertions'] and p['overflow_checks'] and p['debuginfo']==2,r
 src=Path(r['target']['src_path'])
 if name in ['voteboat','voteboat-matched-framework']:
  rel=str(src.resolve().relative_to((root/'source').resolve()));assert h(src)==source['sha256'][rel]
 else:
  checkout=next(a for a in src.parents if (a/'.git').exists());assert subprocess.check_output(['/usr/bin/git','-C',str(checkout),'rev-parse','HEAD']).decode().strip()=='e88bb40707f2e511d8ecc44d31edcb1d814a11a8'
 original=Path(r['executable']) if r.get('executable') else Path(next(f for f in r['filenames'] if f.endswith('.rlib')))
 copy=root/'artifacts'/'matched'/original.name;copy.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(original,copy);assert h(copy)==h(original)
 if r.get('executable'):copy.chmod(0o555)
 artifacts.append({'target':name,'original':str(original),'copy':str(copy),'sha256':h(copy),'compiler_record':r})
lines=[s for s in (root/'logs/matched-release.stderr').read_text().splitlines() if any(f'--crate-name {n} ' in s for n in ['voteboat','voteboat_matched_framework','openraft','bench_minimal'])]
assert len(lines)==4 and all('-C codegen-units=1' in s for s in lines) and any('-C lto=thin' in s for s in lines),len(lines)
(root/'logs/matched-root-rustc-lines.txt').write_text('\n'.join(lines)+'\n')
(root/'ARTIFACTS.json').write_text(json.dumps(artifacts,indent=2)+'\n')
(root/'QUALIFICATION.json').write_text(json.dumps({'commit':source['commit'],'guarded_inputs':source['inputs'],'scope':'Full426 scoped affected providers/log/snapshot, real TCP/QUIC retry/replacement/native recovery, formatting four strict profiles, matched34 and functional off/on/OpenRaft maintenance histories; no timing or floor claim','controls':controls,'test_counts':counts,'four_artifacts_fresh_false':True,'profile':'opt3/debug2/assertions/overflow/thinLTO/codegen1','source_unchanged':True,'source_permissions_preserved':True,'clippy_config_sha256':source['sha256']['clippy.toml']},indent=2)+'\n')
print('source/permissions, scoped tests, four strict profiles, matched34, CLI maintenance, observer28/4/14, and four fresh asserted artifact proofs pass')
