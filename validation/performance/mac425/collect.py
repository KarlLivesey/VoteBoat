import hashlib,json,re,shutil,subprocess
from pathlib import Path
root=Path('/tmp/voteboat-mac425-ccee8ec');h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
source=json.loads((root/'SOURCE.json').read_text())
for rel,digest in source['sha256'].items():assert h(root/'source'/rel)==digest,rel
assert 'clippy.toml' in source['sha256']
assert re.findall(r'test result: ok\. (\d+) passed; (\d+) failed;', (root/'logs/matched_tests.log').read_text())==[('32','0')]
for name in ['matched_fmt','matched_tests','matched_strict','matched_cli_build','cli-false','cli-true','matched-release']:
 assert (root/f'logs/{name}.exit').read_text().strip()=='0',name
controls=[]
for on in [False,True]:
 name=f'cli-{str(on).lower()}';d=json.loads((root/f'logs/{name}.log').read_text())
 assert d['settings']['attribution']==on and d['settings']['operations']==12000 and d['settings']['warmup']==0
 assert d['completed_originals']==12000 and d['completed_requests']==3000 and d['applied_frontiers']==[12001]*3
 assert all(x['checkpoint_count']==2 for x in d['diagnostics'])
 assert sum(x['proposed_originals'] for x in d['diagnostics'])==12000
 assert sum(sum(w['original_entries']*w['batches'] for w in x['proposal_widths']) for x in d['diagnostics'])==12000
 assert [d[k] for k in ['api_entry_batch_capacity','append_entry_batch_capacity','replication_entry_capacity']]==[16384,16384,1024]
 if not on:assert 'attribution' not in d
 else:
  assert len(d['attribution'])==3
  for a in d['attribution']:
   assert len(a['phases'])==27 and len(a['checkpoint_provider_scopes'])==4
   assert 'not_cpu_or_critical_path' in a['clock'] and 'reported_compact_gaps' in a['residual'] and 'nested_pinned_load' in a['checkpoint_compact_gap_scope']
   assert [g['gap'] for g in a['checkpoint_compact_gaps']]==['state_to_pin','pin_to_append','barrier_to_reconcile']
   for g in a['checkpoint_compact_gaps']:
    assert g['calls']==2 and g['failures']==0 and g['items']==0 and 0<=g['maximum_elapsed_ns']<=g['total_elapsed_ns']
   for p in a['phases']:assert p['failures']==0 and 0<=p['maximum_elapsed_ns']<=p['total_elapsed_ns']
   assert sum(g['total_elapsed_ns'] for g in a['checkpoint_compact_gaps'])<=next(p['total_elapsed_ns'] for p in a['phases'] if p['phase']=='checkpoint_compact_inclusive')
 controls.append({'name':name,'originals':12000,'requests':3000,'frontiers':[12001]*3,'checkpoint_counts':[2]*3,'performance_claim':False})
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
(root/'QUALIFICATION.json').write_text(json.dumps({'commit':source['commit'],'guarded_inputs':source['inputs'],'scope':'benchmark425 six-path delta; exact production unchanged from41; standalone32/fmt/strict and functional debug histories only; no production suite repeated or timing claims','controls':controls,'four_artifacts_fresh_false':True,'profile':'opt3/debug2/assertions/overflow/thinLTO/codegen1','source_unchanged':True,'clippy_config_sha256':source['sha256']['clippy.toml']},indent=2)+'\n')
print('source,32tests,fmt,strict,original/frontier/gaps and four fresh asserted artifact proofs pass')
