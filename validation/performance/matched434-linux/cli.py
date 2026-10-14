from pathlib import Path
import hashlib,json,shlex,subprocess
j=Path(__file__).resolve().parent
assert (j/'STATUS').read_text().strip()=='0' and (j/'STAGE').read_text().strip()=='DONE'
assert (j/'RELEASE_ORACLE.json').is_file() and (j/'SYMBOLS.json').is_file()
meta=json.loads((j/'SOURCE.json').read_text());art=json.loads((j/'ARTIFACTS.json').read_text());exe=next(r for r in art if r['target']=='voteboat-matched-framework');binary=Path(exe['copy'])
h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert h(binary)==exe['sha256']
base=[str(binary),'--clients','4096','--batch','4','--operations','16384','--warmup','1024','--maintenance','default-retention','--envelopes','upstream-default','--campaign-timeout-secs','3600']
rows=[]
for name,engine,trace in [('off','voteboat',False),('on','voteboat',True),('openraft','openraft',False)]:
 args=base+['--engine',engine]+(['--attribution',str(trace).lower()] if engine=='voteboat' else [])
 (j/'CLI_STAGE').write_text(name+'\n');(j/'logs'/(name+'.command')).write_text(shlex.join(args)+'\n')
 with (j/'logs'/(name+'.json')).open('wb') as stdout,(j/'logs'/(name+'.stderr')).open('wb') as stderr:
  result=subprocess.run(args,stdout=stdout,stderr=stderr)
 (j/'logs'/(name+'.status')).write_text(str(result.returncode)+'\n');assert result.returncode==0,(name,result.returncode)
 value=json.loads((j/'logs'/(name+'.json')).read_text())
 assert value['engine']==engine and value['completed_originals']==16384 and value['completed_requests']==4096
 assert value['applied_frontiers']==[17409]*3 and value['client_workers']==1 and value['server_workers']==16 and value['members']==3 and value['payload_bytes']==0
 assert value['upstream_commit']=='e88bb40707f2e511d8ecc44d31edcb1d814a11a8'
 assert value['settings']['operations']==16384 and value['settings']['warmup']==1024 and value['settings']['maintenance']=='default_retention'
 assert value['replication_entry_capacity']==1024 and value['primary_upstream_envelopes_preserved']
 assert 'all_client_joins_excludes_warmup_latency_flatten_sort_follower_verification_and_shutdown' in value['throughput_timing_scope']
 if trace:
  reports=value['attribution'];assert len(reports)==3 and sorted(r['replica_slot'] for r in reports)==[0,1,2]
  for report in reports:
   phases=report['phases'];assert len(phases)==29 and len({p['phase'] for p in phases})==29
   for p in phases:assert p['failures']==0 and p['total_elapsed_ns']>=p['maximum_elapsed_ns']
   for required in ('store_state_clone_nested','store_matches_state_exact_nested','store_checkpoint_state_protected_inclusive_nested'):
    assert sum(p['phase']==required for p in phases)==1
   scopes=report['checkpoint_provider_scopes'];assert len(scopes)==4
   for scope in scopes:
    methods=scope['methods'];assert len(methods)==15 and len({p['method'] for p in methods})==15
    assert all(p['failures']==0 for p in methods)
   compact=next(s for s in scopes if s['scope']=='checkpoint_compact_inclusive');methods={p['method']:p for p in compact['methods']}
   assert methods['store_checkpoint_state_protected_inclusive_nested']['calls']==3
   assert methods['store_checkpoint_state_protected_inclusive_nested']['items']>0
   assert methods['store_matches_state_exact_nested']['calls']==0 and methods['store_state_clone_nested']['calls']==0
   gaps=report['checkpoint_compact_gaps'];assert len(gaps)==3 and {g['gap'] for g in gaps}=={'state_to_pin','pin_to_append','barrier_to_reconcile'}
   assert all(g['calls']==3 and g['failures']==0 and g['total_elapsed_ns']>=g['maximum_elapsed_ns'] for g in gaps)
   assert 'starts_after_protected_checkpoint_verification_including_any_full_fallback' in report['checkpoint_compact_gap_scope']
   assert 'not_actual_compared_or_skipped_entries_full_fallback_inside_this_phase' in report['item_units']
  assert len(value['diagnostics'])==3 and all(r['checkpoint_count']==3 for r in value['diagnostics'])
 else:assert not value.get('attribution')
 assert h(binary)==exe['sha256']
 rows.append({'case':name,'completed_originals':value['completed_originals'],'completed_requests':value['completed_requests'],'applied_frontiers':value['applied_frontiers'],'phase_scope_method_counts':[29,4,15] if trace else None,'guarded_output_sha256':h(j/'logs'/(name+'.json')),'meaning':'small functional control; CLI rates not sustained or paired capacity evidence'})
assert all(h(j/'source'/p)==digest for p,digest in meta['sha256'].items())
(j/'CLI.json').write_text(json.dumps(rows,indent=2)+'\n');(j/'CLI_STAGE').write_text('DONE\n');print('all3 functional CLI controls and exact source/artifact guards passed')
