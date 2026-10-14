import hashlib,json,stat
from pathlib import Path
root=Path(__file__).resolve().parent
required_commit=(root/'REQUIRED_COMMIT').read_text().strip()
assert len(required_commit)==40 and all(c in '0123456789abcdef' for c in required_commit),'unpublished candidate refused'
source=json.loads((root/'SOURCE.json').read_text())
assert source['commit']==required_commit
actual={p.relative_to(root/'source').as_posix() for p in (root/'source').rglob('*') if p.is_file()}
assert actual==set(source['sha256']),(actual-set(source['sha256']),set(source['sha256'])-actual)
for relative,digest in source['sha256'].items():
 path=root/'source'/relative
 assert hashlib.sha256(path.read_bytes()).hexdigest()==digest,relative
 assert oct(stat.S_IMODE(path.stat().st_mode))==source['modes'][relative],relative
proof={'commit':source['commit'],'inputs':len(actual),'exact_sha256_and_modes':True,'build_rs_present':True,'archive_sha256':source['archive_sha256'],'source_inventory_scope':source['inventory_scope']}
(root/'SOURCE_TRANSFER.json').write_text(json.dumps(proof,indent=2)+'\n')
print(json.dumps(proof,sort_keys=True))

import sys
if len(sys.argv)>1:
 import hashlib,json,sys
 from pathlib import Path
 j=Path(__file__).resolve().parent
 h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
 s=json.loads((j/'SOURCE.json').read_text());r=json.loads((j/'ROOT_REUSE.json').read_text());b=Path(r['baseline_remote']);old=json.loads((b/'SOURCE.json').read_text());q=json.loads((b/'QUALIFICATION.json').read_text())
 assert old['commit']==r['baseline_commit'] and old['inputs']==1436 and s['inputs']==1437
 proof={Path(p['path']).name:p['sha256'] for p in r['proofs'] if '/mac435/collected/' in p['path']}
 assert h(b/'SOURCE.json')==proof['SOURCE.json'] and h(b/'QUALIFICATION.json')==proof['QUALIFICATION.json']
 for rel in r['unchanged_paths']:
  assert s['sha256'][rel]==old['sha256'][rel] and s['modes'][rel]==old['modes'][rel]
  assert h(j/'source'/rel)==s['sha256'][rel] and h(b/'source'/rel)==old['sha256'][rel]
 e=j/'source'/r['engine'];suffix=r['exact_cfgtest_suffix'].encode();raw=e.read_bytes();assert raw.endswith(suffix) and hashlib.sha256(raw[:-len(suffix)]).hexdigest()==r['engine_projection_sha256']==old['sha256'][r['engine']]
 assert set(s['sha256'])-set(old['sha256'])=={'benchmarks/matched/src/voteboat_engine/client_outcomes.rs'}
 client=Path(r['client437_remote']);control='benchmarks/matched/src/voteboat_engine/client_outcomes.rs';assert h(j/'source'/control)==r['client437_control_sha256']==h(client/'source'/control);assert h(client/'QUALIFICATION.json')==r['client437_qualification_sha256'];assert (client/'logs/client_outcomes.exit').read_text().strip()=='0' and '8 passed; 0 failed' in (client/'logs/client_outcomes.log').read_text();assert h(j/'source'/'benchmarks/matched/src/voteboat_engine/actor.rs')==old['sha256']['benchmarks/matched/src/voteboat_engine/actor.rs']
 for name in ['rustc.txt','cargo.txt','tool-sha256.txt']:assert (j/'logs'/name).read_bytes()==(b/'logs'/name).read_bytes()==(client/'logs'/name).read_bytes(),name
 for name in ['affected_all','affected_core','effect_owner_all','effect_owner_core','services','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_fmt','matched_tests','matched_strict']:
  assert (b/'logs'/f'{name}.exit').read_text().strip()=='0',name
  assert (b/'logs'/f'{name}.command').is_file()
 assert '56 passed; 0 failed' in (b/'logs/matched_tests.log').read_text()
 if sys.argv[1]=='final':
  for name in ['replies','matched_fmt','matched_strict']:assert (j/'logs'/f'{name}.exit').read_text().strip()=='0'
  assert '8 passed; 0 failed' in (j/'logs/replies.log').read_text()
 result={'stage':sys.argv[1],'source_inputs':1437,'unchanged_prior_inputs':r['unchanged_prior_hashes_modes'],'engine_projection_exact':True,'same_compiler_tool_protoc_hashes':True,'old435_functional_scope_and437client8_reused':True,'currentreply8_executed':sys.argv[1]=='final','baseline_proof_command_hashes':{name:h(b/'logs'/f'{name}.command') for name in ['affected_all','affected_core','effect_owner_all','effect_owner_core','services','root_fmt','strict_default','strict_all','strict_core','strict_native','matched_tests']},'actual8_fmt_strict_pass':sys.argv[1]=='final','no_new_performance_claim':True}
 (j/f'REUSE_{sys.argv[1]}.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,sort_keys=True))
