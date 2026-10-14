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
 reuse=json.loads((root/'ROOT_REUSE.json').read_text());baseline=Path(reuse['baseline_job']);h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
 assert reuse['candidate_commit']==required_commit and reuse['baseline_commit']=='d6395be7ec56af68f346a72308e90a20200fb731'
 assert set(reuse['excluded_changed_private_files'])=={'benchmarks/matched/src/voteboat_engine.rs','benchmarks/matched/src/voteboat_engine/client_outcomes.rs'}
 assert set(reuse['exact_unchanged_inputs'])==set(source['sha256'])-set(reuse['excluded_changed_private_files'])
 assert len(reuse['exact_unchanged_inputs'])==1438
 for rel,digest in reuse['exact_unchanged_inputs'].items():
  assert source['sha256'][rel]==digest and h(root/'source'/rel)==h(baseline/'source'/rel)==digest,rel
  assert source['modes'][rel]==reuse['modes'][rel]==oct(stat.S_IMODE((baseline/'source'/rel).stat().st_mode)),rel
 for rel,digest in reuse['baseline_evidence_sha256'].items():assert h(baseline/rel)==digest,rel
 for name in reuse['reused_steps']:assert (baseline/f'logs/{name}.exit').read_text().strip()=='0',name
 for name in ['rustc.txt','cargo.txt','tool-sha256.txt']:assert (root/'logs'/name).read_bytes()==(baseline/'logs'/name).read_bytes(),name
 assert (baseline/'STATUS').read_text().strip()=='0' and (baseline/'STAGE').read_text().strip()=='DONE'
 print('Exact450 root1438source/mode/tool/command scopes reused; actual new matched70 follows')
