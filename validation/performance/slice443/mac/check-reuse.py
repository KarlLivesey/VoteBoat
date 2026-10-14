import hashlib,json,stat
from pathlib import Path
j=Path(__file__).resolve().parent;r=json.loads((j/'ROOT_REUSE.json').read_text());b=Path(r['baseline_remote']);s=json.loads((j/'SOURCE.json').read_text());h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
assert s['commit']==r['candidate_commit']
for rel,d in r['exact_scoped_sha256'].items():
 assert h(j/'source'/rel)==h(b/'source'/rel)==d,rel
 assert oct(stat.S_IMODE((j/'source'/rel).stat().st_mode))==oct(stat.S_IMODE((b/'source'/rel).stat().st_mode))==r['exact_scoped_modes'][rel],rel
for rel,d in r['baseline_evidence_sha256'].items():assert h(b/rel)==d,rel
for n in r['reused_receipt_steps']:assert(b/f'logs/{n}.exit').read_text().strip()=='0'
for n in ['rustc.txt','cargo.txt','tool-sha256.txt']:assert(j/'logs'/n).read_bytes()==(b/'logs'/n).read_bytes()
assert 'required-features = ["tls"]'in(j/'source/Cargo.toml').read_text().split('name = "native_benchmark"')[1].split('[[example]]')[0]
print('exact noTLS/core selected source/modes/tool/commands receipt reuse; changed native example excluded by requiredTLS; protected private fixtures excluded; no broader rootfunctional claim')
