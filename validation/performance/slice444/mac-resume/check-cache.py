import hashlib,json
from pathlib import Path
j=Path(__file__).resolve().parent;b=Path('/tmp/voteboat-mac441-d4f1d39-cpLN4U');h=lambda f:hashlib.sha256(f.read_bytes()).hexdigest();s=json.loads((j/'SOURCE.json').read_text());old=json.loads((b/'SOURCE.json').read_text())
assert(b/'STATUS').read_text().strip()=='0' and(b/'STAGE').read_text().strip()=='DONE' and(b/'target-matched-checks').is_dir()
for n in ['rustc.txt','cargo.txt','tool-sha256.txt']:assert(j/'logs'/n).read_bytes()==(b/'logs'/n).read_bytes(),n
paths=['Cargo.toml','Cargo.lock','build.rs','clippy.toml','rust-toolchain.toml','benchmarks/matched/Cargo.toml','benchmarks/matched/Cargo.lock']+[rel for rel in s['sha256']if rel.startswith('src/')]
for rel in paths:assert h(j/'source'/rel)==s['sha256'][rel]==old['sha256'][rel]==h(b/'source'/rel),rel
for step in ['replies','matched_fmt','matched_strict']:assert(b/f'logs/{step}.exit').read_text().strip()=='0'
r={'cache':str(b/'target-matched-checks'),'source_scope':'exact compiledroot/config/locks bytehashes; currentchangedtests execute, no test-result reuse claim','exact_inputs':len(paths),'compiler_tools_same':True,'old_command_receipts_sha256':{n:h(b/f'logs/{n}.command')for n in ['replies','matched_fmt','matched_strict']},'no_performance_library_freshness_claim':True}
(j/'CACHE_REUSE.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r))
