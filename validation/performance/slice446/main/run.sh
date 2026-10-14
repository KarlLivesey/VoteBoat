#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice446/main
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";result=$?;set -e;printf '%s\n' "$result" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$result";return "$result"; }
printf '%s\n' "$BASHPID" > "$job/PID"
printf 'format\n' > "$job/STAGE"
run format-write cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml
python3 - <<'PY'
from pathlib import Path
import json,hashlib,subprocess,shutil
out=Path('target/p9/slice446/main');base=json.loads((out/'SOURCE_BEFORE.json').read_text());paths=sorted(set(base['sha256'])|set(subprocess.check_output(['rg','--files','src','tests','examples','benchmarks/matched/src'],text=True).splitlines()));v={'baseline_commit':base['baseline_commit'],'inputs':len(paths),'sha256':{r:hashlib.sha256(Path(r).read_bytes()).hexdigest() for r in paths},'physical_modes':{r:oct(Path(r).stat().st_mode&0o777) for r in paths}}
v['changed']=[r for r in paths if r in base['sha256'] and v['sha256'][r]!=base['sha256'][r]];v['new']=[r for r in paths if r not in base['sha256']];v['unchanged_inputs']=len(paths)-len(v['changed'])-len(v['new']);(out/'SOURCE.json').write_text(json.dumps(v,indent=2)+'\n')
for rel in v['changed']+v['new']:
 q=out/'source-final'/rel;q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(rel,q)
(out/'PATCH.diff').write_bytes(subprocess.check_output(['git','diff','--',*v['changed']]))
print(v['inputs'],v['changed'],v['new'])
PY
printf 'matched_tests\n' > "$job/STAGE"
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1 --nocapture
printf 'matched_format\n' > "$job/STAGE"
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
printf 'matched_strict\n' > "$job/STAGE"
run matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'source_guard\n' > "$job/STAGE"
run source-guard python3 - <<'PY'
from pathlib import Path
import json,hashlib,subprocess
p=Path('target/p9/slice446/main');m=json.loads((p/'SOURCE.json').read_text());base=json.loads((p/'SOURCE_BEFORE.json').read_text())
for rel,sha in m['sha256'].items():
 assert hashlib.sha256(Path(rel).read_bytes()).hexdigest()==sha,rel
 assert oct(Path(rel).stat().st_mode&0o777)==m['physical_modes'][rel],rel
unchanged=[r for r in base['sha256'] if not r.startswith('benchmarks/matched/src/')]
assert all(m['sha256'][r]==base['sha256'][r] and m['physical_modes'][r]==base['physical_modes'][r] for r in unchanged)
(p/'ROOT_REUSE.json').write_text(json.dumps({'unchanged_outside_matched_src':len(unchanged),'exact_hash_modes':True,'root_functional_checks_not_repeated':True,'scope':'No productionroot/examples/tests/locks/config/contract edits; only privatecomparisonactor table+diagnostics/newtests'},indent=2)+'\n')
print(f'exact{m["inputs"]}checkedinputs/hash/modes;rootoutsidebenchunchanged{len(unchanged)}')
PY
printf 'DONE\n' > "$job/STAGE"
