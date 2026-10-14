#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice448/main
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";result=$?;set -e;printf '%s\n' "$result" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$result";return "$result"; }
printf '%s\n' "$BASHPID" > "$job/PID"
printf 'format\n' > "$job/STAGE"
run final-format-write cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml
python3 - <<'PY'
from pathlib import Path
import json,hashlib,subprocess,shutil
out=Path('target/p9/slice448/main');base=json.loads((out/'SOURCE.before.json').read_text());paths=sorted(set(base['sha256'])|set(subprocess.check_output(['rg','--files','src','tests','examples','benchmarks/matched/src'],text=True).splitlines()));v={'baseline_commit':base['baseline_commit'],'inputs':len(paths),'sha256':{r:hashlib.sha256(Path(r).read_bytes()).hexdigest() for r in paths},'physical_modes':{r:oct(Path(r).stat().st_mode&0o777) for r in paths}}
v['changed']=[r for r in paths if r in base['sha256'] and v['sha256'][r]!=base['sha256'][r]];v['new']=[r for r in paths if r not in base['sha256']];v['unchanged_inputs']=len(paths)-len(v['changed'])-len(v['new']);(out/'SOURCE.json').write_text(json.dumps(v,indent=2)+'\n')
for rel in v['changed']+v['new']:
 q=out/'source-final'/rel;q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(rel,q)
patch=subprocess.check_output(['git','diff','--',*v['changed']])
for rel in v['new']:
 result=subprocess.run(['git','diff','--no-index','--','/dev/null',rel],capture_output=True);assert result.returncode==1;patch+=result.stdout
(out/'PATCH.diff').write_bytes(patch)
print(v['inputs'],v['changed'],v['new'])
PY
printf 'matched_tests\n' > "$job/STAGE"
run final-coop cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline cooperative_budget_matches_tokio_for_ready_and_closed_receipts -- --test-threads=1 --nocapture
run final-layout cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline actual_dev_reply_group_endpoint_and_client_future_layouts -- --test-threads=1 --nocapture
printf 'matched_format\n' > "$job/STAGE"
run final-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
printf 'matched_strict\n' > "$job/STAGE"
run final-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'source_guard\n' > "$job/STAGE"
run final-source-guard python3 - <<'PY'
from pathlib import Path
import json,hashlib,subprocess
p=Path('target/p9/slice448/main');m=json.loads((p/'SOURCE.json').read_text());base=json.loads((p/'SOURCE.before.json').read_text())
for rel,sha in m['sha256'].items():
 assert hashlib.sha256(Path(rel).read_bytes()).hexdigest()==sha,rel
 assert oct(Path(rel).stat().st_mode&0o777)==m['physical_modes'][rel],rel
unchanged=[r for r in base['sha256'] if not r.startswith('benchmarks/matched/src/')]
assert all(m['sha256'][r]==base['sha256'][r] and m['physical_modes'][r]==base['physical_modes'][r] for r in unchanged)
(p/'ROOT_REUSE.json').write_text(json.dumps({'unchanged_outside_matched_src':len(unchanged),'exact_hash_modes':True,'root_functional_checks_not_repeated':True,'scope':'No productionroot/examples/tests/locks/config/contract edits; only privatecomparison replygroup, factoryintegration+newtests'},indent=2)+'\n')
print(f'exact{m["inputs"]}checkedinputs/hash/modes;rootoutsidebenchunchanged{len(unchanged)}')
PY
printf 'DONE\n' > "$job/STAGE"
