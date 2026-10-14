#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice450/main
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";result=$?;set -e;printf '%s\n' "$result" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$result";return "$result"; }
printf '%s\n' "$BASHPID" > "$job/PID"
printf 'format_and_freeze\n' > "$job/STAGE"
run format-write cargo +stable fmt --all
python3 - <<'PY'
from pathlib import Path
import json,hashlib,subprocess,shutil
out=Path('target/p9/slice450/main');base=json.loads((out/'SOURCE_BEFORE.json').read_text());paths=sorted(set(base['sha256'])|set(subprocess.check_output(['rg','--files','src','tests','examples','benchmarks/matched/src'],text=True).splitlines()));v={'baseline_commit':base['baseline_commit'],'inputs':len(paths),'sha256':{r:hashlib.sha256(Path(r).read_bytes()).hexdigest() for r in paths},'physical_modes':{r:oct(Path(r).stat().st_mode&0o777) for r in paths}}
v['changed']=[r for r in paths if r in base['sha256'] and v['sha256'][r]!=base['sha256'][r]];v['new']=[r for r in paths if r not in base['sha256']];assert v['changed']==['src/raft.rs'] and v['new']==['src/raft/append_prefix_tests.rs'],(v['changed'],v['new']);(out/'SOURCE.json').write_text(json.dumps(v,indent=2)+'\n')
for rel in v['changed']+v['new']:
 q=out/'source-final'/rel;q.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(rel,q)
patch=subprocess.check_output(['git','diff','--',*v['changed']]);new=subprocess.run(['git','diff','--no-index','--','/dev/null',v['new'][0]],capture_output=True);assert new.returncode==1;(out/'PATCH.diff').write_bytes(patch+new.stdout);print(v['inputs'],v['changed'],v['new'])
PY
printf 'focused\n' > "$job/STAGE"
run focused cargo +stable test --locked --offline --all-features --lib raft::append_prefix_tests -- --test-threads=1
printf 'allfeature_raft_library\n' > "$job/STAGE"
run all-raft-lib cargo +stable test --locked --offline --all-features --lib raft:: -- --test-threads=1
printf 'allfeature_integrations\n' > "$job/STAGE"
run all-integrations cargo +stable test --locked --offline --all-features --test raft --test replication_scope --test snapshot --test effect_owner -- --test-threads=1
printf 'core_raft_library\n' > "$job/STAGE"
run core-raft-lib cargo +stable test --locked --offline --no-default-features --lib raft:: -- --test-threads=1
printf 'core_integrations\n' > "$job/STAGE"
run core-integrations cargo +stable test --locked --offline --no-default-features --test raft --test replication_scope --test snapshot --test effect_owner -- --test-threads=1
printf 'actual_three_services\n' > "$job/STAGE"
run three-services cargo +stable test --locked --offline --all-features --test counter_service three_ -- --test-threads=1
printf 'matched\n' > "$job/STAGE"
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
printf 'root_fmt_strict\n' > "$job/STAGE"
run root-format cargo +stable fmt --all -- --check
run root-strict cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
printf 'matched_fmt_strict\n' > "$job/STAGE"
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-strict cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'source_guard\n' > "$job/STAGE"
run source-guard python3 - <<'PY'
from pathlib import Path
import hashlib,json
p=Path('target/p9/slice450/main');s=json.loads((p/'SOURCE.json').read_text())
for n,d in s['sha256'].items():assert hashlib.sha256(Path(n).read_bytes()).hexdigest()==d and oct(Path(n).stat().st_mode&0o777)==s['physical_modes'][n],n
print('all',s['inputs'],'final source hashes and physical modes unchanged; production raft source changed, affected scopes checked rather than reused')
PY
printf 'DONE\n' > "$job/STAGE"
