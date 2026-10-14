#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice455/main
trap 'rc=$?; printf "%s\n" "$rc" > "$job/PADDING_STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PADDING_PID"
printf 'format_freeze\n' > "$job/STAGE"
run final-format-write cargo +stable fmt --all
run final-matched-format-write cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml
python3 - <<'PY'
from pathlib import Path
import json,hashlib,subprocess,shutil
j=Path('target/p9/slice455/main');v=json.loads((j/'SOURCE.json').read_text());v['sha256']={r:hashlib.sha256(Path(r).read_bytes()).hexdigest() for r in v['sha256']};v['physical_modes']={r:oct(Path(r).stat().st_mode&0o777) for r in v['sha256']};v['changed']=sorted(set(subprocess.check_output(['git','diff','--name-only'],text=True).splitlines()) & set(v['sha256']));v['new']=sorted(set(subprocess.check_output(['git','ls-files','--others','--exclude-standard'],text=True).splitlines()) & set(v['sha256']));(j/'SOURCE.json').write_text(json.dumps(v,indent=2)+'\n');patch=subprocess.check_output(['git','diff','--',*v['changed']])
for r in v['new']:
 z=subprocess.run(['git','diff','--no-index','--','/dev/null',r],capture_output=True);assert z.returncode==1;patch+=z.stdout
(j/'PATCH.diff').write_bytes(patch)
for r in v['changed']+v['new']:
 p=j/'source-final'/r;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(r,p)
print('final frozen',v['inputs'],'source/modes, changed',len(v['changed']),'new',len(v['new']))
PY
printf 'padded_library\n' > "$job/STAGE"
run padded-all-lib cargo +stable test --locked --offline --all-features --lib -- --test-threads=1
run padded-core-lib cargo +stable test --locked --offline --no-default-features --lib -- --test-threads=1
printf 'padded_accounting_integrations\n' > "$job/STAGE"
run padded-integrations cargo +stable test --locked --offline --all-features --test outbound --test provider_conformance --test wire --test log_store --test log_staging --test log_owned --test log_reclaim --test parser_campaign --test configuration_capacity --test snapshot --test effect_owner -- --test-threads=1
printf 'padded_matched\n' > "$job/STAGE"
run padded-matched cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
printf 'strict_four_profiles\n' > "$job/STAGE"
run root-format cargo +stable fmt --all -- --check
run strict-default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run strict-all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run strict-core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run strict-native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-strict cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'candidate_cold_focused\n' > "$job/STAGE"
run candidate-cold cargo +stable test --locked --offline --all-features --test logical_nested_native cold_recovery::quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores -- --exact --test-threads=1
printf 'source_guard\n' > "$job/STAGE"
run source-guard python3 - <<'CHECK'
from pathlib import Path
import json,hashlib
v=json.loads(Path('target/p9/slice455/main/SOURCE.json').read_text())
for r,h in v['sha256'].items():
 assert hashlib.sha256(Path(r).read_bytes()).hexdigest()==h,r
 assert oct(Path(r).stat().st_mode&0o777)==v['physical_modes'][r],r
print('padded candidate frozen source/modes unchanged',v['inputs'])
CHECK
printf 'DONE\n' > "$job/STAGE"
