#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice454/main
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
printf 'allfeature_library\n' > "$job/STAGE"
run all-lib cargo +stable test --locked --offline --all-features --lib -- --test-threads=1
printf 'allfeature_integrations\n' > "$job/STAGE"
run all-integrations cargo +stable test --locked --offline --all-features --test log_owned --test log_store --test provider_conformance --test raft --test snapshot --test effect_owner -- --test-threads=1
printf 'core_library\n' > "$job/STAGE"
run core-lib cargo +stable test --locked --offline --no-default-features --lib -- --test-threads=1
printf 'core_integrations\n' > "$job/STAGE"
run core-integrations cargo +stable test --locked --offline --no-default-features --test log_owned --test log_store --test provider_conformance --test raft --test snapshot --test effect_owner -- --test-threads=1
printf 'actual_three_services\n' > "$job/STAGE"
run three-services cargo +stable test --locked --offline --all-features --test counter_service three_ -- --test-threads=1
printf 'matched\n' > "$job/STAGE"
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
printf 'root_fmt_fourstrict\n' > "$job/STAGE"
run root-format cargo +stable fmt --all -- --check
run strict-default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run strict-all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run strict-core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run strict-native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
printf 'matched_fmt_strict\n' > "$job/STAGE"
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-strict cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'source_guard\n' > "$job/STAGE"
run source-guard python3 - <<'PY'
from pathlib import Path
import hashlib,json
v=json.loads(Path('target/p9/slice454/main/SOURCE.json').read_text())
for r,h in v['sha256'].items():assert hashlib.sha256(Path(r).read_bytes()).hexdigest()==h and oct(Path(r).stat().st_mode&0o777)==v['physical_modes'][r],r
print(v['inputs'],'frozen hashes and physical modes unchanged; private14sourcepaths qualified; no locks/dependencies/API/limits changes')
PY
printf 'DONE\n' > "$job/STAGE"
