#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice455/crc
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run format cargo +stable fmt --all -- --check
run crc-focused cargo +stable test --locked --offline --all-features --lib format_crc::tests -- --test-threads=1
run all-lib cargo +stable test --locked --offline --all-features --lib -- --test-threads=1
run format-integrations cargo +stable test --locked --offline --all-features --test parser_campaign --test wire --test log_store --test striped_log_store --test snapshot --test native_member_startup -- --test-threads=1
run core-lib cargo +stable test --locked --offline --no-default-features --lib -- --test-threads=1
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
run metadata node validation/check-provider-conformance.mjs
run strict-default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run strict-all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run strict-core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run strict-native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-strict cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run candidate-cold cargo +stable test --locked --offline --all-features --test logical_nested_native cold_recovery::quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores -- --exact --test-threads=1
run source-guard python3 - <<'PY'
from pathlib import Path
import json,hashlib
v=json.loads(Path('target/p9/slice455/crc/SOURCE.json').read_text())
for p,h in v['sha256'].items():
 assert hashlib.sha256(Path(p).read_bytes()).hexdigest()==h,p
 assert oct(Path(p).stat().st_mode&0o777)==v['physical_modes'][p],p
print('CRC source scope unchanged',v['inputs'])
PY
printf 'DONE\n' > "$job/STAGE"
