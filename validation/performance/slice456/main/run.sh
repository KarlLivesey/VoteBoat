#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice456
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
trap 'rc=$?;printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run all-lib cargo +stable test --locked --offline --all-features --lib -- --test-threads=1
run accounting cargo +stable test --locked --offline --all-features --test log_store --test provider_conformance --test snapshot -- --test-threads=1
run core-lib cargo +stable test --locked --offline --no-default-features --lib -- --test-threads=1
run matched cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
run format cargo +stable fmt --all -- --check
run strict-default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run strict-all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run strict-core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run strict-native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-strict cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run source-guard python3 - <<'PY'
from pathlib import Path
import json,hashlib
v=json.loads(Path('target/p9/slice456/SOURCE.json').read_text())
for p,h in v['sha256'].items():
 assert hashlib.sha256(Path(p).read_bytes()).hexdigest()==h,p
 assert oct(Path(p).stat().st_mode&0o777)==v['physical_modes'][p],p
print('456 source/modes unchanged',v['inputs'])
PY
printf 'DONE\n' > "$job/STAGE"
