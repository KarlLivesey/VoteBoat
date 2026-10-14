#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice463/preflight/fix2
mkdir -p "$job"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
export CARGO_BUILD_JOBS=2
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
trap 'rc=$?;printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run runtime-joined cargo +stable test --locked --offline --all-features --lib joined -- --test-threads=1
run source-defer cargo +stable test --locked --offline --all-features --lib deferred_payload_reservation_keeps_source_capacity_until_visit_retirement -- --test-threads=1
run benchmark-ready cargo +stable test --locked --offline --all-features --example native_benchmark ready_items -- --test-threads=1
run benchmark-joined cargo +stable test --locked --offline --all-features --example native_benchmark joined -- --test-threads=1
printf 'DONE\n' > "$job/STAGE"
