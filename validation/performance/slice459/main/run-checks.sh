#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice459/main
mkdir -p "$job"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
trap 'rc=$?;printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run format-write cargo +stable fmt --all
run log-all cargo +stable test --locked --offline --all-features --lib log:: -- --test-threads=1
run compact-core-all cargo +stable test --locked --offline --all-features --lib raft::proposals::tests::compact -- --test-threads=1
run providers-snapshot-all cargo +stable test --locked --offline --all-features --test log_store --test provider_conformance --test snapshot -- --test-threads=1
run log-core cargo +stable test --locked --offline --no-default-features --lib log:: -- --test-threads=1
run compact-core-core cargo +stable test --locked --offline --no-default-features --lib raft::proposals::tests::compact -- --test-threads=1
run providers-snapshot-core cargo +stable test --locked --offline --no-default-features --test log_store --test provider_conformance --test snapshot -- --test-threads=1
run services-all cargo +stable test --locked --offline --all-features --test counter_service three_ -- --test-threads=1
run matched cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
run enabled-hook .githooks/pre-push
printf 'DONE\n' > "$job/STAGE"
