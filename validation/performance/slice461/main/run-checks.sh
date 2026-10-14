#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice461/main
mkdir -p "$job"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
export CARGO_BUILD_JOBS=2
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
trap 'rc=$?;printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run compiler rustc +stable -Vv
run cargo cargo +stable -V
run lib-all cargo +stable test --locked --offline --all-features --lib -- --test-threads=1
run integrations-all cargo +stable test --locked --offline --all-features --test application --test provider_conformance --test raft --test runtime --test effect_owner --test log_store --test snapshot -- --test-threads=1
run services-all cargo +stable test --locked --offline --all-features --test counter_service three_ -- --test-threads=1
run lib-core cargo +stable test --locked --offline --no-default-features --lib -- --test-threads=1
run integrations-core cargo +stable test --locked --offline --no-default-features --test application --test provider_conformance --test raft --test runtime --test effect_owner --test log_store --test snapshot -- --test-threads=1
run matched cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
run provider-metadata node validation/check-provider-conformance.mjs
run enabled-hook .githooks/pre-push
printf 'DONE\n' > "$job/STAGE"
