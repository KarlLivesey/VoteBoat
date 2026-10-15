#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice466/integration1
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
export CARGO_BUILD_JOBS=2
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";printf '%s\n' "$name" > "$job/STAGE";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run integration-all cargo +stable test --locked --offline --all-features --test log_store --test log_owned --test log_staging --test provider_conformance --test snapshot --test snapshot_worker --test raft --test runtime --test worker --test effect_owner --test membership --test learners --test member_recovery --test log_reclaim --test striped_log_store --test logical_formats -- --test-threads=1
run integration-core cargo +stable test --locked --offline --no-default-features --test log_owned --test log_staging --test provider_conformance --test snapshot --test snapshot_worker --test raft --test runtime --test worker --test effect_owner --test membership --test learners -- --test-threads=1
run service-recovery cargo +stable test --locked --offline --all-features --test counter_service three_ -- --test-threads=1
run matched cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
run metadata node validation/check-provider-conformance.mjs
printf 'DONE\n' > "$job/STAGE"
