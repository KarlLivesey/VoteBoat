#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice466/checks2
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
export CARGO_BUILD_JOBS=2
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";printf '%s\n' "$name" > "$job/STAGE";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run membership-helper cargo +stable test --locked --offline --all-features --test membership -- --test-threads=1
run member-service-helper cargo +stable test --locked --offline --all-features --test counter_service member_service_recovers -- --test-threads=1
run metadata node validation/check-provider-conformance.mjs
run enabled-hook .githooks/pre-push
printf 'DONE\n' > "$job/STAGE"
