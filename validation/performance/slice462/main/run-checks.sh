#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice462/main
mkdir -p "$job"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
trap 'rc=$?;printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run providers-all cargo +stable test --locked --offline --all-features --test provider_conformance -- --test-threads=1
run providers-core cargo +stable test --locked --offline --no-default-features --test provider_conformance -- --test-threads=1
run enabled-hook .githooks/pre-push
printf 'DONE\n' > "$job/STAGE"
