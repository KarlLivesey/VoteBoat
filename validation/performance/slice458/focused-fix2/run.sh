#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice458/focused-fix2
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
printf '%s\n' "$BASHPID" > "$job/PID"
export CARGO_BUILD_JOBS=2
export CARGO_TARGET_DIR="$PWD/target/p9/slice455/padded-target-checks"
run(){ name=$1;shift;printf '%s\n' "$name" > "$job/STAGE";printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
run format-write cargo +stable fmt --all
run offered-tests cargo +stable test --locked --offline --all-features --example native_benchmark offered -- --test-threads=1
run format-check cargo +stable fmt --all -- --check
run strict-default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
printf 'DONE\n' > "$job/STAGE"
