#!/bin/bash
set -uo pipefail
ROOT=$1
cd "$ROOT/repo" || exit 2
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export CARGO_TARGET_DIR="$ROOT/build-checks"
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_BUILD_JOBS=2
export TMPDIR="$ROOT/test-tmp"
mkdir -p "$ROOT/results" "$TMPDIR"
ulimit -n 4096
failures=0
run() {
 label=$1
 shift
 printf '%s\n' "$label" > "$ROOT/results/stage.txt"
 printf '%q ' "$@" > "$ROOT/results/$label.command"
 printf '\n' >> "$ROOT/results/$label.command"
 "$@" > "$ROOT/results/$label.log" 2>&1
 result=$?
 printf '%s=%s\n' "$label" "$result" >> "$ROOT/results/exits.txt"
 if [ "$result" -ne 0 ]; then failures=$((failures+1)); fi
}
run platform bash -c 'sysctl -n machdep.cpu.brand_string; uname -sm; rustc +stable --version; cargo +stable --version; printf "CARGO_TARGET_DIR=%s\n" "$CARGO_TARGET_DIR"; printf "PROTOC=%s\n" "$PROTOC"'
run source_start shasum -a 256 -c "$ROOT/SOURCE_SHA256.txt"
run fmt cargo +stable fmt --all -- --check
printf 'clippy_all\n' > "$ROOT/results/stage.txt"
printf '%s\n' 'cargo +stable clippy --locked --offline --keep-going --all-targets --all-features --message-format=json -- -D warnings' > "$ROOT/results/clippy_all.command"
cargo +stable clippy --locked --offline --keep-going --all-targets --all-features --message-format=json -- -D warnings > "$ROOT/results/clippy_all.jsonl" 2> "$ROOT/results/clippy_all.log"
result=$?
printf 'clippy_all=%s\n' "$result" >> "$ROOT/results/exits.txt"
if [ "$result" -ne 0 ]; then failures=$((failures+1)); fi
run compile_identity node "$ROOT/check-compile.mjs" "$ROOT"
run clippy_default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run clippy_core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run clippy_native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
run correctness_all cargo +stable test --locked --offline --all-features --lib --test effect_owner --test worker --test log_owned --example framework_benchmark
run source_after_all shasum -a 256 -c "$ROOT/SOURCE_SHA256.txt"
run correctness_core cargo +stable test --locked --offline --no-default-features --lib --test effect_owner --test worker --test log_owned --example framework_benchmark
run source_terminal shasum -a 256 -c "$ROOT/SOURCE_SHA256.txt"
printf 'failures=%s\n' "$failures" > "$ROOT/results/terminal.txt"
printf 'terminal\n' > "$ROOT/results/stage.txt"
exit "$failures"
