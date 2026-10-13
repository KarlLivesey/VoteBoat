#!/bin/bash
set -uo pipefail
ROOT=$1
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export CARGO_TARGET_DIR="$ROOT/build"
export CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
if [ "$(uname -s)" = Darwin ]; then
 export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
 SHA=shasum
else
 SHA=sha256sum
fi
cd "$ROOT/repo" || exit 2
mkdir -p "$ROOT/results"
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
run platform bash -c 'uname -sm; rustc +stable --version; cargo +stable --version; printf "CARGO_TARGET_DIR=%s\n" "$CARGO_TARGET_DIR"; printf "CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=%s\n" "$CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS"; printf "CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=%s\n" "$CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS"'
if [ "$SHA" = shasum ]; then
 run source_start shasum -a256 -c "$ROOT/SOURCE_SHA256.txt"
else
 run source_start sha256sum -c "$ROOT/SOURCE_SHA256.txt"
fi
printf 'build\n' > "$ROOT/results/stage.txt"
printf '%s\n' 'cargo +stable build --locked --offline --release --all-features --example native_benchmark --message-format=json' > "$ROOT/results/build.command"
cargo +stable build --locked --offline --release --all-features --example native_benchmark --message-format=json > "$ROOT/results/build.jsonl" 2> "$ROOT/results/build.log"
result=$?
printf 'build=%s\n' "$result" >> "$ROOT/results/exits.txt"
if [ "$result" -ne 0 ]; then failures=$((failures+1)); fi
run collect node "$ROOT/collect-artifacts.mjs" "$ROOT"
if [ "$SHA" = shasum ]; then
 run source_terminal shasum -a256 -c "$ROOT/SOURCE_SHA256.txt"
else
 run source_terminal sha256sum -c "$ROOT/SOURCE_SHA256.txt"
fi
printf 'failures=%s timed_cases=0\n' "$failures" > "$ROOT/results/terminal.txt"
printf 'terminal\n' > "$ROOT/results/stage.txt"
exit "$failures"
