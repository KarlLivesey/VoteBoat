#!/bin/bash
set -uo pipefail
ROOT=$1
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
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
run hardware bash -c 'sysctl -n machdep.cpu.brand_string; sysctl -n hw.physicalcpu; sysctl -n hw.logicalcpu; sysctl -n hw.memsize; sw_vers; uname -sm; rustc +stable --version; cargo +stable --version; node --version'
for variant in reference candidate; do
 export CARGO_TARGET_DIR="$ROOT/build-$variant"
 cd "$ROOT/$variant/repo" || exit 2
 run "source_${variant}_start" shasum -a 256 -c "$ROOT/$variant/SOURCE_SHA256.txt"
 printf 'build_%s\n' "$variant" > "$ROOT/results/stage.txt"
 printf 'CARGO_TARGET_DIR=%q CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true cargo +stable build --locked --offline --release --all-features --example framework_benchmark --example log_staging_benchmark --message-format=json\n' "$CARGO_TARGET_DIR" > "$ROOT/results/build_$variant.command"
 cargo +stable build --locked --offline --release --all-features --example framework_benchmark --example log_staging_benchmark --message-format=json > "$ROOT/results/build_$variant.jsonl" 2> "$ROOT/results/build_$variant.log"
 result=$?
 printf 'build_%s=%s\n' "$variant" "$result" >> "$ROOT/results/exits.txt"
 if [ "$result" -ne 0 ]; then failures=$((failures+1)); fi
 run "collect_$variant" node "$ROOT/collect-artifacts.mjs" "$ROOT" "$variant"
 run "source_${variant}_built" shasum -a 256 -c "$ROOT/$variant/SOURCE_SHA256.txt"
done
if [ "$failures" -eq 0 ]; then
 run measurement node "$ROOT/measure.mjs" "$ROOT"
fi
for variant in reference candidate; do
 cd "$ROOT/$variant/repo" || exit 2
 run "source_${variant}_terminal" shasum -a 256 -c "$ROOT/$variant/SOURCE_SHA256.txt"
done
printf 'failures=%s\n' "$failures" > "$ROOT/results/terminal.txt"
printf 'terminal\n' > "$ROOT/results/stage.txt"
exit "$failures"
