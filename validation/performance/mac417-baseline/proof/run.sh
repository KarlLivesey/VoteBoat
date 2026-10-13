#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-mac417-baseline-355cfa6
cargo=/Users/karllivesey/.cargo/bin/cargo
export PATH=/Users/karllivesey/.cargo/bin:/usr/bin:/bin:/usr/sbin:/sbin
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
export CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_PROFILE_RELEASE_LTO=thin
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
export CARGO_TARGET_DIR="$job/target-matched-release"
mkdir "$job/logs"
printf '%s\n' "$$" > "$job/PID"
printf '%s\n' running > "$job/STATUS"
stage(){ printf '%s\n' "$1" > "$job/STAGE"; }
finish(){
 rc=$?
 (cd "$job/source" && shasum -a 256 -c "$job/source.sha256") > "$job/logs/source-final.log" 2>&1 || rc=102
 printf '%s\n' "$rc" > "$job/STATUS"
 exit "$rc"
}
trap finish EXIT
stage source_guard
(cd "$job/source" && shasum -a 256 -c "$job/source.sha256") > "$job/logs/source-before.log"
/Users/karllivesey/.cargo/bin/rustc +stable -Vv > "$job/logs/rustc.txt"
"$cargo" +stable -V > "$job/logs/cargo.txt"
printf '%s\n' 'opt3 debug_assertions=true overflow_checks=true lto=thin codegen_units=1; no native/tests/timing' > "$job/logs/profile.txt"
stage matched_release
printf '%q ' "$cargo" +stable build --locked --offline --release --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/build.command"
"$cargo" +stable build --locked --offline --release --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/build.jsonl" 2> "$job/logs/build.stderr"
stage compiler_artifact_guards
/usr/bin/python3 "$job/collect.py"
stage DONE
