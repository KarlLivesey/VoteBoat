#!/usr/bin/env bash
set -euo pipefail
root=/home/karllivesey/Documents/ChatGPT/VoteBoat
job="$root/target/p9/matched-artifacts415/linux"
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_PROFILE_RELEASE_LTO=thin
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
stage(){ printf '%s\n' "$1" > "$job/STAGE"; }
(cd "$job/source" && sha256sum -c "$job/source.sha256") > "$job/logs/source-before.log"
cargo +stable -V > "$job/logs/cargo.txt"
rustc +stable -Vv > "$job/logs/rustc.txt"
uname -a > "$job/logs/host.txt"
stage release_harness
export CARGO_TARGET_DIR="$job/target-harness"
cargo +stable build --locked --offline --release --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/harness-release.jsonl" 2> "$job/logs/harness-release.stderr"
stage release_native
unset CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS
export CARGO_TARGET_DIR="$job/target-native"
cargo +stable build --locked --offline --release --all-features --manifest-path "$job/source/Cargo.toml" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.jsonl" 2> "$job/logs/native-release.stderr"
stage guards
(cd "$job/source" && sha256sum -c "$job/source.sha256") > "$job/logs/source-after.log"
sha256sum "$job/target-harness/release/voteboat-matched-framework" "$job/target-native/release/examples/native_benchmark" > "$job/logs/artifacts.sha256"
stage DONE
