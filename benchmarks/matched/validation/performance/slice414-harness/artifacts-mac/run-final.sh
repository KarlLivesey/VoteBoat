#!/bin/bash
set -euo pipefail
job=/tmp/voteboat-matched414-mac
cargo=/Users/karllivesey/.cargo/bin/cargo
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS
stage(){ printf '%s\n' "$1" > "$job/STAGE"; }
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
mkdir -p "$job/logs"
stage preflight
/Users/karllivesey/.cargo/bin/rustc +stable -Vv > "$job/logs/rustc.txt"
"$cargo" +stable -V > "$job/logs/cargo.txt"
uname -a > "$job/logs/host.txt"
sysctl -n machdep.cpu.brand_string >> "$job/logs/host.txt"
command -v protoc > "$job/logs/protoc.txt"
printf '%s  %s\n' 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" | shasum -a 256 -c >> "$job/logs/protoc.txt"
"$PROTOC" --version >> "$job/logs/protoc.txt"
(cd "$job/source-final" && shasum -a 256 -c "$job/source-final.sha256") > "$job/logs/source-before.log"
(cd "$job/native-source" && shasum -a 256 -c "$job/native-source306b26b.sha256") > "$job/logs/native-source-before.log"
export CARGO_TARGET_DIR="$job/target-checks"
stage matched_fmt
"$cargo" +stable fmt --manifest-path "$job/source-final/benchmarks/matched/Cargo.toml" -- --check > "$job/logs/matched-fmt.log" 2>&1
stage matched_tests
"$cargo" +stable test --locked --offline --manifest-path "$job/source-final/benchmarks/matched/Cargo.toml" -- --test-threads=1 > "$job/logs/matched-tests.log" 2>&1
stage matched_strict
"$cargo" +stable clippy --locked --offline --all-targets --manifest-path "$job/source-final/benchmarks/matched/Cargo.toml" -- -D warnings > "$job/logs/matched-clippy.log" 2>&1
stage matched_cli_originals
"$cargo" +stable build --locked --offline --manifest-path "$job/source-final/benchmarks/matched/Cargo.toml" > "$job/logs/matched-cli-build.log" 2>&1
for profile in upstream-default common64; do
 for engine in voteboat openraft; do
  "$CARGO_TARGET_DIR/debug/voteboat-matched-framework" --engine "$engine" --clients 4096 --batch 4 --operations 16384 --warmup 1024 --maintenance default-retention --envelopes "$profile" > "$job/logs/cli-$engine-$profile.json" 2> "$job/logs/cli-$engine-$profile.stderr"
 done
done
export CARGO_TARGET_DIR="$job/target-root-checks"
manifest="$job/native-source/Cargo.toml"
stage root_fmt
"$cargo" +stable fmt --manifest-path "$manifest" --all -- --check > "$job/logs/root-fmt.log" 2>&1
stage root_lib_all
"$cargo" +stable test --locked --offline --manifest-path "$manifest" --lib --all-features -- --test-threads=1 > "$job/logs/root-lib-all.log" 2>&1
stage root_lib_core
"$cargo" +stable test --locked --offline --manifest-path "$manifest" --lib --no-default-features -- --test-threads=1 > "$job/logs/root-lib-core.log" 2>&1
stage root_model_io_large_batch
"$cargo" +stable test --locked --offline --manifest-path "$manifest" --test log_store --all-features batch_limits::large_batch_and_commit_publication_cuts_recover_without_premature_original_results -- --exact > "$job/logs/root-model-io.log" 2>&1
stage root_strict_default
"$cargo" +stable clippy --locked --offline --manifest-path "$manifest" --keep-going --all-targets -- -D warnings > "$job/logs/root-clippy-default.log" 2>&1
stage root_strict_all
"$cargo" +stable clippy --locked --offline --manifest-path "$manifest" --keep-going --all-targets --all-features -- -D warnings > "$job/logs/root-clippy-all.log" 2>&1
stage root_strict_core
"$cargo" +stable clippy --locked --offline --manifest-path "$manifest" --keep-going --all-targets --no-default-features -- -D warnings > "$job/logs/root-clippy-core.log" 2>&1
stage root_strict_native
"$cargo" +stable clippy --locked --offline --manifest-path "$manifest" --keep-going --all-targets --no-default-features --features native -- -D warnings > "$job/logs/root-clippy-native.log" 2>&1
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_PROFILE_RELEASE_LTO=thin
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
stage release_harness
export CARGO_TARGET_DIR="$job/target-harness"
"$cargo" +stable build --locked --offline --release --manifest-path "$job/source-final/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/harness-release.jsonl" 2> "$job/logs/harness-release.stderr"
stage release_native
export CARGO_TARGET_DIR="$job/target-native"
"$cargo" +stable build --locked --offline --release --all-features --manifest-path "$manifest" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.jsonl" 2> "$job/logs/native-release.stderr"
stage guards
(cd "$job/source-final" && shasum -a 256 -c "$job/source-final.sha256") > "$job/logs/source-after.log"
(cd "$job/native-source" && shasum -a 256 -c "$job/native-source306b26b.sha256") > "$job/logs/native-source-after.log"
shasum -a 256 "$job/source-final/benchmarks/matched/Cargo.lock" "$job/native-source/Cargo.lock" > "$job/logs/locks.sha256"
shasum -a 256 "$job/target-harness/release/voteboat-matched-framework" "$job/target-native/release/examples/native_benchmark" > "$job/logs/artifacts.sha256"
stage DONE
