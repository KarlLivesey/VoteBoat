#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/upstream-guard424
cd "$job/source"
printf '%s\n' "$$" > "$job/PID"
printf 'building\n' > "$job/STAGE"
finish(){ rc=$?; printf '%s\n' "$rc" > "$job/STATUS"; exit "$rc"; }
trap finish EXIT
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
export CARGO_TARGET_DIR="$job/target-release"
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_PROFILE_RELEASE_LTO=thin
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
cargo +stable -V > "$job/cargo.txt"
rustc +stable -Vv > "$job/rustc.txt"
sha256sum -c "$job/source.sha256" > "$job/source-before.log"
printf '%s\n' 'cargo +stable build --locked --offline --release --manifest-path benchmarks/minimal/Cargo.toml --bin bench --message-format=json -vv' > "$job/build.command"
cargo +stable build --locked --offline --release --manifest-path benchmarks/minimal/Cargo.toml --bin bench --message-format=json -vv > "$job/build.jsonl" 2> "$job/build.stderr"
sha256sum -c "$job/source.sha256" > "$job/source-after.log"
printf 'BUILT\n' > "$job/STAGE"
