#!/usr/bin/env bash
set -uo pipefail
job=target/p9/slice475
export CARGO_TARGET_DIR="$PWD/$job/checks" CARGO_BUILD_JOBS=2
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
printf '%s\n' "$$" > "$job/PID"
printf 'focused_example\n' > "$job/STAGE"
"$PROTOC" --version > "$job/protoc.txt"
rustc +stable -Vv > "$job/rustc.txt"
printf '%s\n' 'cargo +stable test --locked --offline --all-features --example native_benchmark offered_capacity -- --nocapture --test-threads=1' > "$job/focused.command"
cargo +stable test --locked --offline --all-features --example native_benchmark offered_capacity -- --nocapture --test-threads=1 > "$job/focused.stdout" 2> "$job/focused.stderr"
rc=$?
printf '%s\n' "$rc" > "$job/focused.exit"
printf 'FOCUSED_EXIT=%s\n' "$rc"
printf 'DONE\n' > "$job/STAGE"
exit "$rc"
