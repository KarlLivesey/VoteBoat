#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice428
trap 'rc=$?; printf "%s\n" "$rc" > "$job/DRAIN_STATUS"' EXIT
cd "$job/source"
export CARGO_TARGET_DIR="$job/checks" CARGO_BUILD_JOBS=2 TMPDIR="$job/test-tmp" RUST_BACKTRACE=1
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
command=(cargo +stable test --locked --offline --all-features --test effect_owner native::hundred_compacted_groups_catch_up_over_workers_and_authenticated_transport -- --exact --nocapture --test-threads=1)
printf '%q ' "${command[@]}" > "$job/drain-focus.command"
printf '\n' >> "$job/drain-focus.command"
"${command[@]}" > "$job/drain-focus.log" 2>&1
