#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice431
trap 'rc=$?; printf "%s\n" "$rc" > "$job/FOCUSED_TIMEOUT_STATUS"' EXIT
cd "$job/source"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice429/checks CARGO_BUILD_JOBS=2 TMPDIR="$job/test-tmp" RUST_BACKTRACE=1
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
for name in hundred_compacted_groups_catch_up_over_workers_and_authenticated_transport native_shared_peer_quota_pressure_reconnect_restart_and_retry; do
 args=(cargo +stable test --locked --offline --all-features --test effect_owner "native::$name" -- --exact --nocapture --test-threads=1)
 printf '%s\n' "$name" > "$job/FOCUSED_TIMEOUT_STAGE"
 printf '%q ' "${args[@]}" > "$job/focused-$name.command"; printf '\n' >> "$job/focused-$name.command"
 set +e
 "${args[@]}" > "$job/focused-$name.log" 2>&1
 rc=$?
 set -e
 printf '%s\n' "$rc" > "$job/focused-$name.status"
 if [[ "$rc" -ne 0 ]]; then tail -40 "$job/focused-$name.log"; exit "$rc"; fi
 done
printf '%s\n' DONE > "$job/FOCUSED_TIMEOUT_STAGE"
