#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice428
trap 'rc=$?; printf "%s\n" "$rc" > "$job/PUSH_STATUS"' EXIT
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
export CARGO_BUILD_JOBS=2
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
printf '%s\n' 'git push origin main (enabled .githooks/pre-push)' > "$job/push.command"
printf '%s\n' RUNNING > "$job/PUSH_STAGE"
git push origin main > "$job/push.log" 2>&1
printf '%s\n' DONE > "$job/PUSH_STAGE"
