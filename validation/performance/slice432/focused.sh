#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice432
trap 'rc=$?; printf "%s\n" "$rc" > "$job/FOCUSED_STATUS"' EXIT
cd "$job/source"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice429/checks
export CARGO_BUILD_JOBS=2
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
mkdir -p "$job/test-tmp"
export TMPDIR="$job/test-tmp"
run() {
 local name=$1
 shift
 printf '%s\n' "$name" > "$job/FOCUSED_STAGE"
 printf '%q ' "$@" > "$job/$name.command"
 printf '\n' >> "$job/$name.command"
 set +e
 "$@" > "$job/$name.log" 2>&1
 local rc=$?
 set -e
 printf '%s\n' "$rc" > "$job/$name.status"
 if [[ $rc -ne 0 ]]; then tail -45 "$job/$name.log"; exit "$rc"; fi
}
run format-apply cargo +stable fmt --all
run focused-clone cargo +stable test --locked --offline --all-features --test provider_conformance entry_clone --message-format=json -vv -- --test-threads=1
run fmt-initial cargo +stable fmt --all -- --check
run clippy-all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run clippy-default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run clippy-core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run clippy-native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
printf 'DONE\n' > "$job/FOCUSED_STAGE"
