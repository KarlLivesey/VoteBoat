#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice431
trap 'rc=$?; printf "%s\n" "$rc" > "$job/RESUME_STATUS"' EXIT
printf '%s\n' "$$" > "$job/PID"
cd "$job/source"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice429/checks
export CARGO_BUILD_JOBS=2
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
mkdir -p "$job/test-tmp"
export TMPDIR="$job/test-tmp"
run() {
 local name=$1
 shift
 printf '%s\n' "$name" > "$job/RESUME_STAGE"
 printf '%q ' "$@" > "$job/$name.command"
 printf '\n' >> "$job/$name.command"
 set +e
 "$@" > "$job/$name.log" 2>&1
 local rc=$?
 set -e
 printf '%s\n' "$rc" > "$job/$name.status"
 if [[ $rc -ne 0 ]]; then tail -40 "$job/$name.log"; exit "$rc"; fi
}
run remaining-all cargo +stable test --locked --offline --all-features --test log_store --test provider_conformance --test snapshot --test raft -- --test-threads=1
run affected-core cargo +stable test --locked --offline --no-default-features --lib --test log_store --test provider_conformance --test snapshot --test raft --test effect_owner -- --test-threads=1
run actual-services cargo +stable test --locked --offline --all-features --test counter_service three_ -- --test-threads=1
run fmt-final cargo +stable fmt --all -- --check
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice429/matched-checks
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1
run matched-fmt cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml --all -- --check
run matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run source-after sha256sum -c "$job/source.sha256"
printf 'DONE\n' > "$job/RESUME_STAGE"
