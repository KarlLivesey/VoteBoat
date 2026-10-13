#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice430
trap 'rc=$?; printf "%s\n" "$rc" > "$job/RESUME_STATUS"' EXIT
printf '%s\n' "$$" > "$job/PID"
cd "$job/source"
export CARGO_TARGET_DIR="$job/matched-checks"
export CARGO_BUILD_JOBS=2
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
run() {
 local name=$1
 shift
 printf '%s\n' "$name" > "$job/CHECK_STAGE"
 printf '%q ' "$@" > "$job/$name.command"
 printf '\n' >> "$job/$name.command"
 set +e
 "$@" > "$job/$name.log" 2>&1
 local rc=$?
 set -e
 printf '%s\n' "$rc" > "$job/$name.status"
 if [[ $rc -ne 0 ]]; then tail -50 "$job/$name.log"; exit "$rc"; fi
}
run source-before sha256sum -c "$job/source.sha256"
run focused-replies cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline replies:: -- --test-threads=1 --nocapture
run matched-fmt cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml --all -- --check
run matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run source-after sha256sum -c "$job/source.sha256"
printf 'DONE\n' > "$job/CHECK_STAGE"
