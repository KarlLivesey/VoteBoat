#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-mac-upstream-guard424
export PATH=/Users/karllivesey/.cargo/bin:/usr/bin:/bin:/usr/sbin:/sbin
export CARGO_BUILD_JOBS=2
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3 CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
export CARGO_TARGET_DIR="$job/target-release"
ulimit -n 4096
mkdir "$job/logs"
printf '%s\n' "$$" > "$job/PID"
printf 'prepare\n' > "$job/STAGE"
finish(){ rc=$?;printf '%s\n' "$rc" > "$job/STATUS";exit "$rc"; }
trap finish EXIT
/usr/bin/python3 "$job/prepare.py" > "$job/logs/prepare.log" 2>&1
cd "$job/source"
shasum -a256 -c "$job/source.sha256" > "$job/logs/source-before.log"
/Users/karllivesey/.cargo/bin/cargo +stable -V > "$job/logs/cargo.txt"
/Users/karllivesey/.cargo/bin/rustc +stable -Vv > "$job/logs/rustc.txt"
printf 'release\n' > "$job/STAGE"
printf '%s\n' 'cargo +stable build --locked --offline --release --manifest-path benchmarks/minimal/Cargo.toml --bin bench --message-format=json -vv' > "$job/logs/build.command"
/Users/karllivesey/.cargo/bin/cargo +stable build --locked --offline --release --manifest-path benchmarks/minimal/Cargo.toml --bin bench --message-format=json -vv > "$job/logs/build.jsonl" 2> "$job/logs/build.stderr"
printf 'collect_and_functional\n' > "$job/STAGE"
/usr/bin/python3 "$job/collect.py"
shasum -a256 -c "$job/source.sha256" > "$job/logs/source-after.log"
printf 'DONE\n' > "$job/STAGE"
