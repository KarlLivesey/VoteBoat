#!/bin/sh
set -eu
task_dir=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/divide-provider/cooperative396
task_profile=$1
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/divide-provider/cooperative396-build
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
cd "$task_dir/repo"
case "$task_profile" in
 ALL) set -- --all-features ;;
 DEFAULT) set -- ;;
 *) exit 2 ;;
esac
if [ "$task_profile" = DEFAULT ]; then
 test -f "$task_dir/ALL_RESULT.json"
 python3 "$task_dir/run.py" guard BEFORE_DEFAULT_BUILD_PRIOR_ALL ALL > "$task_dir/DEFAULT_PRIOR_GUARD.log"
fi
python3 "$task_dir/run.py" guard "BEFORE_BUILD_$task_profile"
cargo +stable test --locked --offline --release "$@" --test startup --no-run --message-format=json > "$task_dir/BUILD_$task_profile.jsonl" 2> "$task_dir/BUILD_$task_profile.log"
printf 'build_%s=0\n' "$task_profile" >> "$task_dir/BUILD_EXITS.txt"
python3 "$task_dir/run.py" guard "AFTER_BUILD_$task_profile" "$task_profile"
if [ "$task_profile" = DEFAULT ]; then
 python3 "$task_dir/run.py" guard AFTER_DEFAULT_BUILD_PRIOR_ALL ALL >> "$task_dir/DEFAULT_PRIOR_GUARD.log"
fi
