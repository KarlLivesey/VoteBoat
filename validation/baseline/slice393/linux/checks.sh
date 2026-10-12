#!/bin/sh
set -u
task_dir=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/divide-provider/election393
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/divide-provider/election393-build
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
cd "$task_dir/repo" || exit 1
cargo +stable --version > "$task_dir/CARGO_VERSION.txt"
rustc +stable -vV > "$task_dir/RUSTC_VERSION.txt"
"$PROTOC" --version > "$task_dir/PROTOC_VERSION.txt"
python3 "$task_dir/run.py" guard CHECKS_BEFORE || exit 1
cargo +stable fmt --all -- --check > "$task_dir/FMT.log" 2>&1
task_exit=$?
printf 'fmt=%s\n' "$task_exit" > "$task_dir/CHECK_EXITS.txt"
[ "$task_exit" -eq 0 ] || exit "$task_exit"
for task_configuration in all default core native; do
    case "$task_configuration" in
        all) set -- --all-features ;;
        default) set -- ;;
        core) set -- --no-default-features ;;
        native) set -- --no-default-features --features native ;;
    esac
    cargo +stable clippy --locked --offline --keep-going --all-targets "$@" -- -D warnings > "$task_dir/CLIPPY_$task_configuration.log" 2>&1
    task_exit=$?
    printf '%s=%s\n' "$task_configuration" "$task_exit" >> "$task_dir/CHECK_EXITS.txt"
    [ "$task_exit" -eq 0 ] || exit "$task_exit"
done
cargo +stable test --locked --offline --release --all-features --test logical_activation_nested --no-run --message-format=json > "$task_dir/BUILD.jsonl" 2> "$task_dir/BUILD.log"
task_exit=$?
printf 'build=%s\n' "$task_exit" >> "$task_dir/CHECK_EXITS.txt"
[ "$task_exit" -eq 0 ] || exit "$task_exit"
python3 "$task_dir/run.py" guard CHECKS_AFTER
