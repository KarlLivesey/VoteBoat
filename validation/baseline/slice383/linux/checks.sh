#!/bin/sh
set -u
task_dir=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/divide-provider/checkpoint383
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/divide-provider/checkpoint383-build
cd "$task_dir/repo" || exit 1
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
cargo +stable test --locked --offline --all-features --test logical_activation_nested --no-run --message-format=json > "$task_dir/BUILD.jsonl" 2> "$task_dir/BUILD.log"
task_exit=$?
printf 'build=%s\n' "$task_exit" >> "$task_dir/CHECK_EXITS.txt"
[ "$task_exit" -eq 0 ] || exit "$task_exit"
python3 "$task_dir/run.py" guard CHECKS_AFTER
