#!/bin/bash
set -u
root=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice426
cd "$root/source" || exit 1
export CARGO_TARGET_DIR="$root/checks"
run() {
    local name=$1
    shift
    printf '%s\n' "$name" > "$root/CHECK_STAGE"
    "$@" > "$root/$name.log" 2>&1
    local code=$?
    printf '%s\n' "$code" > "$root/$name.status"
    if [ "$code" -ne 0 ]; then tail -28 "$root/$name.log"; exit "$code"; fi
}
run affected-final-all cargo +stable test --locked --offline --all-features --test provider_conformance --test log_store --test snapshot
run affected-final-core cargo +stable test --locked --offline --no-default-features --test provider_conformance --test log_store --test snapshot
run fmt-final cargo +stable fmt --all -- --check
run clippy-final-all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run clippy-final-default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run clippy-final-core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run clippy-final-native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
export CARGO_TARGET_DIR="$root/matched-checks"
run matched-final-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline
run matched-final-fmt cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml --all -- --check
run matched-final-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'DONE\n' > "$root/CHECK_STAGE"
printf '0\n' > "$root/CHECK_STATUS"
