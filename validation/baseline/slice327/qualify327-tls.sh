#!/bin/bash
set -euo pipefail
repo=$1
results=$2
platform=$3
build=$4
completed=0
cd "$repo"
export CARGO_TARGET_DIR="$build"
if [ "$platform" = mac ]; then
    export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:$PATH"
    hash=(shasum -a 256)
else
    hash=(sha256sum)
fi
guard() {
    "${hash[@]}" -c "$results/source.sha256"
    bash "$results/qualify315-inputs.sh" | LC_ALL=C sort -u > "$results/$platform-tls-$1-files.txt"
    cmp "$results/source-files.txt" "$results/$platform-tls-$1-files.txt"
}
terminal() {
    status=$?
    trap - EXIT
    set +e
    if [ "$status" -eq 0 ] && [ "$completed" -ne 1 ]; then status=125; fi
    guard terminal > "$results/$platform-tls-terminal-source.log" 2>&1
    checked=$?
    printf 'run=%s\nsource_guard=%s\n' "$status" "$checked" > "$results/$platform-tls-terminal.txt"
    if [ "$status" -eq 0 ] && [ "$checked" -ne 0 ]; then status=$checked; fi
    printf 'terminal=%s\n' "$status" > "$results/$platform-tls-stage.txt"
    exit "$status"
}
trap terminal EXIT
test "$(cat "$results/$platform-terminal.txt")" = "$(printf 'run=0\nsource_guard=0')"
guard before > "$results/$platform-tls-before-source.log" 2>&1
{
    printf 'initial_soft_nofile='; ulimit -Sn
    printf 'initial_hard_nofile='; ulimit -Hn
} > "$results/$platform-tls-fd-adjustment.log"
ulimit -Sn 8192
{
    uname -a
    cargo +stable --version
    rustc +stable --version
    printf 'CARGO_TARGET_DIR=%s\n' "$CARGO_TARGET_DIR"
    printf 'soft_nofile='; ulimit -Sn
    printf 'hard_nofile='; ulimit -Hn
} > "$results/$platform-tls-environment.log"
: > "$results/$platform-tls-exits.txt"
run() {
    name=$1
    shift
    printf '%s\n' "$name" > "$results/$platform-tls-stage.txt"
    "$@" > "$results/$platform-$name.log" 2>&1
    printf '%s=0\n' "$name" >> "$results/$platform-tls-exits.txt"
}
run tls-snapshot-ack cargo +stable test --locked --offline --no-default-features --features tls \
    --test logical_voter_native authority::snapshot_ack:: -- --nocapture
run tls-parent-driver cargo +stable test --locked --offline --no-default-features --features tls \
    --test logical_activation_driver -- --nocapture
completed=1
