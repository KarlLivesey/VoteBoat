#!/bin/bash
set -euo pipefail
qualification_repo=$1
results=$2
platform=$3
build=$4
resume=${5:-full}
completed=0
cd "$qualification_repo"
export CARGO_TARGET_DIR="$build"
if [ "$platform" = mac ]; then
    export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:$PATH"
    export VOTEBOAT_REFERENCE_TOOLS=/tmp/voteboat-macos-CzK1if/reference-tools296
    # Existing reviewed protoc only; never invoke archive download/unzip fallback.
    export PROTOC="$VOTEBOAT_REFERENCE_TOOLS/bin/protoc"
    printf '%s  %s\n' 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" \
        | shasum -a 256 -c > "$results/$platform-protoc-guard.log"
    "$PROTOC" --version >> "$results/$platform-protoc-guard.log"
    hash=(shasum -a 256)
else
    hash=(sha256sum)
fi
guard() {
    "${hash[@]}" -c "$results/source.sha256"
    bash "$results/qualify315-inputs.sh" | LC_ALL=C sort -u > "$results/$platform-$1-files.txt"
    cmp "$results/source-files.txt" "$results/$platform-$1-files.txt"
}
terminal() {
    status=$?
    trap - EXIT
    set +e
    # Bash3 can report zero through EXIT after a failed nounset expansion.
    # Successful qualification additionally requires reaching the full loop end.
    if [ "$status" -eq 0 ] && [ "$completed" -ne 1 ]; then status=125; fi
    guard terminal > "$results/$platform-terminal-source.log" 2>&1
    guard_status=$?
    printf 'run=%s\nsource_guard=%s\n' "$status" "$guard_status" > "$results/$platform-terminal.txt"
    if [ "$status" -eq 0 ] && [ "$guard_status" -ne 0 ]; then status=$guard_status; fi
    printf 'terminal=%s\n' "$status" > "$results/$platform-stage.txt"
    exit "$status"
}
trap terminal EXIT
guard before > "$results/$platform-before-source.log" 2>&1
{
    printf 'initial_soft_nofile='; ulimit -Sn
    printf 'initial_hard_nofile='; ulimit -Hn
} > "$results/$platform-fd-adjustment.log"
ulimit -Sn 8192
{
    uname -a
    cargo +stable --version
    rustc +stable --version
    printf 'CARGO_TARGET_DIR=%s\n' "$CARGO_TARGET_DIR"
    printf 'soft_nofile='; ulimit -Sn
    printf 'hard_nofile='; ulimit -Hn
} > "$results/$platform-environment.log"
run() {
    name=$1
    shift
    if [ "$resume" = passed ] && grep -Fxq "$name=0" "$results/$platform-exits.txt"; then
        printf '%s\n' "$name" >> "$results/$platform-resume-skips.txt"
        return
    fi
    printf '%s\n' "$name" > "$results/$platform-stage.txt"
    "$@" > "$results/$platform-$name.log" 2>&1
    printf '%s=0\n' "$name" >> "$results/$platform-exits.txt"
}
if [ "$resume" = passed ]; then
    test -f "$results/$platform-exits.txt"
else
    : > "$results/$platform-exits.txt"
fi
run fmt cargo +stable fmt --all -- --check
run inventory node validation/check-inventory.mjs
run conformance node validation/check-provider-conformance.mjs
run conformance_controls node validation/check-provider-conformance.test.mjs
for profile in all default core native; do
    case "$profile" in
        all) flags=(--all-features);;
        default) flags=();;
        core) flags=(--no-default-features);;
        native) flags=(--no-default-features --features native);;
    esac
    run "$profile-clippy" cargo +stable clippy --locked --offline --keep-going --all-targets ${flags[@]+"${flags[@]}"} -- -D warnings
    run "$profile-vote-window" cargo +stable test --locked --offline ${flags[@]+"${flags[@]}"} \
        --lib raft::send_window::tests:: -- --nocapture
    case "$profile" in
        all|default)
            run "$profile-logical-native" cargo +stable test --locked --offline ${flags[@]+"${flags[@]}"} \
                --test logical_voter_native -- --nocapture
            run "$profile-logical-network" cargo +stable test --locked --offline ${flags[@]+"${flags[@]}"} \
                --test logical_voter_network --test logical_network -- --nocapture
            ;;
    esac

done
completed=1
