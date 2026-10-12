#!/bin/bash
set -uo pipefail
RET383_ROOT=$1
mkdir -p "$RET383_ROOT/results" "$RET383_ROOT/test-tmp"
cd "$RET383_ROOT/repo" || exit 2
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_TARGET_DIR="$RET383_ROOT/build"
export CARGO_BUILD_JOBS=2
export TMPDIR="$RET383_ROOT/test-tmp"
ulimit -n 4096
RET383_FAILURES=0
run() {
    RET383_LABEL=$1
    shift
    printf '%s\n' "$RET383_LABEL" > "$RET383_ROOT/results/stage.txt"
    printf '%q ' "$@" > "$RET383_ROOT/results/$RET383_LABEL.command"
    printf '\n' >> "$RET383_ROOT/results/$RET383_LABEL.command"
    "$@" > "$RET383_ROOT/results/$RET383_LABEL.log" 2>&1
    RET383_RC=$?
    printf '%s=%s\n' "$RET383_LABEL" "$RET383_RC" >> "$RET383_ROOT/results/exits.txt"
    if [ "$RET383_RC" -ne 0 ]; then RET383_FAILURES=$((RET383_FAILURES + 1)); fi
}
run platform bash -c 'uname -sm; rustc +stable --version; cargo +stable --version'
run source_start shasum -a 256 -c "$RET383_ROOT/MAC_RETIRED383_SOURCE_SHA256.txt"
run prior_binary_clone shasum -a 256 -c "$RET383_ROOT/PRIOR_BINARY_SHA256.txt"
run protoc bash -c 'printf "%s  %s\n" 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" | shasum -a 256 -c && "$PROTOC" --version'
if [ "$RET383_FAILURES" -ne 0 ]; then printf 'initial_guard_failed\n' > "$RET383_ROOT/results/terminal.txt"; exit 2; fi
run fmt cargo +stable fmt --all -- --check
run clippy_all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run clippy_default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run clippy_core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run clippy_native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
run build cargo +stable test --locked --offline --all-features --test logical_activation_nested --no-run
if [ "$RET383_FAILURES" -ne 0 ]; then printf 'build_failed\n' > "$RET383_ROOT/results/terminal.txt"; exit 2; fi
capture_binary() {
    find "$CARGO_TARGET_DIR/debug/deps" -type f -name 'logical_activation_nested-*' ! -name '*.d' ! -name '*.rmeta' -exec shasum -a 256 {} + | sort
}
capture_binary > "$RET383_ROOT/results/binary_start.log" || exit 2
test -s "$RET383_ROOT/results/binary_start.log" || exit 2
case_one() {
    RET383_CASE=$1
    RET383_TEST=$2
    if [ -f "$RET383_ROOT/STOP_BEFORE_NEXT_CASE" ]; then printf 'stopped_before_%s\n' "$RET383_CASE" > "$RET383_ROOT/results/terminal.txt"; exit 3; fi
    run "$RET383_CASE" cargo +stable test --locked --offline --all-features --test logical_activation_nested "$RET383_TEST" -- --exact --nocapture
    run "${RET383_CASE}_selected_one" /usr/bin/grep -q '^running 1 test$' "$RET383_ROOT/results/$RET383_CASE.log"
    capture_binary > "$RET383_ROOT/results/binary_after_$RET383_CASE.log" || exit 2
    run "${RET383_CASE}_binary_unchanged" cmp "$RET383_ROOT/results/binary_start.log" "$RET383_ROOT/results/binary_after_$RET383_CASE.log"
    if [ "$RET383_RC" -ne 0 ]; then
        run source_terminal shasum -a 256 -c "$RET383_ROOT/MAC_RETIRED383_SOURCE_SHA256.txt"
        printf 'binary_guard_failed_after_%s\n' "$RET383_CASE" > "$RET383_ROOT/results/terminal.txt"
        exit 2
    fi
}
case_one repeated_tcp history::tcp_fifteen_native_owners_two_original_logical_replacements
case_one repeated_quic history::quic_fifteen_native_owners_two_original_logical_replacements
case_one imported_tcp history::tcp_original_import_lost_image_target_stores_cold_claim_then_activation
case_one imported_quic history::quic_original_import_lost_image_target_stores_cold_claim_then_activation
case_one joint_tcp cold_joint::tcp_all_twelve_stores_joint_and_parent_intent_cold_reconstruct_then_final
case_one joint_quic cold_joint::quic_all_twelve_stores_joint_and_parent_intent_cold_reconstruct_then_final
case_one committed_unknown_tcp history::tcp_original_unknown_import_target_cold_resolves_then_activation
case_one committed_unknown_quic history::quic_original_unknown_import_target_cold_resolves_then_activation
case_one uncommitted_unknown_tcp history::tcp_original_uncommitted_unknown_import_wal_cold_then_real_activation
case_one uncommitted_unknown_quic history::quic_original_uncommitted_unknown_import_wal_cold_then_real_activation
capture_binary > "$RET383_ROOT/results/binary_terminal.log" || exit 2
run binary_terminal_unchanged cmp "$RET383_ROOT/results/binary_start.log" "$RET383_ROOT/results/binary_terminal.log"
run source_terminal shasum -a 256 -c "$RET383_ROOT/MAC_RETIRED383_SOURCE_SHA256.txt"
printf 'failures=%s\n' "$RET383_FAILURES" > "$RET383_ROOT/results/terminal.txt"
printf 'terminal\n' > "$RET383_ROOT/results/stage.txt"
test "$RET383_FAILURES" -eq 0
