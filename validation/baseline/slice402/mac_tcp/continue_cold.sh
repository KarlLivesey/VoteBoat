#!/bin/bash
set -uo pipefail
QUAL_ROOT=$1
cd "$QUAL_ROOT/repo" || exit 2
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_TARGET_DIR="$QUAL_ROOT/build"
export CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export TMPDIR="$QUAL_ROOT/test-tmp"
ulimit -n 4096
QUAL_FAILURES=0
run() {
    QUAL_LABEL=$1
    shift
    printf '%s\n' "$QUAL_LABEL" > "$QUAL_ROOT/results/stage.txt"
    printf '%q ' "$@" > "$QUAL_ROOT/results/$QUAL_LABEL.command"
    printf '\n' >> "$QUAL_ROOT/results/$QUAL_LABEL.command"
    "$@" > "$QUAL_ROOT/results/$QUAL_LABEL.log" 2>&1
    QUAL_RC=$?
    printf '%s=%s\n' "$QUAL_LABEL" "$QUAL_RC" >> "$QUAL_ROOT/results/exits.txt"
    if [ "$QUAL_RC" -ne 0 ]; then QUAL_FAILURES=$((QUAL_FAILURES+1)); fi
}
capture() {
    while IFS= read -r QUAL_ARTIFACT; do
        shasum -a 256 "$QUAL_ARTIFACT" || return 2
    done < "$QUAL_ROOT/results/artifact_paths.txt"
}
run native_hundred_selected_count /usr/bin/grep -q '^running 1 test$' "$QUAL_ROOT/results/native_hundred.log"
if [ "$QUAL_RC" -ne 0 ]; then exit 2; fi
run native_hundred_passed /usr/bin/grep -q '^test result: ok. 1 passed; 0 failed;' "$QUAL_ROOT/results/native_hundred.log"
if [ "$QUAL_RC" -ne 0 ]; then exit 2; fi
run continuation_source_start shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
if [ "$QUAL_RC" -ne 0 ]; then exit 2; fi
capture > "$QUAL_ROOT/results/binary_continuation_start.log" || exit 2
run continuation_binary_unchanged cmp "$QUAL_ROOT/results/binary_start.log" "$QUAL_ROOT/results/binary_continuation_start.log"
if [ "$QUAL_RC" -ne 0 ]; then exit 2; fi
case_one() {
    QUAL_CASE=$1
    QUAL_TEST=$2
    if [ -f "$QUAL_ROOT/STOP_BEFORE_NEXT_CASE" ]; then
        printf 'stopped_before_%s\n' "$QUAL_CASE" > "$QUAL_ROOT/results/terminal.txt"
        exit 3
    fi
    run "$QUAL_CASE" cargo +stable test --locked --offline --release --all-features --test logical_nested_native "$QUAL_TEST" -- --exact --nocapture --test-threads=1
    run "${QUAL_CASE}_selected_count" /usr/bin/grep -q '^running 1 test$' "$QUAL_ROOT/results/$QUAL_CASE.log"
    if [ "$QUAL_RC" -ne 0 ]; then exit 2; fi
    capture > "$QUAL_ROOT/results/binary_after_$QUAL_CASE.log" || exit 2
    run "${QUAL_CASE}_binary_unchanged" cmp "$QUAL_ROOT/results/binary_start.log" "$QUAL_ROOT/results/binary_after_$QUAL_CASE.log"
    if [ "$QUAL_RC" -ne 0 ]; then exit 2; fi
    run "${QUAL_CASE}_source_unchanged" shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
    if [ "$QUAL_RC" -ne 0 ]; then exit 2; fi
}
case_one cold_tcp cold_recovery::tcp_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores
case_one cold_quic cold_recovery::quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores
capture > "$QUAL_ROOT/results/binary_terminal.log" || exit 2
run binary_terminal_unchanged cmp "$QUAL_ROOT/results/binary_start.log" "$QUAL_ROOT/results/binary_terminal.log"
run source_terminal shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
printf 'failures=%s\n' "$QUAL_FAILURES" > "$QUAL_ROOT/results/terminal.txt"
printf 'terminal\n' > "$QUAL_ROOT/results/stage.txt"
test "$QUAL_FAILURES" -eq 0
