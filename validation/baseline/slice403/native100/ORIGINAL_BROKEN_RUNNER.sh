#!/bin/bash
set -uo pipefail
QUAL_ROOT=$1
mkdir -p "$QUAL_ROOT/results" "$QUAL_ROOT/test-tmp"
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
run platform bash -c 'uname -sm; rustc +stable --version; cargo +stable --version'
run profile bash -c 'printf "release_debug_assertions=%s\nrelease_overflow_checks=%s\n" "$CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS" "$CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS"'
run source_start shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
run prior_binary_clone shasum -a 256 -c "$QUAL_ROOT/PRIOR_BINARY_SHA256.txt"
run protoc bash -c 'printf "%s  %s\n" 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" | shasum -a 256 -c && "$PROTOC" --version'
if [ "$QUAL_FAILURES" -ne 0 ]; then printf 'initial_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
run fmt cargo +stable fmt --all -- --check
run clippy_all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run clippy_default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run clippy_core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run clippy_native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
run build cargo +stable test --locked --offline --release --all-features --test logical_nested_native --no-run --verbose --message-format=json-render-diagnostics
if [ "$QUAL_FAILURES" -ne 0 ]; then printf 'build_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
run artifact_profile python3 "$QUAL_ROOT/artifact_profile.py" "$QUAL_ROOT"
if [ "$QUAL_RC" -ne 0 ]; then printf 'artifact_profile_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
run source_after_build shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
if [ "$QUAL_RC" -ne 0 ]; then printf 'source_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
capture() {
    while IFS= read -r QUAL_ARTIFACT; do shasum -a 256 "$QUAL_ARTIFACT" || return 2; done < "$QUAL_ROOT/results/artifact_paths.txt"
}
capture > "$QUAL_ROOT/results/binary_start.log" || exit 2
test -s "$QUAL_ROOT/results/binary_start.log" || exit 2
case_one() {
    QUAL_CASE=$1
    QUAL_TARGET=$2
    QUAL_TEST=$3
    QUAL_EXPECTED=$4
    if [ -f "$QUAL_ROOT/STOP_BEFORE_NEXT_CASE" ]; then printf 'stopped_before_%s\n' "$QUAL_CASE" > "$QUAL_ROOT/results/terminal.txt"; exit 3; fi
    if [ "$QUAL_EXPECTED" -eq 1 ]; then
        run "$QUAL_CASE" cargo +stable test --locked --offline --release --all-features --test "$QUAL_TARGET" "$QUAL_TEST" -- --exact --nocapture --test-threads=1
    else
        run "$QUAL_CASE" cargo +stable test --locked --offline --release --all-features --test "$QUAL_TARGET" "$QUAL_TEST" -- --nocapture --test-threads=1
    fi
    run "${QUAL_CASE}_selected_count" /usr/bin/grep -q "^running $QUAL_EXPECTED tests\{0,1\}$" "$QUAL_ROOT/results/$QUAL_CASE.log"
    if [ "$QUAL_RC" -ne 0 ]; then printf 'selection_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
    capture > "$QUAL_ROOT/results/binary_after_$QUAL_CASE.log" || exit 2
    run "${QUAL_CASE}_binary_unchanged" cmp "$QUAL_ROOT/results/binary_start.log" "$QUAL_ROOT/results/binary_after_$QUAL_CASE.log"
    if [ "$QUAL_RC" -ne 0 ]; then printf 'binary_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
    run "${QUAL_CASE}_source_unchanged" shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
    if [ "$QUAL_RC" -ne 0 ]; then printf 'source_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
}
run native_hundred cargo +stable test --locked --offline --no-default-features --features native --test effect_owner native::hundred_compacted_groups_catch_up_over_workers_and_authenticated_transport -- --exact --nocapture --test-threads=1
run native_hundred_selected_count /usr/bin/grep -q '^running 1 test +stable test --locked --offline --release --all-features --test logical_nested_native --no-run --verbose --message-format=json-render-diagnostics
if [ "$QUAL_FAILURES" -ne 0 ]; then printf 'build_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
run artifact_profile python3 "$QUAL_ROOT/artifact_profile.py" "$QUAL_ROOT"
if [ "$QUAL_RC" -ne 0 ]; then printf 'artifact_profile_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
run source_after_build shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
if [ "$QUAL_RC" -ne 0 ]; then printf 'source_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
capture() {
    while IFS= read -r QUAL_ARTIFACT; do shasum -a 256 "$QUAL_ARTIFACT" || return 2; done < "$QUAL_ROOT/results/artifact_paths.txt"
}
capture > "$QUAL_ROOT/results/binary_start.log" || exit 2
test -s "$QUAL_ROOT/results/binary_start.log" || exit 2
case_one() {
    QUAL_CASE=$1
    QUAL_TARGET=$2
    QUAL_TEST=$3
    QUAL_EXPECTED=$4
    if [ -f "$QUAL_ROOT/STOP_BEFORE_NEXT_CASE" ]; then printf 'stopped_before_%s\n' "$QUAL_CASE" > "$QUAL_ROOT/results/terminal.txt"; exit 3; fi
    if [ "$QUAL_EXPECTED" -eq 1 ]; then
        run "$QUAL_CASE" cargo +stable test --locked --offline --release --all-features --test "$QUAL_TARGET" "$QUAL_TEST" -- --exact --nocapture --test-threads=1
    else
        run "$QUAL_CASE" cargo +stable test --locked --offline --release --all-features --test "$QUAL_TARGET" "$QUAL_TEST" -- --nocapture --test-threads=1
    fi
    run "${QUAL_CASE}_selected_count" /usr/bin/grep -q "^running $QUAL_EXPECTED tests\{0,1\}$" "$QUAL_ROOT/results/$QUAL_CASE.log"
    if [ "$QUAL_RC" -ne 0 ]; then printf 'selection_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
    capture > "$QUAL_ROOT/results/binary_after_$QUAL_CASE.log" || exit 2
    run "${QUAL_CASE}_binary_unchanged" cmp "$QUAL_ROOT/results/binary_start.log" "$QUAL_ROOT/results/binary_after_$QUAL_CASE.log"
    if [ "$QUAL_RC" -ne 0 ]; then printf 'binary_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
    run "${QUAL_CASE}_source_unchanged" shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
    if [ "$QUAL_RC" -ne 0 ]; then printf 'source_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
}
case_one cold_tcp logical_nested_native cold_recovery::tcp_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores 1
case_one cold_quic logical_nested_native cold_recovery::quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores 1
capture > "$QUAL_ROOT/results/binary_terminal.log" || exit 2
run binary_terminal_unchanged cmp "$QUAL_ROOT/results/binary_start.log" "$QUAL_ROOT/results/binary_terminal.log"
run source_terminal shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
printf 'failures=%s\n' "$QUAL_FAILURES" > "$QUAL_ROOT/results/terminal.txt"
printf 'terminal\n' > "$QUAL_ROOT/results/stage.txt"
test "$QUAL_FAILURES" -eq 0
 "$QUAL_ROOT/results/native_hundred.log"
if [ "$QUAL_RC" -ne 0 ]; then printf 'selection_guard_failed\n' > "$QUAL_ROOT/results/terminal.txt"; exit 2; fi
run native_hundred_source_unchanged shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
case_one cold_tcp logical_nested_native cold_recovery::tcp_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores 1
case_one cold_quic logical_nested_native cold_recovery::quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores 1
capture > "$QUAL_ROOT/results/binary_terminal.log" || exit 2
run binary_terminal_unchanged cmp "$QUAL_ROOT/results/binary_start.log" "$QUAL_ROOT/results/binary_terminal.log"
run source_terminal shasum -a 256 -c "$QUAL_ROOT/SOURCE_SHA256.txt"
printf 'failures=%s\n' "$QUAL_FAILURES" > "$QUAL_ROOT/results/terminal.txt"
printf 'terminal\n' > "$QUAL_ROOT/results/stage.txt"
test "$QUAL_FAILURES" -eq 0
