#!/bin/bash
set -u
qual_repo=$(pwd)
qual_base="$qual_repo/.."
qual_results="$qual_base/results"
mkdir -p "$qual_results" "$qual_base/test-tmp"
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:/usr/local/bin:$PATH"
export CARGO_TARGET_DIR="$qual_base/build"
export CARGO_BUILD_JOBS=2
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export TMPDIR="$qual_base/test-tmp"
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
qual_stage(){
    local name=$1
    shift
    printf '%q ' "$@" > "$qual_results/$name.command"
    printf '\n' >> "$qual_results/$name.command"
    "$@" > "$qual_results/$name.log" 2>&1
    local rc=$?
    printf '%s=%s\n' "$name" "$rc" >> "$qual_results/exits.txt"
    if [ "$rc" != 0 ]; then printf 'failure=%s\nexit=%s\n' "$name" "$rc" > "$qual_results/terminal.txt"; exit "$rc"; fi
}
qual_transfer(){ ( cd "$qual_base" && shasum -a 256 -c TRANSFER_SHA256.txt ); }
qual_protoc(){
    printf '%s  %s\n' 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" | shasum -a 256 -c - || return $?
    "$PROTOC" --version
}
qual_binary(){
    local candidate found=
    for candidate in "$CARGO_TARGET_DIR/release/deps/logical_activation_nested"-*; do
        if [ -f "$candidate" ] && [ -x "$candidate" ]; then
            [ -z "$found" ] || return 2
            found=$candidate
        fi
    done
    [ -n "$found" ] || return 2
    printf '%s\n' "$found" > "$qual_results/logical_activation_nested.path"
    shasum -a 256 "$found" > "$qual_results/BINARY_BEFORE.txt" || return $?
    local libraries=0
    for candidate in "$CARGO_TARGET_DIR/release/deps/libvoteboat"-*.rlib; do
        [ -f "$candidate" ] || continue
        libraries=$((libraries+1))
        shasum -a 256 "$candidate" >> "$qual_results/BINARY_BEFORE.txt" || return $?
    done
    [ "$libraries" -eq 1 ]
}
qual_stage transfer_before qual_transfer
qual_stage source_before shasum -a 256 -c SOURCE_SHA256.txt
qual_stage protoc_before qual_protoc
qual_stage platform bash -c 'uname -sm; cargo +stable --version; rustc +stable -vV; printf "test_debug_assertions=%s\ntest_overflow_checks=%s\n" "$CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS" "$CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS"'
qual_stage fmt cargo +stable fmt --all -- --check
qual_stage strict_all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
qual_stage strict_default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
qual_stage strict_core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
qual_stage strict_native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
qual_stage build cargo +stable test --locked --offline --release --all-features --test logical_activation_nested --no-run --message-format=json
qual_profile(){
    local line
    line=$(/usr/bin/grep -F '"reason":"compiler-artifact"' "$qual_results/build.log" | /usr/bin/grep -F '"name":"logical_activation_nested"') || return 2
    [ "$(printf '%s\n' "$line" | wc -l | tr -d ' ')" -eq 1 ] || return 2
    printf '%s\n' "$line" | /usr/bin/grep -F '"opt_level":"3"' | /usr/bin/grep -F '"debug_assertions":true' | /usr/bin/grep -F '"overflow_checks":true' | /usr/bin/grep -F '"fresh":false' >/dev/null || return 2
    printf '%s\n' "$line" > "$qual_results/ACTUAL_TEST_ARTIFACT.json"
}
qual_library_profile(){
    local line
    line=$(/usr/bin/grep -F '"reason":"compiler-artifact"' "$qual_results/build.log" | /usr/bin/grep -F '"name":"voteboat"' | /usr/bin/grep -F '"kind":["lib"]') || return 2
    [ "$(printf '%s\n' "$line" | wc -l | tr -d ' ')" -eq 1 ] || return 2
    printf '%s\n' "$line" | /usr/bin/grep -F '"opt_level":"3"' | /usr/bin/grep -F '"debug_assertions":true' | /usr/bin/grep -F '"overflow_checks":true' | /usr/bin/grep -F '"fresh":false' >/dev/null || return 2
    printf '%s\n' "$line" > "$qual_results/ACTUAL_LIBRARY_ARTIFACT.json"
}
qual_stage actual_profile qual_profile
qual_stage actual_library_profile qual_library_profile
qual_stage binary_before qual_binary
qual_executable=$(cat "$qual_results/logical_activation_nested.path")
qual_case_failed=0
qual_case(){
    local name=$1
    shift
    printf '%q ' "$@" > "$qual_results/$name.command"
    printf '\n' >> "$qual_results/$name.command"
    "$@" > "$qual_results/$name.log" 2>&1
    local rc=$?
    printf '%s=%s\n' "$name" "$rc" >> "$qual_results/exits.txt"
    if [ "$rc" != 0 ]; then qual_case_failed=1; fi
}
qual_case joint_tcp "$qual_executable" cold_joint::tcp_all_twelve_stores_joint_and_parent_intent_cold_reconstruct_then_final --exact --nocapture --test-threads=1
qual_stage tcp_selected_one /usr/bin/grep -q '^running 1 test$' "$qual_results/joint_tcp.log"
qual_stage tcp_binary_unchanged shasum -a 256 -c "$qual_results/BINARY_BEFORE.txt"
qual_case joint_quic "$qual_executable" cold_joint::quic_all_twelve_stores_joint_and_parent_intent_cold_reconstruct_then_final --exact --nocapture --test-threads=1
qual_stage quic_selected_one /usr/bin/grep -q '^running 1 test$' "$qual_results/joint_quic.log"
qual_stage binary_after shasum -a 256 -c "$qual_results/BINARY_BEFORE.txt"
qual_stage source_after shasum -a 256 -c SOURCE_SHA256.txt
qual_stage protoc_after qual_protoc
qual_stage transfer_after qual_transfer
printf 'case_failures=%s\nexit=%s\n' "$qual_case_failed" "$qual_case_failed" > "$qual_results/terminal.txt"
exit "$qual_case_failed"
