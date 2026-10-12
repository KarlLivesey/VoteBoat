#!/bin/bash
set -u
qual_base=$1
qual_prior=/tmp/voteboat-macos-read394-vuYC17yB
qual_joint=/tmp/voteboat-macos-joint393-44XuvTd2
qual_repo="$qual_base/repo"
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
    if [ "$rc" != 0 ]; then printf 'guard_failure=%s\nexit=%s\n' "$name" "$rc" > "$qual_results/terminal.txt"; exit "$rc"; fi
}
qual_transfer(){ ( cd "$qual_base" && shasum -a 256 -c TRANSFER_SHA256.txt ); }
qual_protoc(){
    printf '%s  %s\n' 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" | shasum -a 256 -c - || return $?
    "$PROTOC" --version
}
qual_old_source(){ ( cd "$qual_prior/repo" && shasum -a 256 -c ../SOURCE_SHA256.txt ); }
qual_stage transfer_before qual_transfer
qual_stage old394_binary_before shasum -a 256 -c "$qual_prior/results/binary_terminal.log"
qual_stage old394_source_before qual_old_source
qual_stage old393_binary_before shasum -a 256 -c "$qual_joint/results/BINARY_BEFORE.txt"
qual_stage old393_source_before bash -c 'cd "$1/repo" && shasum -a 256 -c SOURCE_SHA256.txt' -- "$qual_joint"
cd "$qual_base" || exit 2
tar -xzf MAC_SOURCE.tar.gz || exit 2
cd "$qual_repo" || exit 2
qual_stage source_before shasum -a 256 -c SOURCE_SHA256.txt
qual_stage protoc_before qual_protoc
qual_stage platform bash -c 'uname -sm; cargo +stable --version; rustc +stable -vV; printf "release_debug_assertions=%s\nrelease_overflow_checks=%s\n" "$CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS" "$CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS"'
qual_stage fmt cargo +stable fmt --all -- --check
qual_stage strict_all cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
qual_stage strict_default cargo +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
qual_stage strict_core cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
qual_stage strict_native cargo +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
qual_actual(){
    local profile=$1 target=$2 kind=$3 line expected
    line=$(/usr/bin/grep -F '"reason":"compiler-artifact"' "$qual_results/build_$profile.log" | /usr/bin/grep -F "\"name\":\"$target\"" | /usr/bin/grep -F "\"kind\":[\"$kind\"]") || return 2
    [ "$(printf '%s\n' "$line" | wc -l | tr -d ' ')" -eq 1 ] || return 2
    if [ "$profile" = all ]; then expected='"features":["default","native","quic","tls"]'; else expected='"features":["default","native","tls"]'; fi
    printf '%s\n' "$line" | /usr/bin/grep -F '"opt_level":"3"' | /usr/bin/grep -F '"debug_assertions":true' | /usr/bin/grep -F '"overflow_checks":true' | /usr/bin/grep -F '"fresh":false' | /usr/bin/grep -F "$expected" >/dev/null || return 2
    printf '%s\n' "$line" > "$qual_results/ACTUAL_${target}_${profile}.json"
    local filenames
    if [ "$target" = startup ]; then
        filenames=$(printf '%s\n' "$line" | sed -E 's/.*"executable":"([^"]+)".*/\1/')
        [ -f "$filenames" ] && [ -x "$filenames" ] || return 2
        printf '%s\n' "$filenames" > "$qual_results/startup_$profile.path"
    else
        filenames=$(printf '%s\n' "$line" | sed -E 's/.*"filenames":\["([^"]+\.rlib)".*/\1/')
        [ -f "$filenames" ] || return 2
    fi
    shasum -a 256 "$filenames" >> "$qual_results/BINARY_$profile.txt"
}
qual_case_failed=0
qual_case(){
    local profile=$1 executable
    executable=$(cat "$qual_results/startup_$profile.path")
    qual_stage "list_$profile" "$executable" --list
    local selected
    selected=$(/usr/bin/grep -c ': test$' "$qual_results/list_$profile.log")
    if [ "$profile" = all ]; then [ "$selected" -eq 57 ] || exit 2; else [ "$selected" -eq 42 ] || exit 2; fi
    printf '%q ' "$executable" --nocapture --test-threads=1 > "$qual_results/$profile.command"
    printf '\n' >> "$qual_results/$profile.command"
    "$executable" --nocapture --test-threads=1 > "$qual_results/$profile.log" 2>&1
    local rc=$?
    printf '%s=%s\n' "$profile" "$rc" >> "$qual_results/exits.txt"
    if [ "$rc" != 0 ]; then qual_case_failed=1; fi
}
qual_stage build_all cargo +stable test --locked --offline --release --all-features --test startup --no-run --message-format=json
qual_stage actual_test_all qual_actual all startup test
qual_stage actual_library_all qual_actual all voteboat lib
qual_stage source_before_all shasum -a 256 -c SOURCE_SHA256.txt
qual_case all
qual_stage binary_after_all shasum -a 256 -c "$qual_results/BINARY_all.txt"
qual_stage source_after_all shasum -a 256 -c SOURCE_SHA256.txt
qual_stage build_default cargo +stable test --locked --offline --release --test startup --no-run --message-format=json
qual_stage prior_all_unchanged shasum -a 256 -c "$qual_results/BINARY_all.txt"
qual_stage actual_test_default qual_actual default startup test
qual_stage actual_library_default qual_actual default voteboat lib
qual_stage source_before_default shasum -a 256 -c SOURCE_SHA256.txt
qual_case default
qual_stage binary_after_default shasum -a 256 -c "$qual_results/BINARY_default.txt"
qual_stage prior_all_terminal shasum -a 256 -c "$qual_results/BINARY_all.txt"
qual_stage source_terminal shasum -a 256 -c SOURCE_SHA256.txt
qual_stage protoc_terminal qual_protoc
qual_stage transfer_terminal qual_transfer
qual_stage old394_binary_terminal shasum -a 256 -c "$qual_prior/results/binary_terminal.log"
qual_stage old394_source_terminal qual_old_source
qual_stage old393_binary_terminal shasum -a 256 -c "$qual_joint/results/BINARY_BEFORE.txt"
qual_stage old393_source_terminal bash -c 'cd "$1/repo" && shasum -a 256 -c SOURCE_SHA256.txt' -- "$qual_joint"
printf 'case_failures=%s\nexit=%s\n' "$qual_case_failed" "$qual_case_failed" > "$qual_results/terminal.txt"
exit "$qual_case_failed"
