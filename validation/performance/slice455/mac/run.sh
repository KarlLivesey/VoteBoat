#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
required_commit=$(cat "$job/REQUIRED_COMMIT")
[[ "$required_commit" =~ ^[0-9a-f]{40}$ ]] || { printf "Unpublished candidate refused\n" >&2; exit 103; }
expected_tests=$(cat "$job/EXPECTED_MATCHED_TESTS")
[[ "$expected_tests" =~ ^[1-9][0-9]*$ ]] || { printf "Actual checked test count required\n" >&2; exit 104; }
/usr/bin/python3 "$job/verify-source.py"
cargo=/Users/karllivesey/.cargo/bin/cargo
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_BUILD_JOBS=2
unset RUSTC RUSTDOC CARGO_INCREMENTAL CARGO_PROFILE_RELEASE_PANIC CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER CARGO_BUILD_RUSTFLAGS CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS CARGO_PROFILE_RELEASE_OPT_LEVEL CARGO_PROFILE_RELEASE_DEBUG CARGO_PROFILE_RELEASE_STRIP
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) unset "$name";; esac
done < <(env)
unset RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS CARGO_BUILD_TARGET
ulimit -n 4096
mkdir "$job/logs" "$job/test-tmp"
export TMPDIR="$job/test-tmp"
printf '%s\n' "$$" > "$job/PID"
printf '%s\n' running > "$job/STATUS"
stage(){ printf '%s\n' "$1" > "$job/STAGE"; }
finish(){
 rc=$?
 (cd "$job/source" && shasum -a 256 -c "$job/source.sha256") > "$job/logs/source-final.log" 2>&1 || rc=102
 printf '%s\n' "$rc" > "$job/STATUS"
 exit "$rc"
}
trap finish EXIT
run(){
 name=$1; shift; stage "$name"
 printf '%q ' "$@" > "$job/logs/$name.command"; printf '\n' >> "$job/logs/$name.command"
 set +e
 "$@" > "$job/logs/$name.log" 2>&1
 rc=$?; set -e
 printf '%s\n' "$rc" > "$job/logs/$name.exit"
 return "$rc"
}
stage preflight
/Users/karllivesey/.cargo/bin/rustc +stable -Vv > "$job/logs/rustc.txt"
"$cargo" +stable -V > "$job/logs/cargo.txt"
/Users/karllivesey/.cargo/bin/rustup which --toolchain stable rustc > "$job/logs/rustc-path.txt"
/Users/karllivesey/.cargo/bin/rustup which --toolchain stable cargo > "$job/logs/cargo-path.txt"
shasum -a 256 "$(cat "$job/logs/rustc-path.txt")" "$(cat "$job/logs/cargo-path.txt")" "$PROTOC" > "$job/logs/tool-sha256.txt"
uname -sm > "$job/logs/host.txt"
sysctl -n machdep.cpu.brand_string hw.physicalcpu hw.logicalcpu hw.memsize >> "$job/logs/host.txt"
sw_vers >> "$job/logs/host.txt"
printf '%s  %s\n' 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" | shasum -a 256 -c > "$job/logs/protoc.txt"
"$PROTOC" --version >> "$job/logs/protoc.txt"
(cd "$job/source" && shasum -a 256 -c "$job/source.sha256") > "$job/logs/source-before.log"
run source_preflight /usr/bin/python3 "$job/verify-source.py"
cd "$job/source"
scopes=(application log_owned log_store log_staging log_reclaim provider_conformance raft snapshot effect_owner wire striped_log_store logical_voter_application logical_voter_native logical_voter_network logical_voter_payload logical_formats logical_configuration_history logical_nested_native logical_relay_native transaction_application program_transaction_application metadata_transfer routed scoped_source transfer_source transfer_target retirement)
args=(); for name in "${scopes[@]}"; do args+=(--test "$name"); done
export CARGO_TARGET_DIR="$job/target-root-all"
run root_crc "$cargo" +stable test --locked --offline --all-features --lib format_crc::tests -- --test-threads=1
run root_lib_all "$cargo" +stable test --locked --offline --all-features --lib -- --test-threads=1 --nocapture
run root_protocol_all "$cargo" +stable test --locked --offline --all-features "${args[@]}" -- --test-threads=1
export CARGO_TARGET_DIR="$job/target-root-core"
run root_lib_core "$cargo" +stable test --locked --offline --no-default-features --lib -- --test-threads=1
run root_protocol_core "$cargo" +stable test --locked --offline --no-default-features "${args[@]}" -- --test-threads=1
export CARGO_TARGET_DIR="$job/target-root-all"
run root_three_services "$cargo" +stable test --locked --offline --all-features --test counter_service three_ -- --test-threads=1
run root_fmt "$cargo" +stable fmt --all -- --check
export CARGO_TARGET_DIR="$job/target-root-strict"
run root_default_strict "$cargo" +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run root_all_strict "$cargo" +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run root_core_strict "$cargo" +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run root_native_strict "$cargo" +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
export CARGO_TARGET_DIR="$job/target-matched-checks"
manifest="$job/source/benchmarks/matched/Cargo.toml"
run matched_fmt "$cargo" +stable fmt --manifest-path "$manifest" -- --check
run matched_tests "$cargo" +stable test --locked --offline --manifest-path "$manifest" --message-format=json -vv -- --test-threads=1 --nocapture
run matched_strict "$cargo" +stable clippy --locked --offline --keep-going --all-targets --manifest-path "$manifest" -- -D warnings
export CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=packed
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3 CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 CARGO_PROFILE_RELEASE_DEBUG=2
stage release_matched
export CARGO_TARGET_DIR="$job/target-matched-release"
test ! -e "$CARGO_TARGET_DIR"
printf '%s\n' 'opt3 debug2 debug_assertions=true overflow_checks=true lto=thin codegen_units=1' > "$job/logs/matched-release-profile.txt"
printf '%q ' "$cargo" +stable build --locked --offline --release --manifest-path "$manifest" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.command"
set +e
"$cargo" +stable build --locked --offline --release --manifest-path "$manifest" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.jsonl" 2> "$job/logs/matched-release.stderr"
rc=$?;set -e;printf '%s\n' "$rc" > "$job/logs/matched-release.exit";test "$rc" = 0
for mode in off on openraft; do
 args=(--clients 4096 --batch 4 --operations 16384 --warmup 1024 --maintenance default-retention --envelopes upstream-default --campaign-timeout-secs 3600)
 if [[ "$mode" == openraft ]]; then args+=(--engine openraft); else args+=(--engine voteboat --attribution "$([[ "$mode" == on ]] && printf true || printf false)"); fi
 run "cli-$mode" "$CARGO_TARGET_DIR/release/voteboat-matched-framework" "${args[@]}"
done
stage artifact_guards
run artifact_guards /usr/bin/python3 "$job/collect.py"
stage DONE
