#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-mac421-1091610
cargo=/Users/karllivesey/.cargo/bin/cargo
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_BUILD_JOBS=2
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS
unset CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS CARGO_PROFILE_RELEASE_OPT_LEVEL
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
 name=$1; shift
 stage "$name"
 printf '%q ' "$@" > "$job/logs/$name.command"; printf '\n' >> "$job/logs/$name.command"
 set +e
 "$@" > "$job/logs/$name.log" 2>&1
 rc=$?
 set -e
 printf '%s\n' "$rc" > "$job/logs/$name.exit"
 return "$rc"
}
stage preflight
/Users/karllivesey/.cargo/bin/rustc +stable -Vv > "$job/logs/rustc.txt"
"$cargo" +stable -V > "$job/logs/cargo.txt"
uname -sm > "$job/logs/host.txt"
sysctl -n machdep.cpu.brand_string hw.physicalcpu hw.logicalcpu hw.memsize >> "$job/logs/host.txt"
sw_vers >> "$job/logs/host.txt"
printf '%s  %s\n' 9f40368c858666e7036df0b9b58a4acd085fed58ab531294c3e5b5820f8af360 "$PROTOC" | shasum -a 256 -c > "$job/logs/protoc.txt"
"$PROTOC" --version >> "$job/logs/protoc.txt"
(cd "$job/source" && shasum -a 256 -c "$job/source.sha256") > "$job/logs/source-before.log"
cd "$job/source"
export CARGO_TARGET_DIR="$job/target-root-checks"
run root_fmt "$cargo" +stable fmt --all -- --check
run lib_ready_all "$cargo" +stable test --locked --offline --all-features --lib ready_ -- --test-threads=1
run owner_runtime_log_ready_all "$cargo" +stable test --locked --offline --all-features --test runtime --test effect_owner --test log_store ready_ -- --test-threads=1
run startup_member_maintenance_all "$cargo" +stable test --locked --offline --all-features --test startup --test native_member_startup --test maintenance --test leadership_maintenance -- --test-threads=1
run native_original_recovery "$cargo" +stable test --locked --offline --all-features --test counter_service service_processes_retry_replace_leader_and_recover_native_files -- --test-threads=1
run native_maintenance "$cargo" +stable test --locked --offline --all-features --test counter_service maintenance:: -- --test-threads=1
run native_cli_ready "$cargo" +stable test --locked --offline --all-features --example native_benchmark ready_items -- --test-threads=1
run native_cli_build "$cargo" +stable build --locked --offline --all-features --example native_benchmark
for assembly in startup shared; do
 for protocol in tcp quic; do
  for quantum in 32 64; do
   name="functional-$assembly-$protocol-q$quantum"
   args=("$CARGO_TARGET_DIR/debug/examples/native_benchmark" "$job/$name" "$protocol" 64 32)
   if [[ "$assembly" == shared ]]; then args+=(2); fi
   args+=(--ready-items "$quantum")
   run "$name" "${args[@]}"
  done
 done
done
export CARGO_TARGET_DIR="$job/target-core-checks"
run owner_runtime_ready_core "$cargo" +stable test --locked --offline --no-default-features --test runtime --test effect_owner ready_ -- --test-threads=1
export CARGO_TARGET_DIR="$job/target-root-checks"
run root_strict_default "$cargo" +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run root_strict_all "$cargo" +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run root_strict_core "$cargo" +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run root_strict_native "$cargo" +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
export CARGO_TARGET_DIR="$job/target-matched-checks"
manifest="$job/source/benchmarks/matched/Cargo.toml"
run matched_fmt "$cargo" +stable fmt --manifest-path "$manifest" -- --check
run matched_tests "$cargo" +stable test --locked --offline --manifest-path "$manifest" -- --test-threads=1
run matched_strict "$cargo" +stable clippy --locked --offline --all-targets --manifest-path "$manifest" -- -D warnings
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
unset CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS
stage release_native
export CARGO_TARGET_DIR="$job/target-native-release"
printf '%s\n' 'opt3 debug_assertions=true overflow_checks=true lto=default codegen_units=default' > "$job/logs/native-release-profile.txt"
printf '%q ' "$cargo" +stable build --locked --offline --release --all-features --manifest-path "$job/source/Cargo.toml" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.command"
"$cargo" +stable build --locked --offline --release --all-features --manifest-path "$job/source/Cargo.toml" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.jsonl" 2> "$job/logs/native-release.stderr"
stage artifact_guards
/usr/bin/python3 "$job/collect.py"
stage DONE
