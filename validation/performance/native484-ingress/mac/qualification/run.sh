#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
required_commit=$(cat "$job/REQUIRED_COMMIT")
[[ "$required_commit" =~ ^[0-9a-f]{40}$ ]] || { echo 'Published483 REQUIRED_COMMIT is unset; refusing qualification' >&2; exit 103; }
[[ ! -e "$job/STATUS" && ! -e "$job/target-native-release" ]] || exit 104
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_BUILD_JOBS=2
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER
mkdir "$job/logs" "$job/test-tmp"
export TMPDIR="$job/test-tmp"
ulimit -n 4096
printf '%s\n' "$$" > "$job/PID"
printf '%s\n' running > "$job/STATUS"
stage(){ printf '%s\n' "$1" > "$job/STAGE"; printf 'STAGE %s\n' "$1"; }
finish(){
 rc=$?
 /usr/bin/python3 "$job/verify-source.py" > "$job/logs/source-final.log" 2>&1 || rc=102
 printf '%s\n' "$rc" > "$job/STATUS"
 printf 'MAC484_EXIT=%s\n' "$rc"
 exit "$rc"
}
trap finish EXIT
run(){
 name=$1; shift; stage "$name"
 printf '%q ' "$@" > "$job/logs/$name.command"; printf '\n' >> "$job/logs/$name.command"
 set +e
 "$@" > "$job/logs/$name.stdout" 2> "$job/logs/$name.stderr"
 rc=$?; set -e
 printf '%s\n' "$rc" > "$job/logs/$name.exit"
 return "$rc"
}
stage preflight
cargo=/Users/karllivesey/.cargo/bin/cargo
rustc=/Users/karllivesey/.cargo/bin/rustc
baseline=/private/tmp/voteboat-mac480-f7b0ef1-mccmJS
"$cargo" +stable -V > "$job/logs/cargo.txt"
"$rustc" +stable -Vv > "$job/logs/rustc.txt"
/Users/karllivesey/.cargo/bin/rustup which --toolchain stable cargo > "$job/logs/cargo-path.txt"
/Users/karllivesey/.cargo/bin/rustup which --toolchain stable rustc > "$job/logs/rustc-path.txt"
shasum -a 256 "$(cat "$job/logs/rustc-path.txt")" "$(cat "$job/logs/cargo-path.txt")" "$PROTOC" > "$job/logs/tool-sha256.txt"
"$PROTOC" --version > "$job/logs/protoc.txt"
node --version > "$job/logs/node.txt"
run tool_rustc_identity /usr/bin/cmp "$job/logs/rustc.txt" "$baseline/logs/rustc.txt"
run tool_cargo_identity /usr/bin/cmp "$job/logs/cargo.txt" "$baseline/logs/cargo.txt"
run tool_hash_identity /usr/bin/cmp "$job/logs/tool-sha256.txt" "$baseline/logs/tool-sha256.txt"
run tool_protoc_identity /usr/bin/cmp "$job/logs/protoc.txt" "$baseline/logs/protoc.txt"
run source_before /usr/bin/python3 "$job/verify-source.py"
cd "$job/source"
export CARGO_TARGET_DIR="$job/target-checks"
run application_all "$cargo" +stable test --locked --offline --all-features --test application --message-format=json -vv -- --test-threads=1
run client_all "$cargo" +stable test --locked --offline --all-features --test effect_owner --message-format=json -vv client_admission:: -- --test-threads=1
run node_all "$cargo" +stable test --locked --offline --all-features --test effect_owner --message-format=json -vv node_facade::submission_batches:: -- --test-threads=1
run provider_all "$cargo" +stable test --locked --offline --all-features --test provider_conformance --message-format=json -vv -- --test-threads=1
run all_library "$cargo" +stable test --locked --offline --all-features --lib --message-format=json -vv -- --test-threads=1
run counter_service "$cargo" +stable test --locked --offline --all-features --test counter_service --message-format=json -vv three_ -- --test-threads=1
run native_scoped "$cargo" +stable test --locked --offline --all-features --test startup --message-format=json -vv multi::ingress:: -- --test-threads=1
run application_core "$cargo" +stable test --locked --offline --no-default-features --test application --message-format=json -vv -- --test-threads=1
run client_core "$cargo" +stable test --locked --offline --no-default-features --test effect_owner --message-format=json -vv client_admission:: -- --test-threads=1
run node_core "$cargo" +stable test --locked --offline --no-default-features --test effect_owner --message-format=json -vv node_facade::submission_batches:: -- --test-threads=1
run provider_core "$cargo" +stable test --locked --offline --no-default-features --test provider_conformance --message-format=json -vv -- --test-threads=1
run provider_metadata node validation/check-provider-conformance.mjs
run provider_metadata_controls node --test --test-reporter=tap --test-isolation=none validation/check-provider-conformance.test.mjs
run root_fmt "$cargo" +stable fmt --all -- --check
run strict_default "$cargo" +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
run strict_all "$cargo" +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run strict_core "$cargo" +stable clippy --locked --offline --keep-going --all-targets --no-default-features -- -D warnings
run strict_native "$cargo" +stable clippy --locked --offline --keep-going --all-targets --no-default-features --features native -- -D warnings
export CARGO_TARGET_DIR="$job/target-matched-checks"
run matched_tests "$cargo" +stable test --locked --offline --manifest-path benchmarks/matched/Cargo.toml --message-format=json -vv -- --test-threads=1
run matched_fmt "$cargo" +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched_strict "$cargo" +stable clippy --locked --offline --keep-going --all-targets --manifest-path benchmarks/matched/Cargo.toml -- -D warnings
test ! -e "$job/target-native-release"
export CARGO_TARGET_DIR="$job/target-native-release"
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3 CARGO_PROFILE_RELEASE_DEBUG=0 CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
stage native_release
printf '%s\n' 'opt3 debug0 assertions/overflow true; native default LTO/codegen/split; inherited flags/wrappers/profile overrides unset' > "$job/logs/native-release-profile.txt"
printf '%q ' "$cargo" +stable build --locked --offline --release --all-features --example native_benchmark --message-format=json -vv > "$job/logs/native-release.command"
set +e
"$cargo" +stable build --locked --offline --release --all-features --example native_benchmark --message-format=json -vv > "$job/logs/native-release.jsonl" 2> "$job/logs/native-release.stderr"
rc=$?; set -e; printf '%s\n' "$rc" > "$job/logs/native-release.exit"; test "$rc" = 0
run artifact_guard /usr/bin/python3 "$job/collect-artifacts.py"
run positive_controls /usr/bin/python3 "$job/controls.py"
run source_after /usr/bin/python3 "$job/verify-source.py"
stage DONE
printf '%s\n' 'MAC484_SETUP_READY; no measurement campaign launched'
