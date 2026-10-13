#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-mac425-ccee8ec
cargo=/Users/karllivesey/.cargo/bin/cargo
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_BUILD_JOBS=2
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS CARGO_PROFILE_RELEASE_OPT_LEVEL
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
cd "$job/source"
export CARGO_TARGET_DIR=/tmp/voteboat-mac424-41ee217/target-matched-checks
manifest="$job/source/benchmarks/matched/Cargo.toml"
run matched_fmt "$cargo" +stable fmt --manifest-path "$manifest" -- --check
run matched_tests "$cargo" +stable test --locked --offline --manifest-path "$manifest" -- --test-threads=1
run matched_strict "$cargo" +stable clippy --locked --offline --all-targets --manifest-path "$manifest" -- -D warnings
run matched_cli_build "$cargo" +stable build --locked --offline --manifest-path "$manifest"
for mode in false true; do
 run "cli-$mode" "$CARGO_TARGET_DIR/debug/voteboat-matched-framework" --engine voteboat --clients 1 --batch 4 --operations 12000 --warmup 0 --maintenance default-retention --envelopes upstream-default --attribution "$mode" --campaign-timeout-secs 60
done
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3 CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 CARGO_PROFILE_RELEASE_DEBUG=2
stage release_matched
export CARGO_TARGET_DIR="$job/target-matched-release"
printf '%s\n' 'opt3 debug2 debug_assertions=true overflow_checks=true lto=thin codegen_units=1' > "$job/logs/matched-release-profile.txt"
printf '%q ' "$cargo" +stable build --locked --offline --release --manifest-path "$manifest" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.command"
set +e
"$cargo" +stable build --locked --offline --release --manifest-path "$manifest" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.jsonl" 2> "$job/logs/matched-release.stderr"
rc=$?;set -e;printf '%s\n' "$rc" > "$job/logs/matched-release.exit";test "$rc" = 0
stage artifact_guards
/usr/bin/python3 "$job/collect.py"
stage DONE
