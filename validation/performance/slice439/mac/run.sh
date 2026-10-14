#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
required_commit=$(cat "$job/REQUIRED_COMMIT")
[[ "$required_commit" =~ ^[0-9a-f]{40}$ ]] || { printf "Unpublished candidate refused\n" >&2; exit 103; }
/usr/bin/python3 "$job/verify-source.py"
cargo=/Users/karllivesey/.cargo/bin/cargo
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export PROTOC=/tmp/voteboat-macos-CzK1if/reference-tools296/bin/protoc
export CARGO_BUILD_JOBS=2
unset RUSTC RUSTDOC CARGO_INCREMENTAL CARGO_PROFILE_RELEASE_PANIC CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER CARGO_BUILD_RUSTFLAGS CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS CARGO_PROFILE_RELEASE_OPT_LEVEL CARGO_PROFILE_RELEASE_DEBUG CARGO_PROFILE_RELEASE_STRIP
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
run reuse_preflight /usr/bin/python3 "$job/verify-source.py" before
cd "$job/source"
export CARGO_TARGET_DIR="$job/target-matched-checks"
manifest="$job/source/benchmarks/matched/Cargo.toml"
run matched_fmt "$cargo" +stable fmt --manifest-path "$manifest" -- --check
run replies "$cargo" +stable test --locked --offline --manifest-path "$manifest" actor::tests::replies -- --test-threads=1 --nocapture
run matched_strict "$cargo" +stable clippy --locked --offline --keep-going --all-targets --manifest-path "$manifest" -- -D warnings
stage final_guards
run final_guards /usr/bin/python3 "$job/verify-source.py" final
stage DONE
