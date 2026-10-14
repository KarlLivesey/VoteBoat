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
node=/opt/homebrew/bin/node
test -x "$node"
"$node" --version > "$job/logs/node-version.txt"
shasum -a 256 "$node" > "$job/logs/node-sha256.txt"
run root_scope_reuse /usr/bin/python3 "$job/check-reuse.py"
cd "$job/source"
export CARGO_TARGET_DIR="$job/target-debug-checks"
run ready_items "$cargo" +stable test --locked --offline --all-features --example native_benchmark --message-format=json -vv ready_items::tests -- --test-threads=1 --nocapture
run lanes_bounds "$cargo" +stable test --locked --offline --all-features --example native_benchmark lanes::tests::assignments_preserve_totals_and_never_overlap -- --test-threads=1 --nocapture
run result_checkers "$node" --test validation/check-offered.test.mjs validation/check-lanes.test.mjs
run root_fmt "$cargo" +stable fmt --all -- --check
stage native_debug_build
cmd=("$cargo" +stable build --locked --offline --all-features --example native_benchmark --message-format=json -vv)
printf '%q ' "${cmd[@]}" > "$job/logs/native_debug_build.command"; printf '\n' >> "$job/logs/native_debug_build.command"
set +e
"${cmd[@]}" > "$job/logs/native_debug_build.jsonl" 2> "$job/logs/native_debug_build.stderr"
rc=$?; set -e; printf '%s\n' "$rc" > "$job/logs/native_debug_build.exit";test "$rc" = 0
exe="$CARGO_TARGET_DIR/debug/examples/native_benchmark"
for kind in normal lanes; do
 stage "invalid_$kind"
 root="$job/invalid-$kind"; test ! -e "$root"
 args=("$root" tcp 128 65 --ready-items 64)
 message='count/window out of bounds'
 if [[ "$kind" == lanes ]]; then args=(--lanes "$root" tcp 128 33 1 1); message='invalid lanes/groups/operations/window bounds'; fi
 printf '%q ' "$exe" "${args[@]}" > "$job/logs/invalid_$kind.command";printf '\n' >> "$job/logs/invalid_$kind.command"
 set +e
 "$exe" "${args[@]}" > "$job/logs/invalid_$kind.stdout" 2> "$job/logs/invalid_$kind.stderr"
 rc=$?;set -e;printf '%s\n' "$rc" > "$job/logs/invalid_$kind.exit"
 test "$rc" = 1;test ! -e "$root"; grep -Fq "$message" "$job/logs/invalid_$kind.stderr"
done
run native_window64 "$exe" "$job/window64" tcp 128 64 --ready-items 64 --journal-timings
run native_result_checker "$node" validation/check-native-benchmark.mjs "$job/window64"
run publication_checker "$node" validation/check-publication-timings.mjs "$job/window64/publication.csv"
export CARGO_TARGET_DIR="$job/target-default-strict"
run strict_default "$cargo" +stable clippy --locked --offline --keep-going --all-targets -- -D warnings
export CARGO_TARGET_DIR="$job/target-debug-checks"
run strict_all "$cargo" +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run source_final /usr/bin/python3 "$job/verify-source.py"
stage DONE
