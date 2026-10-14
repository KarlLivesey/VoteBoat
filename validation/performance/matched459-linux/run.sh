#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/matched459-linux
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
stage(){ printf '%s\n' "$1" > "$job/STAGE"; }
printf '%s\n' "$$" > "$job/PID"
if [[ -e "$job/target-matched-release" ]]; then printf 'Refusing nonfresh matched target\n' >&2; exit 1; fi
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) printf '%s\n' "$name" >> "$job/logs/unset-inherited-env-names.txt"; unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET
unset CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS
unset CARGO_PROFILE_RELEASE_DEBUG CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO
unset CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS CARGO_PROFILE_RELEASE_OPT_LEVEL
cargo=$(rustup which --toolchain stable cargo)
rustc=$(rustup which --toolchain stable rustc)
export RUSTC="$rustc"
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG=2
export CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=packed
export CARGO_PROFILE_RELEASE_LTO=thin
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_TARGET_DIR="$job/target-matched-release"
stage publication_source_guard
python3 "$job/guard.py" "$job" source > "$job/logs/source-preflight.log"
stage metadata
printf '%s\n' "$cargo" > "$job/logs/cargo-path.txt"
printf '%s\n' "$rustc" > "$job/logs/rustc-path.txt"
"$cargo" -V > "$job/logs/cargo.txt"
"$rustc" -Vv > "$job/logs/rustc.txt"
printf '%s\n' "$PROTOC" > "$job/logs/protoc-path.txt"
"$PROTOC" --version > "$job/logs/protoc.txt"
sha256sum "$cargo" "$rustc" "$PROTOC" > "$job/logs/tool-binaries.sha256"
uname -a > "$job/logs/host.txt"
lscpu > "$job/logs/cpu.txt"
printf '%s\n' 'opt3 debug_assertions=true overflow_checks=true debuginfo=2 split-debuginfo=packed lto=thin codegen_units=1; rustflags/wrappers unset' > "$job/logs/matched-release-profile.txt"
stage source_before
python3 "$job/guard.py" "$job" source > "$job/logs/source-before.log"
(cd "$job/source" && sha256sum -c "$job/source.sha256" && sha256sum -c "$job/slice459-source.sha256") > "$job/logs/source-before-full.log"
stage matched_release
printf '%q ' "$cargo" build --locked --offline --release --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.command"
printf '\n' >> "$job/logs/matched-release.command"
"$cargo" build --locked --offline --release --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.jsonl" 2> "$job/logs/matched-release.stderr"
stage source_after
python3 "$job/guard.py" "$job" source > "$job/logs/source-after.log"
(cd "$job/source" && sha256sum -c "$job/source.sha256" && sha256sum -c "$job/slice459-source.sha256") > "$job/logs/source-after-full.log"
stage artifact_guards
python3 "$job/guard.py" "$job" artifacts > "$job/logs/artifact-guards.log"
# Keep exact qualified physical modes, including seven0600 fixture keys; sourceguards detect changes.
stage DONE
