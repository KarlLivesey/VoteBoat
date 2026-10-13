#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native421-linux
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
stage(){ printf '%s\n' "$1" > "$job/STAGE"; }
printf '%s\n' "$$" > "$job/PID"
if [[ -e "$job/target-native-release" ]]; then printf 'Refusing nonfresh native target\n' >&2; exit 1; fi
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
unset CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS
unset CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS CARGO_PROFILE_RELEASE_OPT_LEVEL
cargo=$(rustup which --toolchain stable cargo)
rustc=$(rustup which --toolchain stable rustc)
export RUSTC="$rustc"
export PROTOC=/usr/bin/protoc
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true
export CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_TARGET_DIR="$job/target-native-release"
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
printf '%s\n' 'opt3 debug_assertions=true overflow_checks=true debuginfo=default0 lto=default codegen_units=default; framework overrides and rustflags/wrappers unset' > "$job/logs/native-release-profile.txt"
stage source_before
python3 "$job/guard.py" "$job" source > "$job/logs/source-before.log"
(cd "$job/source" && sha256sum -c "$job/source.sha256" && sha256sum -c "$job/ready-source.sha256") > "$job/logs/source-before-full.log"
stage native_release
printf '%q ' "$cargo" build --locked --offline --release --all-features --manifest-path "$job/source/Cargo.toml" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.command"
printf '\n' >> "$job/logs/native-release.command"
"$cargo" build --locked --offline --release --all-features --manifest-path "$job/source/Cargo.toml" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.jsonl" 2> "$job/logs/native-release.stderr"
stage source_after
python3 "$job/guard.py" "$job" source > "$job/logs/source-after.log"
(cd "$job/source" && sha256sum -c "$job/source.sha256" && sha256sum -c "$job/ready-source.sha256") > "$job/logs/source-after-full.log"
stage artifact_guards
python3 "$job/guard.py" "$job" artifacts > "$job/logs/artifact-guards.log"
chmod -R a-w "$job/source"
stage DONE
