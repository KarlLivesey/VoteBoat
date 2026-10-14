#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/baseline430-pressure
trap 'rc=$?; printf "%s\n" "$rc" > "$job/BUILD_STATUS"' EXIT
printf '%s\n' "$$" > "$job/PID"
printf '%s\n' source-before > "$job/BUILD_STAGE"
(cd "$job/source" && sha256sum -c "$job/source.sha256") > "$job/logs/source-before.log"
[[ ! -e "$job/target-checks" ]] || exit 1
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER
for name in $(env | sed -n 's/^\(CARGO_PROFILE_\(DEV\|TEST\)_[A-Z_]*\)=.*/\1/p'); do unset "$name"; done
cargo=$(rustup which --toolchain stable cargo)
rustc=$(rustup which --toolchain stable rustc)
export RUSTC="$rustc"
export CARGO_TARGET_DIR="$job/target-checks" CARGO_BUILD_JOBS=2
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
"$cargo" -V > "$job/logs/cargo.txt"
"$rustc" -Vv > "$job/logs/rustc.txt"
"$PROTOC" --version > "$job/logs/protoc.txt"
printf '%s\n' "$cargo" > "$job/logs/cargo-path.txt"
printf '%s\n' "$rustc" > "$job/logs/rustc-path.txt"
printf '%s\n' "$PROTOC" > "$job/logs/protoc-path.txt"
sha256sum "$cargo" "$rustc" "$PROTOC" > "$job/logs/tool-binaries.sha256"
printf '%s\n' default_debug_profile_no_overrides > "$job/logs/profile.txt"
printf '%s\n' build > "$job/BUILD_STAGE"
cmd=("$cargo" test --manifest-path "$job/source/Cargo.toml" --locked --offline --all-features --test effect_owner --no-run --message-format=json -vv)
printf '%q ' "${cmd[@]}" > "$job/logs/build.command"; printf '\n' >> "$job/logs/build.command"
"${cmd[@]}" > "$job/logs/build.jsonl" 2> "$job/logs/build.stderr"
printf '%s\n' source-after > "$job/BUILD_STAGE"
(cd "$job/source" && sha256sum -c "$job/source.sha256") > "$job/logs/source-after.log"
printf '%s\n' DONE > "$job/BUILD_STAGE"
