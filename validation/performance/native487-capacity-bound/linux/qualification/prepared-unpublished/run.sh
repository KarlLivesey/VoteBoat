#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native486-capacity-bound/linux
finish(){
 rc=$?
 printf '%s\n' "$rc" > "$job/STATUS"
 /usr/bin/python3.14 - "$job" "$rc" <<'RECEIPT'
from pathlib import Path
import json,sys,datetime
job=Path(sys.argv[1]);(job/'BUILD_TERMINAL.json').write_text(json.dumps({'actual_exit':int(sys.argv[2]),'stage':(job/'STAGE').read_text().strip() if (job/'STAGE').exists() else 'preflight','end':datetime.datetime.now(datetime.timezone.utc).isoformat()},indent=2)+'\n')
RECEIPT
}
trap finish EXIT
stage(){ printf '%s\n' "$1" > "$job/STAGE"; }
printf '%s\n' "$$" > "$job/PID"
/usr/bin/python3.14 "$job/guard.py" "$job" qualification > "$job/logs/qualification-preflight.log"
if [[ -e "$job/target-native-release" ]]; then printf 'Refusing nonfresh native target\n' >&2; exit 1; fi
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) printf '%s\n' "$name" >> "$job/logs/unset-inherited-env-names.txt"; unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET CARGO_BUILD_RUSTC_WRAPPER CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER
unset CARGO_PROFILE_RELEASE_LTO CARGO_PROFILE_RELEASE_CODEGEN_UNITS CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO
unset CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS CARGO_PROFILE_RELEASE_OPT_LEVEL
cargo=$(rustup which --toolchain stable cargo)
rustc=$(rustup which --toolchain stable rustc)
export RUSTC="$rustc"
export PROTOC=/home/karllivesey/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/protobuf-build-0.14.1/bin/protoc-linux-x86_64
sha256sum -c /home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native480-admission/linux/logs/tool-binaries.sha256 > "$job/logs/baseline-tool-preflight.log"
test "$("$PROTOC" --version)" = 'libprotoc 3.9.0'
export CARGO_BUILD_JOBS=2 CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3
export CARGO_PROFILE_RELEASE_DEBUG=0
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
printf '%s\n' 'opt3 debug_assertions=true overflow_checks=true debuginfo=0 acceptance native-default split/lto/codegen lto=default codegen_units=default; framework overrides and rustflags/wrappers unset' > "$job/logs/native-release-profile.txt"
stage source_before
/usr/bin/python3.14 "$job/guard.py" "$job" source > "$job/logs/source-before.log"
(cd "$job/source" && sha256sum -c "$job/source.sha256") > "$job/logs/source-before-full.log"
stage native_release
printf '%q ' "$cargo" build --locked --offline --release --all-features --manifest-path "$job/source/Cargo.toml" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.command"
printf '\n' >> "$job/logs/native-release.command"
"$cargo" build --locked --offline --release --all-features --manifest-path "$job/source/Cargo.toml" --example native_benchmark --message-format=json -vv > "$job/logs/native-release.jsonl" 2> "$job/logs/native-release.stderr"
stage source_after
/usr/bin/python3.14 "$job/guard.py" "$job" source > "$job/logs/source-after.log"
(cd "$job/source" && sha256sum -c "$job/source.sha256") > "$job/logs/source-after-full.log"
stage artifact_guards
/usr/bin/python3.14 "$job/guard.py" "$job" artifacts > "$job/logs/artifact-guards.log"
# Preserveexactqualifiedphysicalpermissions including qualified private0600fixtures.
stage DONE
