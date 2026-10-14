#!/usr/bin/env bash
set -euo pipefail
job=$1
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"' EXIT
stage(){ printf '%s\n' "$1" > "$job/STAGE"; printf 'STAGE=%s\n' "$1"; }
run(){
 name=$1
 shift
 printf '%q ' "$@" > "$job/logs/$name.command"
 printf '\n' >> "$job/logs/$name.command"
 set +e
 "$@" > "$job/logs/$name.stdout" 2> "$job/logs/$name.stderr"
 result=$?
 set -e
 printf '%s\n' "$result" > "$job/logs/$name.exit"
 printf 'COMMAND_END=%s exit=%s\n' "$name" "$result"
 return "$result"
}
printf '%s\n' "$$" > "$job/PID"
cd "$job"
mkdir -p "$job/logs"
export RUSTUP_TOOLCHAIN=1.98.1
export PATH="$HOME/.rustup/toolchains/1.98.1-aarch64-unknown-linux-gnu/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) printf '%s\n' "$name" >> "$job/logs/unset-inherited-env-names.txt"; unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export PROTOC=/home/karllivesey/voteboat-p9-pi443-2YkqdC/tools/protoc
cargo=$(rustup which --toolchain 1.98.1 cargo)
rustc=$(rustup which --toolchain 1.98.1 rustc)
export RUSTC="$rustc"
stage source_before
run source-before python3 "$job/guard.py" "$job" source
stage metadata
printf '%s\n' "$cargo" > "$job/logs/cargo-path.txt"
printf '%s\n' "$rustc" > "$job/logs/rustc-path.txt"
printf '%s\n' "$PROTOC" > "$job/logs/protoc-path.txt"
run cargo-version "$cargo" -V
run rustc-version "$rustc" -Vv
run protoc-version "$PROTOC" --version
run tool-binaries sha256sum "$cargo" "$rustc" "$PROTOC"
run os uname -a
run cpu lscpu
run memory-before free -h
run storage-before df -h "$job" /tmp
run temperature-before vcgencmd measure_temp
run throttled-before vcgencmd get_throttled
run frequency-before vcgencmd measure_clock arm
stage qualification_reuse
run qualification-reuse python3 -c 'import json;from pathlib import Path;v=json.loads(Path("QUALIFICATION_REUSE.json").read_text());assert v["patch_final_source_matches"] and v["same_physical_modes"] and v["unchanged_inputs"]==1431;print("final published3filesexactPiqualification;1431baselineinputs+permissionsunchanged; no testsrepeated")'
stage matched_release
[[ ! -e "$job/target-matched-release" ]]
export CARGO_TARGET_DIR="$job/target-matched-release"
export CARGO_PROFILE_RELEASE_OPT_LEVEL=3 CARGO_PROFILE_RELEASE_DEBUG=2
export CARGO_PROFILE_RELEASE_SPLIT_DEBUGINFO=packed
export CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
export CARGO_PROFILE_RELEASE_LTO=thin CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1
printf '%s\n' 'opt3 debug2 packed assertions+overflow thinLTO cgu1; inheritedflags/wrappers/profileoverridesunset; buildjobs1' > "$job/logs/matched-release-profile.txt"
printf '%q ' "$cargo" build --locked --offline --release --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.command"
printf '\n' >> "$job/logs/matched-release.command"
set +e
"$cargo" build --locked --offline --release --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --bin voteboat-matched-framework --message-format=json -vv > "$job/logs/matched-release.jsonl" 2> "$job/logs/matched-release.stderr"
result=$?
set -e
printf '%s\n' "$result" > "$job/logs/matched-release.exit"
[[ "$result" == 0 ]]
stage artifact_guards
run artifacts python3 "$job/guard.py" "$job" artifacts
stage source_after_build
run source-after-build python3 "$job/guard.py" "$job" source
# ExistingCLIcollector insistsbuildDONE0: terminalbuildreceipts distinctfromfinaljobreceipt.
printf '%s\n' 0 > "$job/BUILD_STATUS"
printf '%s\n' DONE > "$job/BUILD_STAGE"
stage current_cli
run current-cli python3 "$job/cli.py"
stage final_environment
run memory-after free -h
run storage-after df -h "$job" /tmp
run temperature-after vcgencmd measure_temp
run throttled-after vcgencmd get_throttled
run frequency-after vcgencmd measure_clock arm
run governor-after cat /sys/devices/system/cpu/cpufreq/policy0/scaling_governor
run load-after uptime
run services-after systemctl --no-pager --plain list-units --type=service --state=running
run source-final python3 "$job/guard.py" "$job" source
stage DONE
