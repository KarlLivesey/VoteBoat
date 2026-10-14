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
mkdir -p "$job/logs"
export PATH="$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) printf '%s\n' "$name" >> "$job/logs/unset-inherited-env-names.txt"; unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET
export CARGO_BUILD_JOBS=1 CARGO_INCREMENTAL=0
export PROTOC="$job/tools/protoc"
cargo=$(rustup which --toolchain 1.98.1 cargo)
rustc=$(rustup which --toolchain 1.98.1 rustc)
export RUSTC="$rustc"
stage components
run components-before rustup component list --toolchain 1.98.1 --installed
components=$(cat "$job/logs/components-before.stdout")
if ! grep -q '^rustfmt-' <<< "$components"; then run install-rustfmt rustup component add --toolchain 1.98.1 rustfmt; fi
if ! grep -q '^clippy-' <<< "$components"; then run install-clippy rustup component add --toolchain 1.98.1 clippy; fi
run default-toolchain rustup show active-toolchain
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
stage pinned_dependencies
if [[ ! -d "$HOME/.cargo/registry/cache" || ! -d "$HOME/.cargo/git/checkouts" ]]; then
 run fetch-root "$cargo" fetch --manifest-path "$job/source/Cargo.toml" --locked --target aarch64-unknown-linux-gnu
 run fetch-matched "$cargo" fetch --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --locked --target aarch64-unknown-linux-gnu
else
 if ! run offline-fetch-root "$cargo" fetch --manifest-path "$job/source/Cargo.toml" --locked --offline --target aarch64-unknown-linux-gnu; then run fetch-root "$cargo" fetch --manifest-path "$job/source/Cargo.toml" --locked --target aarch64-unknown-linux-gnu; fi
 if ! run offline-fetch-matched "$cargo" fetch --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --locked --offline --target aarch64-unknown-linux-gnu; then run fetch-matched "$cargo" fetch --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --locked --target aarch64-unknown-linux-gnu; fi
fi
stage matched_tests
export CARGO_TARGET_DIR="$job/target-checks"
run matched-tests "$cargo" test --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --locked --offline -- --test-threads=1
run matched-format "$cargo" fmt --manifest-path "$job/source/benchmarks/matched/Cargo.toml" -- --check
run matched-clippy "$cargo" clippy --manifest-path "$job/source/benchmarks/matched/Cargo.toml" --locked --offline --keep-going --all-targets -- -D warnings
stage source_after_checks
run source-after-checks python3 "$job/guard.py" "$job" source
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
