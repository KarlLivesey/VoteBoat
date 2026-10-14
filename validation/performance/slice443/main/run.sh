#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice443/main
printf '%s\n' "$$" > "$proof/PID"
trap 'rc=$?; printf "%s\n" "$rc" > "$proof/STATUS"' EXIT
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET
run() {
 local stage="$1"
 shift
 printf '%s\n' "$stage" > "$proof/STAGE"
 printf '%q ' "$@" > "$proof/$stage.command"
 printf '\n' >> "$proof/$stage.command"
 set +e
 "$@" > "$proof/$stage.log" 2> "$proof/$stage.stderr"
 local rc=$?
 set -e
 printf '%s\n' "$rc" > "$proof/$stage.status"
 if [[ "$rc" != 0 ]]; then
  tail -n 60 "$proof/$stage.log" "$proof/$stage.stderr"
  exit "$rc"
 fi
}
run compiler rustc +stable -Vv
run cargo cargo +stable -V
run ready-items cargo +stable test --locked --offline --all-features --example native_benchmark ready_items::tests -- --test-threads=1 --nocapture
run lanes-bounds cargo +stable test --locked --offline --all-features --example native_benchmark lanes::tests::assignments_preserve_totals_and_never_overlap -- --test-threads=1 --nocapture
run result-checkers node --test validation/check-offered.test.mjs validation/check-lanes.test.mjs
run root-format cargo +stable fmt --all -- --check
run root-allfeature-strict cargo +stable clippy --locked --offline --keep-going --all-targets --all-features -- -D warnings
run native-debug-build cargo +stable build --locked --offline --all-features --example native_benchmark --message-format=json -vv
run invalid-normal-window bash -c 'test ! -e target/p9/slice443/main/invalid65; set +e; target/debug/examples/native_benchmark target/p9/slice443/main/invalid65 tcp 128 65 --ready-items 64 > target/p9/slice443/main/invalid65.stdout 2> target/p9/slice443/main/invalid65.stderr; rc=$?; printf "%s\n" "$rc" > target/p9/slice443/main/invalid65.exit; test "$rc" = 1 && test ! -e target/p9/slice443/main/invalid65 && rg -q "count/window out of bounds" target/p9/slice443/main/invalid65.stderr'
run invalid-lanes-window bash -c 'test ! -e target/p9/slice443/main/invalid-lanes33; set +e; target/debug/examples/native_benchmark --lanes target/p9/slice443/main/invalid-lanes33 tcp 128 33 1 1 > target/p9/slice443/main/invalid-lanes33.stdout 2> target/p9/slice443/main/invalid-lanes33.stderr; rc=$?; printf "%s\n" "$rc" > target/p9/slice443/main/invalid-lanes33.exit; test "$rc" = 1 && test ! -e target/p9/slice443/main/invalid-lanes33 && rg -q "invalid lanes/groups/operations/window bounds" target/p9/slice443/main/invalid-lanes33.stderr'
run native-window64 target/debug/examples/native_benchmark target/p9/slice443/main/window64 tcp 128 64 --ready-items 64 --journal-timings
run native-result-checker node validation/check-native-benchmark.mjs target/p9/slice443/main/window64
run publication-checker node validation/check-publication-timings.mjs target/p9/slice443/main/window64/publication.csv
run source-guard python3 -c 'import hashlib,json,stat;from pathlib import Path;r=Path("target/p9/slice443/main");s=json.loads((r/"SOURCE.json").read_text());b=json.loads((r/"SOURCE_BEFORE.json").read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in s["sha256"].items());assert all(oct(stat.S_IMODE(Path(p).stat().st_mode))==v for p,v in s["physical_modes"].items());assert [p for p in s["sha256"] if b["sha256"][p]!=s["sha256"][p]]==s["changed"];assert s["physical_modes"]==b["physical_modes"];print(s["inputs"],"exactsourcehashes/modes; only4declaredCLI/doc/validatorpaths changed")'
printf 'DONE\n' > "$proof/STAGE"
cat "$proof/native-window64.log" "$proof/native-result-checker.log" "$proof/publication-checker.log" "$proof/source-guard.log"
