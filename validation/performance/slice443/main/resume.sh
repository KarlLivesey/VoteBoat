#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice443/main
printf '%s\n' "$$" > "$proof/RESUME_PID"
trap 'rc=$?; printf "%s\n" "$rc" > "$proof/RESUME_STATUS"' EXIT
while IFS='=' read -r name value; do
 case "$name" in CARGO_PROFILE_*|CARGO_TARGET_*_RUSTFLAGS) unset "$name";; esac
done < <(env)
unset RUSTFLAGS CARGO_ENCODED_RUSTFLAGS RUSTDOCFLAGS CARGO_ENCODED_RUSTDOCFLAGS RUSTC_WRAPPER RUSTC_WORKSPACE_WRAPPER RUSTC RUSTDOC CARGO_BUILD_RUSTFLAGS CARGO_BUILD_TARGET
run() {
 local stage="$1"
 shift
 printf '%s\n' "$stage" > "$proof/RESUME_STAGE"
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
run native-result-checker node validation/check-native-benchmark.mjs target/p9/slice443/main/window64-retry
run publication-checker node validation/check-publication-timings.mjs target/p9/slice443/main/window64-retry/publication.csv
run source-guard python3 -c 'import hashlib,json,stat;from pathlib import Path;r=Path("target/p9/slice443/main");s=json.loads((r/"SOURCE.json").read_text());b=json.loads((r/"SOURCE_BEFORE.json").read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in s["sha256"].items());assert all(oct(stat.S_IMODE(Path(p).stat().st_mode))==v for p,v in s["physical_modes"].items());assert [p for p in s["sha256"] if b["sha256"][p]!=s["sha256"][p]]==s["changed"];assert s["physical_modes"]==b["physical_modes"];print(s["inputs"],"exactsourcehashes/modes; only4declaredCLI/doc/validatorpaths changed")'
printf 'DONE\n' > "$proof/RESUME_STAGE"
cat "$proof/native-result-checker.log" "$proof/publication-checker.log" "$proof/source-guard.log"
