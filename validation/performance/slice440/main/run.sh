#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice440/main
printf '%s\n' "$$" > "$proof/PID"
trap 'rc=$?; printf "%s\n" "$rc" > "$proof/STATUS"' EXIT
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
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1 --nocapture
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run source-guard python3 -c 'import hashlib,json,stat;from pathlib import Path;r=Path("target/p9/slice440/main");s=json.loads((r/"SOURCE.json").read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in s["sha256"].items());assert all(oct(stat.S_IMODE(Path(p).stat().st_mode))==v for p,v in s["physical_modes"].items());b=json.loads((r/"SOURCE_BEFORE.json").read_text());assert [p for p in b["sha256"] if b["sha256"][p]!=s["sha256"][p]]==s["changed"];assert set(s["sha256"])-set(b["sha256"])==set(s["new"]);assert all(s["physical_modes"][p]==m for p,m in b["physical_modes"].items());print(s["inputs"],"exactsourcehash+modes; changed2+new2 onlyprivatebenchmarkkey; all1410selectedrootinputs hashes+modes unchanged")'
printf 'DONE\n' > "$proof/STAGE"
tail -n 10 "$proof/matched-tests.log"
cat "$proof/source-guard.log"
