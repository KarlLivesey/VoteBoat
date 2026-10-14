#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice441/main
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
run matched-replies cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline actor::tests::replies -- --test-threads=1 --nocapture
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run source-guard python3 -c 'import hashlib,json,stat;from pathlib import Path;r=Path("target/p9/slice441/main");s=json.loads((r/"SOURCE.json").read_text());b=json.loads((r/"SOURCE_BEFORE.json").read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in s["sha256"].items());assert all(oct(stat.S_IMODE(Path(p).stat().st_mode))==v for p,v in s["physical_modes"].items());assert [p for p in s["sha256"] if b["sha256"][p]!=s["sha256"][p]]==s["changed"];assert set(b["sha256"])-set(s["sha256"])==set(s["deleted"]);assert all(not Path(p).exists() for p in s["deleted"]);baseline=json.loads(Path("validation/performance/slice439/main/SOURCE.json").read_text());assert s["sha256"]==baseline["sha256"] and s["physical_modes"]==baseline["physical_modes"];print(s["inputs"],"sourcehashes/modes exact439; Actor exact435/diag439;1432otherinputs unchanged440;2deletedmodules absent")'
printf 'DONE\n' > "$proof/STAGE"
tail -n 10 "$proof/matched-replies.log"
cat "$proof/source-guard.log"
