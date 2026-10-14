#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice436/main
run() {
 local stage="$1"
 shift
 printf '%s\n' "$stage" > "$proof/FINAL_STAGE"
 printf '%q ' "$@" > "$proof/$stage.command"
 printf '\n' >> "$proof/$stage.command"
 set +e
 "$@" > "$proof/$stage.log" 2> "$proof/$stage.stderr"
 local rc=$?
 set -e
 printf '%s\n' "$rc" > "$proof/$stage.status"
 if [[ "$rc" != 0 ]]; then
  printf '%s\n' "$rc" > "$proof/FINAL_STATUS"
  tail -n 70 "$proof/$stage.log" "$proof/$stage.stderr"
  exit "$rc"
 fi
}
run final-waits-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline voteboat_engine::waits::tests -- --test-threads=1 --show-output
run final-matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run final-matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run final-source-guard python3 -c 'import hashlib,json,stat;from pathlib import Path;s=json.loads(Path("target/p9/slice436/main/SOURCE.json").read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in s["sha256"].items());assert all(oct(stat.S_IMODE(Path(p).stat().st_mode))==v for p,v in s["physical_modes"].items());old=json.loads(Path("target/p9/slice436/main/attempt1/SOURCE.json").read_text());assert [p for p,h in old["sha256"].items() if s["sha256"][p]!=h]==["benchmarks/matched/src/voteboat_engine/waits/tests.rs"];print(s["inputs"],"final source hashes/modes exact; initialproduction+56unchangedcontrols reused only by1434unchangedhashes")'
printf '0\n' > "$proof/FINAL_STATUS"
printf 'DONE\n' > "$proof/FINAL_STAGE"
tail -n 13 "$proof/final-waits-tests.log"
cat "$proof/final-source-guard.log"
