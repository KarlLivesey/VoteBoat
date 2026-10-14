#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice437/main
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
  printf '%s\n' "$rc" > "$proof/STATUS"
  tail -n 70 "$proof/$stage.log" "$proof/$stage.stderr"
  exit "$rc"
 fi
}
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline -- --test-threads=1 --show-output
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
run source-guard python3 -c 'import hashlib,json,stat;from pathlib import Path;r=Path("target/p9/slice437/main");s=json.loads((r/"SOURCE.json").read_text());assert all(hashlib.sha256(Path(p).read_bytes()).hexdigest()==h for p,h in s["sha256"].items());assert all(oct(stat.S_IMODE(Path(p).stat().st_mode))==v for p,v in s["physical_modes"].items());assert all(not Path(p).exists() for p in s["deleted"]);l=json.loads((r/"ENGINE_LOADER.json").read_text());e=Path(l["path"]).read_bytes();suffix=l["appended_loader"].encode();assert e.endswith(suffix);assert hashlib.sha256(e[:-len(suffix)]).hexdigest()==l["baseline_sha256"];print(s["inputs"],"exact source/modes; engineprojection byteidentical435; noWaits production modules")'
printf '0\n' > "$proof/STATUS"
printf 'DONE\n' > "$proof/STAGE"
tail -n 10 "$proof/matched-tests.log"
cat "$proof/source-guard.log"
