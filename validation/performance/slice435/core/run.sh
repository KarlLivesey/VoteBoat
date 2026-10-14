#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice435/core
run() {
  local stage="$1"
  shift
  printf '%s\n' "$stage" > "$proof/STAGE"
  printf '%q ' "$@" > "$proof/$stage.command"
  printf '\n' >> "$proof/$stage.command"
  set +e
  "$@" > "$proof/$stage.log" 2> "$proof/$stage.stderr"
  local result=$?
  set -e
  printf '%s\n' "$result" > "$proof/$stage.status"
  if [[ "$result" != 0 ]]; then
    printf '%s\n' "$result" > "$proof/STATUS"
    tail -n 70 "$proof/$stage.log" "$proof/$stage.stderr"
    exit "$result"
  fi
}
run fmt cargo +stable fmt --all
run focused cargo +stable test --locked --offline --all-features --lib completion_tests -- --test-threads=1
run fmt_check cargo +stable fmt --all -- --check
printf '0\n' > "$proof/STATUS"
printf 'DONE\n' > "$proof/STAGE"
tail -n 15 "$proof/focused.log"
