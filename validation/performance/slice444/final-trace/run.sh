#!/usr/bin/env bash
set -euo pipefail
cd /home/karllivesey/Documents/ChatGPT/VoteBoat
proof=target/p9/slice444/final-trace
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
run matched-tests cargo +stable test --manifest-path benchmarks/matched/Cargo.toml --locked --offline voteboat_engine::tests::compacted_prefix_reaches_a_lagging_follower_through_actual_snapshot_install -- --exact --test-threads=1 --nocapture
run matched-format cargo +stable fmt --manifest-path benchmarks/matched/Cargo.toml -- --check
run matched-clippy cargo +stable clippy --manifest-path benchmarks/matched/Cargo.toml --locked --offline --keep-going --all-targets -- -D warnings
printf 'DONE\n' > "$proof/STAGE"
tail -n 6 "$proof/matched-tests.log"
