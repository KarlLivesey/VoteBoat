#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice455/crc
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
trap 'rc=$?;printf "%s\n" "$rc" > "$job/REMAINING_STATUS"' EXIT
printf '%s\n' "$BASHPID" > "$job/REMAINING_PID"
cmd=(cargo +stable test --locked --offline --all-features --test logical_nested_native -- --test-threads=1 --skip cold_recovery::quic_two_levels_recover_actual_checkpoint_suffix_and_retry_from_closed_stores)
printf '%q ' "${cmd[@]}" > "$job/remaining-cold.command"
printf '\n' >> "$job/remaining-cold.command"
set +e
"${cmd[@]}" > "$job/remaining-cold.log" 2> "$job/remaining-cold.stderr"
rc=$?
set -e
printf '%s\n' "$rc" > "$job/remaining-cold.status"
printf 'COMMAND_END=remaining-cold exit=%s\n' "$rc"
exit "$rc"
