#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/baseline430-pressure
trap 'rc=$?; printf "%s\n" "$rc" > "$job/PRESSURE_STATUS"' EXIT
export TMPDIR="$job/test-tmp" RUST_BACKTRACE=1
mkdir -p "$TMPDIR"
cd "$job/source"
exe="$job/target-checks/debug/deps/effect_owner-3b9732cae5c45d5b"
printf '%s\n' BEFORE > "$job/PRESSURE_STAGE"
sha256sum -c "$job/source.sha256" > "$job/logs/pressure-source-before.log"
printf '%s  %s\n' 25f1b5241a1bc2b98cbd4eb7a9d8f02d9dc925ecd7f928c620b72e6f55606cff "$exe" | sha256sum -c > "$job/logs/pressure-artifact-before.log"
cmd=("$exe" native::native_shared_peer_quota_pressure_reconnect_restart_and_retry --exact --nocapture --test-threads=1)
printf '%q ' "${cmd[@]}" > "$job/logs/pressure.command"; printf '\n' >> "$job/logs/pressure.command"
printf '%s\n' RUNNING > "$job/PRESSURE_STAGE"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
set +e
{ time "${cmd[@]}" > "$job/logs/pressure.log" 2> "$job/logs/pressure.stderr"; } 2> "$job/logs/pressure.time"
rc=$?
set -e
printf '%s\n' "$rc" > "$job/logs/pressure.status"
sha256sum -c "$job/source.sha256" > "$job/logs/pressure-source-after.log"
printf '%s  %s\n' 25f1b5241a1bc2b98cbd4eb7a9d8f02d9dc925ecd7f928c620b72e6f55606cff "$exe" | sha256sum -c > "$job/logs/pressure-artifact-after.log"
printf '%s\n' DONE > "$job/PRESSURE_STAGE"
exit "$rc"
