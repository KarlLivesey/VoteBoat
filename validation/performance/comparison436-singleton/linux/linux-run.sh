#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/comparison436-singleton
export MATCHED_JOB=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/comparison436
trap 'rc=$?; printf "%s\n" "$rc" > "$job/LINUX_STATUS"' EXIT
printf '%s\n' "$$" > "$job/PID"
printf '%s\n' RUNNING > "$job/LINUX_STAGE"
bash "$MATCHED_JOB/phase-diagnostic.sh" "$job/CONFIG_PAIR.linux.json" "$job/linux-results" singleton
printf '%s\n' DONE > "$job/LINUX_STAGE"
