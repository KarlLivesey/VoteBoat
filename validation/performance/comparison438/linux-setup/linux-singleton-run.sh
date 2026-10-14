#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/comparison438
trap 'rc=$?; printf "%s\n" "$rc" > "$job/LINUX_SINGLETON_STATUS"' EXIT
export MATCHED_JOB="$job"
printf '%s\n' RUNNING > "$job/LINUX_SINGLETON_STAGE"
bash "$job/phase-diagnostic.sh" "$job/CONFIG_PAIR.linux.json" "$job/linux-singleton-results" singleton
printf '%s\n' DONE > "$job/LINUX_SINGLETON_STAGE"
