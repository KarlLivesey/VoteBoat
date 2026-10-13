#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-comparison424-mac
export PATH=/usr/bin:/bin:/usr/sbin:/sbin
export MATCHED_JOB="$job"
ulimit -n 4096
printf '%s\n' "$$" > "$job/PID"
printf 'running\n' > "$job/STATUS"
set +e
bash "$job/phase-diagnostic.sh" "$job/CONFIG.json" "$job/results" > "$job/runner.log" 2>&1
rc=$?
printf '%s\n' "$rc" > "$job/STATUS"
exit "$rc"
