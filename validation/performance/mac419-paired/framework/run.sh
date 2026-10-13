#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-mac419-paired
export MATCHED_JOB="$job"
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
printf '%s\n' "$$" > "$job/PID"
printf 'primary18\n' > "$job/STAGE"
set +e
bash "$job/paired-framework.sh" "$job/primary" > "$job/primary.log" 2>&1
rc=$?
printf '%s\n' "$rc" > "$job/primary.exit"
if ((rc == 0)); then
 printf 'phase9\n' > "$job/STAGE"
 bash "$job/phase-diagnostic.sh" "$job/phase" > "$job/phase.log" 2>&1
 rc=$?
 printf '%s\n' "$rc" > "$job/phase.exit"
fi
printf '%s\n' "$rc" > "$job/STATUS"
printf 'DONE\n' > "$job/STAGE"
exit "$rc"
