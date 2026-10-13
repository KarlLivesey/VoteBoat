#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-mac419-native
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
printf '%s\n' "$$" > "$job/PID"
printf 'running\n' > "$job/STATUS"
set +e
/opt/homebrew/bin/node "$job/measure.mjs" "$job/reference" "$job/candidate" "$job/results" > "$job/runner.log" 2>&1
rc=$?
printf '%s\n' "$rc" > "$job/STATUS"
exit "$rc"
