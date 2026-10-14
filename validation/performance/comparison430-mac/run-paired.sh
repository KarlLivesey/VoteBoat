#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-comparison430-LDd4qx
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export MATCHED_JOB="$job"
ulimit -n 4096
printf 'RUNNING\n' > "$job/PAIR_STATUS"
printf 'RUNNING\n' > "$job/PAIR_STAGE"
set +e
bash "$job/phase-diagnostic.sh" "$job/CONFIG_PAIR.mac.json" "$job/results" > "$job/paired.log" 2> "$job/paired.stderr"
rc=$?
set -e
printf '%s\n' "$rc" > "$job/PAIR_STATUS"
if [[ "$rc" == 0 ]]; then printf 'DONE\n' > "$job/PAIR_STAGE"; else printf 'FAILED\n' > "$job/PAIR_STAGE"; fi
exit "$rc"
