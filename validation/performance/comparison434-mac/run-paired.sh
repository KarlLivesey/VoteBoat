#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-comparison434-zQKX8S
cell=${1:?bulk or singleton required}
[[ "$cell" == bulk || "$cell" == singleton ]] || exit 103
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export MATCHED_JOB="$job"
ulimit -n 4096
printf 'RUNNING\n' > "$job/$cell-STATUS"
printf 'RUNNING\n' > "$job/$cell-STAGE"
set +e
bash "$job/phase-diagnostic.sh" "$job/CONFIG_PAIR.mac.json" "$job/$cell-results" "$cell" > "$job/$cell-paired.log" 2> "$job/$cell-paired.stderr"
rc=$?
set -e
printf '%s\n' "$rc" > "$job/$cell-STATUS"
if [[ "$rc" == 0 ]]; then printf 'DONE\n' > "$job/$cell-STAGE"; else printf 'FAILED\n' > "$job/$cell-STAGE"; fi
exit "$rc"
