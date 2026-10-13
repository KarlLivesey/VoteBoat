#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-comparison426-CyZVe6
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
export MATCHED_JOB="$job"
export TMPDIR="$job/test-tmp"
ulimit -n 4096
mkdir "$TMPDIR"
printf '%s\n' "$$" > "$job/PID"
printf 'running\n' > "$job/STATUS"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"; if [[ "$rc" = 0 ]]; then printf "DONE\n" > "$job/STAGE"; else printf "FAILED\n" > "$job/STAGE"; fi' EXIT
printf '%q ' bash "$job/phase-diagnostic.sh" "$job/CONFIG_PAIR.mac.json" "$job/raw" > "$job/run.command"
printf '\n' >> "$job/run.command"
bash "$job/phase-diagnostic.sh" "$job/CONFIG_PAIR.mac.json" "$job/raw"
