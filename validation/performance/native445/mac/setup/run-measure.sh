#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
[[ "${1:-}" == --authorized-quiet-window ]] || exit 103
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
printf '%s\n' "$$" > "$job/MEASURE_PID"
printf 'running\n' > "$job/MEASURE_STATUS"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/MEASURE_STATUS"' EXIT
exec_node=/opt/homebrew/bin/node
"$exec_node" "$job/measure.mjs" "$job/INPUT-mac.json" "$job/results" > "$job/measure.log" 2> "$job/measure.stderr"
