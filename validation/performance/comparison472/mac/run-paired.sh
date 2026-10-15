#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
printf '%s\n' "$$" > "$job/CAMPAIGN_PID"
printf 'RUNNING\n' > "$job/CAMPAIGN_STATUS"
finish(){ rc=$?; printf '%s\n' "$rc" > "$job/CAMPAIGN_STATUS"; exit "$rc"; }
trap finish EXIT
for cell in bulk singleton; do
 printf '%s\n' "$cell" > "$job/CAMPAIGN_STAGE"
 bash "$job/run-cell.sh" "$cell"
done
printf 'DONE\n' > "$job/CAMPAIGN_STAGE"
