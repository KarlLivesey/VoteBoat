#!/usr/bin/env bash
set -euo pipefail
export PYTHONDONTWRITEBYTECODE=1
job=$(cd -- "$(dirname -- "$0")" && pwd)
trap 'rc=$?; printf "%s\n" "$rc" > "$job/ALL_STATUS"; printf "%s\n" "$rc" > "$job/LINUX_STATUS"' EXIT
export MATCHED_JOB="$job"
printf 'BULK_START\n' > "$job/ALL_STAGE"
bash "$job/phase-diagnostic.sh" "$job/CONFIG_PAIR.linux.json" "$job/linux-results" bulk
printf '0\n' > "$job/BULK_STATUS"
printf 'BULK_END_SINGLETON_START\n' > "$job/ALL_STAGE"
bash "$job/phase-diagnostic.sh" "$job/CONFIG_PAIR.linux.json" "$job/linux-singleton-results" singleton
printf '0\n' > "$job/SINGLETON_STATUS"
printf 'ALL_TIMING_END\n' > "$job/ALL_STAGE"
