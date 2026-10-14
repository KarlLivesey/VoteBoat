#!/usr/bin/env bash
set -euo pipefail
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
[[ "${1:-}" == --authorized-quiet-window ]] || { echo "root QUIET_START required" >&2; exit 64; }
job="$(cd -- "$(dirname -- "$0")" && pwd)"
mkdir -p "$job/measurements/mac/cases"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/measurements/mac/STATUS"; date -u +%Y-%m-%dT%H:%M:%SZ > "$job/measurements/mac/DONE"' EXIT
printf "%s\n" "$$" > "$job/measurements/mac/PID"
python3 "$job/guard.py" "$job/CONFIG.mac.json" "$job/measurements/mac/PRE_GUARD.json" > "$job/measurements/mac/PRE_GUARD.stdout" 2> "$job/measurements/mac/PRE_GUARD.stderr"
ulimit -n 4096
case_dir="$job/measurements/mac/cases/trial1-latency-off"
mkdir -p "$case_dir"
printf '%s\n' '/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024' > "$case_dir/command.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/START"
set +e
/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 > "$case_dir/stdout" 2> "$case_dir/stderr"
rc=$?
set -e
printf "%s\n" "$rc" > "$case_dir/exit"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/END"
[[ "$rc" == 0 ]] || exit "$rc"
case_dir="$job/measurements/mac/cases/trial1-latency-on"
mkdir -p "$case_dir"
printf '%s\n' '/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 --latency' > "$case_dir/command.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/START"
set +e
/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 --latency > "$case_dir/stdout" 2> "$case_dir/stderr"
rc=$?
set -e
printf "%s\n" "$rc" > "$case_dir/exit"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/END"
[[ "$rc" == 0 ]] || exit "$rc"
case_dir="$job/measurements/mac/cases/trial2-latency-on"
mkdir -p "$case_dir"
printf '%s\n' '/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 --latency' > "$case_dir/command.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/START"
set +e
/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 --latency > "$case_dir/stdout" 2> "$case_dir/stderr"
rc=$?
set -e
printf "%s\n" "$rc" > "$case_dir/exit"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/END"
[[ "$rc" == 0 ]] || exit "$rc"
case_dir="$job/measurements/mac/cases/trial2-latency-off"
mkdir -p "$case_dir"
printf '%s\n' '/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024' > "$case_dir/command.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/START"
set +e
/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 > "$case_dir/stdout" 2> "$case_dir/stderr"
rc=$?
set -e
printf "%s\n" "$rc" > "$case_dir/exit"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/END"
[[ "$rc" == 0 ]] || exit "$rc"
case_dir="$job/measurements/mac/cases/trial3-latency-off"
mkdir -p "$case_dir"
printf '%s\n' '/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024' > "$case_dir/command.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/START"
set +e
/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 > "$case_dir/stdout" 2> "$case_dir/stderr"
rc=$?
set -e
printf "%s\n" "$rc" > "$case_dir/exit"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/END"
[[ "$rc" == 0 ]] || exit "$rc"
case_dir="$job/measurements/mac/cases/trial3-latency-on"
mkdir -p "$case_dir"
printf '%s\n' '/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 --latency' > "$case_dir/command.txt"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/START"
set +e
/private/tmp/voteboat-upstream-default431-XxW0Du/mac/artifacts/bench --client-workers 1 --server-workers 16 --clients 4096 --batch 4 --operations 20000000 --members 3 --warmup-writes 1024 --latency > "$case_dir/stdout" 2> "$case_dir/stderr"
rc=$?
set -e
printf "%s\n" "$rc" > "$case_dir/exit"
date -u +%Y-%m-%dT%H:%M:%SZ > "$case_dir/END"
[[ "$rc" == 0 ]] || exit "$rc"
python3 "$job/guard.py" "$job/CONFIG.mac.json" "$job/measurements/mac/POST_GUARD.json" > "$job/measurements/mac/POST_GUARD.stdout" 2> "$job/measurements/mac/POST_GUARD.stderr"
