#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/comparison436
trap 'rc=$?; printf "%s\n" "$rc" > "$job/CONTROLS_STATUS"' EXIT
run(){
 local name=$1; shift
 printf '%q ' "$@" > "$job/$name.command"; printf '\n' >> "$job/$name.command"
 set +e
 "$@" > "$job/$name.log" 2> "$job/$name.stderr"
 local rc=$?
 set -e
 printf '%s\n' "$rc" > "$job/$name.status"
 [[ $rc -eq 0 ]] || exit "$rc"
}
run checker-controls python3 "$job/check-main-controls.py"
run artifact-controls python3 "$job/check-artifact-controls.py"
run guard-final python3 "$job/artifact-guard.py" "$job/CONFIG_PAIR.linux.json"
run runner-syntax bash -n "$job/phase-diagnostic.sh" "$job/linux-run.sh"
