#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/matched428-linux
trap 'rc=$?; printf "%s\n" "$rc" > "$job/RESUME_STATUS"' EXIT
printf '%s\n' artifact_guards > "$job/STAGE"
python3 "$job/guard.py" "$job" artifacts > "$job/logs/artifact-guards-resume.log" 2> "$job/logs/artifact-guards-resume.stderr"
chmod -R a-w "$job/source"
printf '%s\n' DONE > "$job/STAGE"
