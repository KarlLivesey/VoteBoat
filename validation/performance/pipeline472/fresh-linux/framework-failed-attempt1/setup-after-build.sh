#!/usr/bin/env bash
# Actual functional controls only; no comparison/timing launch.
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/matched472-linux
trap 'rc=$?; printf "%s\n" "$rc" > "$job/SETUP_STATUS"' EXIT
python3 "$job/guard.py" "$job" qualification > "$job/logs/setup-qualification-preflight.log"
bash "$job/cli-run.sh"
python3 "$job/guard.py" "$job" source > "$job/logs/setup-source-after.log"
printf 'ALL_SETUP_END_NO_TIMING\n' > "$job/SETUP_STAGE"
