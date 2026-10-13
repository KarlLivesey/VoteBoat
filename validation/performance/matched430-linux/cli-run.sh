#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/matched430-linux
trap 'rc=$?; printf "%s\n" "$rc" > "$job/CLI_STATUS"' EXIT
python3 "$job/cli.py"
