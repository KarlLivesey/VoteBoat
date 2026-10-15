#!/usr/bin/env bash
# Four selected fresh artifact functional offered-capacity controls, not capacity timing.
set -euo pipefail
job="$PWD/target/p9/native476-linux"
python3 "$job/guard.py" "$job" qualification
python3 "$job/guard.py" "$job" source
python3 -u "$job/controls/run.py"
python3 "$job/guard.py" "$job" source
