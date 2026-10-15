#!/usr/bin/env bash
# Three selected fresh artifact functional offered-capacity controls, not capacity timing.
set -euo pipefail
job="$PWD/target/p9/native486-capacity-bound/linux"
/usr/bin/python3.14 "$job/guard.py" "$job" qualification
/usr/bin/python3.14 "$job/guard.py" "$job" source
/usr/bin/python3.14 -u "$job/controls/run.py"
/usr/bin/python3.14 "$job/guard.py" "$job" source
/usr/bin/python3.14 "$job/ready.py"
