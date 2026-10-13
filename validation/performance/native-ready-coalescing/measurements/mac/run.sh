#!/usr/bin/env bash
set -euo pipefail
root=/tmp/voteboat-native414-mac
ulimit -n 4096
mkdir "$root/test-tmp"
export TMPDIR="$root/test-tmp"
printf '%s\n' "$$" > "$root/PID"
printf '%s\n' running > "$root/STATUS"
set +e
/opt/homebrew/bin/node "$root/measure.mjs" "$root/reference" "$root/candidate" "$root/mac-results" > "$root/runner.log" 2>&1
code=$?
printf '%s\n' "$code" > "$root/STATUS"
exit "$code"
