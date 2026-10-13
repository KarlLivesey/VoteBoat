#!/usr/bin/env bash
# SPDX-License-Identifier: RPL-1.5
# Untouched published-default reference; no selected64-entry envelope override.
set -euo pipefail
binary=${1:?Usage: run-upstream-curve.sh BINARY NEW_RESULT_DIRECTORY [REPETITIONS] [OPERATIONS]}
out=${2:?Missing result directory}
repetitions=${3:-3}
operations=${4:-20000000}
[[ -x "$binary" ]]
[[ "$repetitions" =~ ^[1-9][0-9]*$ ]]
[[ "$operations" =~ ^[1-9][0-9]*$ ]]
mkdir "$out"
if command -v sha256sum >/dev/null; then
  sha256sum "$binary" > "$out/artifact.sha256"
else
  shasum -a 256 "$binary" > "$out/artifact.sha256"
fi
uname -a > "$out/host.txt"
failed=0
for trial in $(seq 1 "$repetitions"); do
  for cell in '1 1' '64 1' '256 1' '512 1' '1024 1' '4096 1' '4096 4'; do
    read -r clients batch <<< "$cell"
    name="$out/upstream-c$clients-b$batch-t$trial"
    if "$binary" --client-workers 1 --server-workers 16 --clients "$clients" \
      --batch "$batch" --operations "$operations" --members 3 \
      --warmup-writes 1024 --latency > "$name.stdout" 2> "$name.stderr"; then
      printf '0\n' > "$name.exit"
    else
      status=$?
      printf '%s\n' "$status" > "$name.exit"
      failed=1
    fi
  done
done
exit "$failed"
