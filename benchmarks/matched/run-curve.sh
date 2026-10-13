#!/usr/bin/env bash
# SPDX-License-Identifier: RPL-1.5
# Executables must already be built and qualified; this script never runs Cargo.
set -euo pipefail
binary=${1:?Usage: run-curve.sh BINARY NEW_RESULT_DIRECTORY [REPETITIONS] [OPERATIONS] [MAINTENANCE] [ENVELOPES]}
out=${2:?Missing result directory}
repetitions=${3:-3}
operations=${4:-20000000}
maintenance=${5:-default-retention}
envelopes=${6:-upstream-default}
[[ -x "$binary" ]]
[[ "$repetitions" =~ ^[1-9][0-9]*$ ]]
[[ "$operations" =~ ^[1-9][0-9]*$ ]]
[[ "$maintenance" == default-retention || "$maintenance" == retain-all ]]
[[ "$envelopes" == upstream-default || "$envelopes" == common64 ]]
mkdir "$out"
if command -v sha256sum >/dev/null; then
  sha256sum "$binary" > "$out/artifact.sha256"
else
  shasum -a 256 "$binary" > "$out/artifact.sha256"
fi
uname -a > "$out/host.txt"
printf 'operations=%s\nrepetitions=%s\nmaintenance=%s\nenvelopes=%s\n' "$operations" "$repetitions" "$maintenance" "$envelopes" > "$out/settings.txt"
failed=0
for trial in $(seq 1 "$repetitions"); do
  # Interleave engine order so warm host state does not always favor one engine.
  engines=(openraft voteboat)
  if (( trial % 2 == 0 )); then engines=(voteboat openraft); fi
  for cell in '1 1' '64 1' '256 1' '512 1' '1024 1' '4096 1' '4096 4'; do
    read -r clients batch <<< "$cell"
    for engine in "${engines[@]}"; do
      name="$out/$engine-c$clients-b$batch-t$trial"
      if "$binary" --engine "$engine" --clients "$clients" --batch "$batch" \
        --operations "$operations" --warmup 1024 --maintenance "$maintenance" --envelopes "$envelopes" \
        > "$name.json" 2> "$name.stderr"; then
        printf '0\n' > "$name.exit"
      else
        status=$?
        printf '%s\n' "$status" > "$name.exit"
        failed=1
      fi
    done
  done
done
exit "$failed"
