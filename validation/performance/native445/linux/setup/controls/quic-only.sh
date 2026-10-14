#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native445-linux
root="$job/controls/quic"
test ! -e "$root"
printf '%s\n' "$$" > "$job/controls/QUIC_PID"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/controls/QUIC_STATUS"' EXIT
printf '%q ' "$job/artifacts/native/native_benchmark" "$root" quic 128 64 --ready-items 64 --journal-timings > "$job/controls/quic.command";printf '\n' >> "$job/controls/quic.command"
set +e
"$job/artifacts/native/native_benchmark" "$root" quic 128 64 --ready-items 64 --journal-timings > "$job/controls/quic.stdout" 2> "$job/controls/quic.stderr"
rc=$?;set -e;printf '%s\n' "$rc" > "$job/controls/quic.exit";test "$rc" = 0
