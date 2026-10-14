#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native445-linux
binary="$job/artifacts/native/native_benchmark"
printf '%s\n' "$$" > "$job/controls/PID"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/controls/STATUS"' EXIT
for protocol in tcp quic; do
 printf '%s\n' "$protocol" > "$job/controls/STAGE"
 root="$job/controls/$protocol"
 test ! -e "$root"
 printf '%q ' "$binary" "$root" "$protocol" 128 64 --ready-items 64 --journal-timings > "$job/controls/$protocol.command"
 printf '\n' >> "$job/controls/$protocol.command"
 set +e
 "$binary" "$root" "$protocol" 128 64 --ready-items 64 --journal-timings > "$job/controls/$protocol.stdout" 2> "$job/controls/$protocol.stderr"
 rc=$?;set -e;printf '%s\n' "$rc" > "$job/controls/$protocol.exit";test "$rc" = 0
 printf '%q ' node "$job/source/validation/check-native-benchmark.mjs" "$root" > "$job/controls/$protocol-check.command";printf '\n' >> "$job/controls/$protocol-check.command"
 node "$job/source/validation/check-native-benchmark.mjs" "$root" > "$job/controls/$protocol-check.stdout" 2> "$job/controls/$protocol-check.stderr"
 printf '0\n' > "$job/controls/$protocol-check.exit"
 node "$job/source/validation/check-publication-timings.mjs" "$root/publication.csv" > "$job/controls/$protocol-publication.stdout" 2> "$job/controls/$protocol-publication.stderr"
 printf '0\n' > "$job/controls/$protocol-publication.exit"
done
printf 'DONE\n' > "$job/controls/STAGE"
