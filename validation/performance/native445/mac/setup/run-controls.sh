#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
export PATH=/Users/karllivesey/.cargo/bin:/opt/homebrew/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
printf '%s\n' "$$" > "$job/CONTROL_PID"
printf 'running\n' > "$job/CONTROL_STATUS"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/CONTROL_STATUS"' EXIT
exe="$job/artifacts/native/native_benchmark"; node=/opt/homebrew/bin/node
test -x "$node"; "$node" --version > "$job/node-version.txt"; shasum -a 256 "$node" > "$job/node-sha256.txt"
for protocol in tcp quic; do
 printf '%s\n' "$protocol" > "$job/CONTROL_STAGE"
 cmd=("$exe" "$job/control-$protocol" "$protocol" 128 64 --ready-items 64 --journal-timings)
 printf '%q ' "${cmd[@]}" > "$job/control-$protocol.command";printf '\n' >> "$job/control-$protocol.command"
 set +e
 "${cmd[@]}" > "$job/control-$protocol.stdout" 2> "$job/control-$protocol.stderr"
 rc=$?;set -e;printf '%s\n' "$rc" > "$job/control-$protocol.exit";test "$rc" = 0
 "$node" "$job/source/validation/check-native-benchmark.mjs" "$job/control-$protocol" > "$job/control-$protocol-result-checker.log" 2>&1
 "$node" "$job/source/validation/check-publication-timings.mjs" "$job/control-$protocol/publication.csv" > "$job/control-$protocol-publication-checker.log" 2>&1
done
printf 'DONE\n' > "$job/CONTROL_STAGE"
