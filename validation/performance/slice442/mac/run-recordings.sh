#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
[[ "${1:-}" == --authorized-quiet-window ]] || { printf 'Quiet window authorization required\n' >&2; exit 103; }
samply=/tmp/voteboat-samply0131-IDCfdm/extracted/samply-aarch64-apple-darwin/samply
exe="$job/artifacts/native/native_benchmark"
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
mkdir "$job/record-tmp"
export TMPDIR="$job/record-tmp"
printf '%s\n' "$$" > "$job/RECORD_PID"
printf 'running\n' > "$job/RECORD_STATUS"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/RECORD_STATUS"; if [[ "$rc" = 0 ]]; then printf "DONE\n" > "$job/RECORD_STAGE"; else printf "FAILED\n" > "$job/RECORD_STAGE"; fi' EXIT
for cell in bulk singleton; do
 printf 'guard_before_%s\n' "$cell" > "$job/RECORD_STAGE"
 /usr/bin/python3 "$job/verify-source.py" > "$job/$cell-source-before.log" 2>&1
 shasum -a 256 -c "$job/ARTIFACTS.sha256" > "$job/$cell-artifacts-before.log" 2>&1
 shasum -a 256 "$samply" "$exe" > "$job/$cell-sha-before.txt"
 operations=15000; window=32
 if [[ "$cell" == singleton ]]; then operations=4096; window=1; fi
 test ! -e "$job/$cell-workload"
 cmd=("$samply" record --save-only --rate 1000 --unstable-presymbolicate --symbol-dir "$job/target-native-release/release/examples" --symbol-dir "$job/artifacts/symbols" --output "$job/$cell-profile.json.gz" -- "$exe" "$job/$cell-workload" tcp "$operations" "$window" --ready-items 16 --journal-timings)
 printf '%q ' "${cmd[@]}" > "$job/$cell.command"; printf '\n' >> "$job/$cell.command"
 printf 'record_%s\n' "$cell" > "$job/RECORD_STAGE"
 set +e
 "${cmd[@]}" > "$job/$cell.stdout" 2> "$job/$cell.stderr"
 rc=$?; set -e
 printf '%s\n' "$rc" > "$job/$cell.exit"
 test "$rc" = 0
 printf 'guard_after_%s\n' "$cell" > "$job/RECORD_STAGE"
 /usr/bin/python3 "$job/verify-source.py" > "$job/$cell-source-after.log" 2>&1
 shasum -a 256 -c "$job/ARTIFACTS.sha256" > "$job/$cell-artifacts-after.log" 2>&1
 shasum -a 256 "$samply" "$exe" > "$job/$cell-sha-after.txt"
 cmp "$job/$cell-sha-before.txt" "$job/$cell-sha-after.txt"
done
