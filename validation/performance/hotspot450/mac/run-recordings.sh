#!/usr/bin/env bash
set -euo pipefail
job=$(cd "$(dirname "$0")" && pwd)
candidate=/tmp/voteboat-mac450-d6395be-WjqDo4
pair=/tmp/voteboat-comparison450-GUz9m1
samply=/tmp/voteboat-samply0131-IDCfdm/extracted/samply-aarch64-apple-darwin/samply
exe="$candidate/artifacts/matched/voteboat-matched-framework"
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
mkdir "$job/test-tmp"
export TMPDIR="$job/test-tmp"
printf '%s\n' "$$" > "$job/PID"
printf 'running\n' > "$job/STATUS"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"; if [[ "$rc" = 0 ]]; then printf "DONE\n" > "$job/STAGE"; else printf "FAILED\n" > "$job/STAGE"; fi' EXIT
for cell in bulk singleton; do
  printf 'identity_before_%s\n' "$cell" > "$job/STAGE"
  /usr/bin/python3 "$pair/artifact-guard.py" "$pair/CONFIG_PAIR.mac.json" > "$job/$cell-identity-before.log" 2>&1
  shasum -a 256 "$samply" "$exe" > "$job/$cell-sha-before.txt"
  ps -axo pid,ppid,comm,args | grep -E 'cargo|rustc|voteboat-matched-framework|phase-diagnostic|samply record' | grep -v grep > "$job/$cell-quiet-before.txt" || true
  test ! -s "$job/$cell-quiet-before.txt"
  clients=4096; batch=4; operations=20000000
  if [[ "$cell" == singleton ]]; then clients=1; batch=1; operations=1048576; fi
  cmd=("$samply" record --save-only --rate 1000 --unstable-presymbolicate --symbol-dir "$candidate/target-matched-release/release" --output "$job/$cell-profile.json.gz" -- "$exe" --engine voteboat --clients "$clients" --batch "$batch" --operations "$operations" --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution false --campaign-timeout-secs 3600)
  printf '%q ' "${cmd[@]}" > "$job/$cell.command"; printf '\n' >> "$job/$cell.command"
  printf 'record_%s\n' "$cell" > "$job/STAGE"
  set +e
  "${cmd[@]}" > "$job/$cell.stdout.json" 2> "$job/$cell.stderr"
  rc=$?
  set -e
  printf '%s\n' "$rc" > "$job/$cell.exit"
  test "$rc" = 0
  printf 'identity_after_%s\n' "$cell" > "$job/STAGE"
  /usr/bin/python3 "$pair/artifact-guard.py" "$pair/CONFIG_PAIR.mac.json" > "$job/$cell-identity-after.log" 2>&1
  shasum -a 256 "$samply" "$exe" > "$job/$cell-sha-after.txt"
  cmp "$job/$cell-sha-before.txt" "$job/$cell-sha-after.txt"
done
