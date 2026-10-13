#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-samply-record426-QGQfjO
candidate=/tmp/voteboat-mac426-799f2cf-VRN5Xr
samply=/tmp/voteboat-samply0131-IDCfdm/extracted/samply-aarch64-apple-darwin/samply
exe="$candidate/artifacts/matched/voteboat-matched-framework"
export PATH=/Users/karllivesey/.cargo/bin:/tmp/voteboat-macos-CzK1if/reference-tools296/bin:/usr/bin:/bin:/usr/sbin:/sbin
ulimit -n 4096
mkdir "$job/test-tmp"
export TMPDIR="$job/test-tmp"
printf '%s\n' "$$" > "$job/PID"
printf 'running\n' > "$job/STATUS"
trap 'rc=$?; printf "%s\n" "$rc" > "$job/STATUS"; if [[ "$rc" = 0 ]]; then printf "DONE\n" > "$job/STAGE"; else printf "FAILED\n" > "$job/STAGE"; fi' EXIT
printf 'identity_before\n' > "$job/STAGE"
/usr/bin/python3 /tmp/voteboat-comparison426-CyZVe6/artifact-guard.py /tmp/voteboat-comparison426-CyZVe6/CONFIG_PAIR.mac.json > "$job/identity-before.log" 2>&1
shasum -a 256 "$samply" "$exe" "$candidate/target-matched-release/release/voteboat-matched-framework.dSYM/Contents/Resources/DWARF/voteboat_matched_framework-0f72fff4df956fc5" > "$job/sha-before.txt"
printf '%s\n' 'samply0.13.1 presymbolicate does not pass CLI symbol_dir to its SymbolManager. Before recording an adjacent dSYM symlink points only to UUID-matched originalbundle; no binary/source bytes changed.' > "$job/SYMBOL_RESOLUTION.txt"
readlink "$candidate/artifacts/matched/voteboat-matched-framework.dSYM" >> "$job/SYMBOL_RESOLUTION.txt"
ps -axo pid,ppid,comm,args | grep -E 'cargo|rustc|voteboat-matched-framework|phase-diagnostic' | grep -v grep > "$job/quiet-before.txt" || true
test ! -s "$job/quiet-before.txt"
cmd=("$samply" record --save-only --rate 1000 --unstable-presymbolicate --symbol-dir "$candidate/target-matched-release/release" --output "$job/bulk-profile.json.gz" -- "$exe" --engine voteboat --clients 4096 --batch 4 --operations 20000000 --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution false --campaign-timeout-secs 3600)
printf '%q ' "${cmd[@]}" > "$job/bulk.command"; printf '\n' >> "$job/bulk.command"
printf 'record_bulk\n' > "$job/STAGE"
set +e
"${cmd[@]}" > "$job/bulk.stdout.json" 2> "$job/bulk.stderr"
rc=$?
set -e
printf '%s\n' "$rc" > "$job/bulk.exit"
test "$rc" = 0
printf 'identity_after\n' > "$job/STAGE"
/usr/bin/python3 /tmp/voteboat-comparison426-CyZVe6/artifact-guard.py /tmp/voteboat-comparison426-CyZVe6/CONFIG_PAIR.mac.json > "$job/identity-after.log" 2>&1
shasum -a 256 "$samply" "$exe" "$candidate/target-matched-release/release/voteboat-matched-framework.dSYM/Contents/Resources/DWARF/voteboat_matched_framework-0f72fff4df956fc5" > "$job/sha-after.txt"
cmp "$job/sha-before.txt" "$job/sha-after.txt"
