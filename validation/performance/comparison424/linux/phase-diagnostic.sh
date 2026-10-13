#!/usr/bin/env bash
# Frozen41 unified artifact: VB off/on and pinned OpenRaft, never builds.
set -euo pipefail
job=${MATCHED_JOB:?prepared identity/driver directory required}
config=${1:?CONFIG.json required}
out=${2:?fresh diagnostic directory required}
mkdir "$out"
cp "$config" "$out/CONFIG.json"
mode=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["mode"])' "$out/CONFIG.json")
if [[ "$mode" == functional_debug_smoke ]]; then
  operations=16384
  trials=1
elif [[ "$mode" == asserted_release ]]; then
  operations=1048576
  trials=3
else
  printf 'invalid campaign mode\n' >&2
  exit 1
fi
binary=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["executable"])' "$out/CONFIG.json")
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
guards(){ python3 "$job/artifact-guard.py" "$out/CONFIG.json" > "$out/identity-$1.log"; }
guards before
failed=0
for trial in 1 2 3; do
  if (( trial > trials )); then break; fi
  variants=(off on openraft)
  if (( trial == 2 )); then variants=(openraft off on); fi
  if (( trial == 3 )); then variants=(on openraft off); fi
  for variant in "${variants[@]}"; do
    engine=voteboat
    attribution=false
    if [[ "$variant" == openraft ]]; then engine=openraft; fi
    if [[ "$variant" == on ]]; then attribution=true; fi
    name="$out/$variant-c4096-b4-t$trial"
    cmd=("$binary" --engine "$engine" --clients 4096 --batch 4 --operations "$operations" --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution "$attribution" --campaign-timeout-secs 3600)
    printf '%s\n' "$variant-c4096-b4-t$trial" > "$out/STAGE"
    printf '%q ' "${cmd[@]}" > "$name.command"
    printf '\n' >> "$name.command"
    if [[ "$mode" == asserted_release ]]; then
      if { time "${cmd[@]}" > "$name.json" 2> "$name.stderr"; } 2> "$name.time"; then
        printf '0\n' > "$name.exit"
      else
        rc=$?
        printf '%s\n' "$rc" > "$name.exit"
        failed=1
      fi
    else
      # Functional smoke intentionally records no process timing or rate summary.
      if "${cmd[@]}" > "$name.json" 2> "$name.stderr"; then
        printf '0\n' > "$name.exit"
      else
        rc=$?
        printf '%s\n' "$rc" > "$name.exit"
        failed=1
      fi
    fi
  done
done
guards after
python3 "$job/phase-diagnostic-check.py" "$out"
exit "$failed"
