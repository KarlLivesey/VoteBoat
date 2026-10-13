#!/usr/bin/env bash
set -euo pipefail
job=${MATCHED_JOB:?prepared host-specific identity/driver directory required}
out=${1:?fresh diagnostic directory required}
mkdir "$out"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
guards(){ /usr/bin/python3 "$job/paired-framework-check.py" guards "$job/FRAMEWORK_IDENTITY.json" > "$out/identity-$1.log"; }
guards before
failed=0
for trial in 1 2 3; do
 variants=(baseline off on)
 if (( trial == 2 )); then variants=(on off baseline); fi
 for variant in "${variants[@]}"; do
  artifact=candidate;attribution=false
  if [[ "$variant" == baseline ]]; then artifact=baseline; fi
  if [[ "$variant" == on ]]; then attribution=true; fi
  binary=$(/usr/bin/python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["variants"][sys.argv[2]]["executable"])' "$job/FRAMEWORK_IDENTITY.json" "$artifact")
  name="$out/$variant-c4096-b4-t$trial"
  cmd=("$binary" --engine voteboat --clients 4096 --batch 4 --operations 1048576 --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution "$attribution")
  printf '%s\n' "$variant-c4096-b4-t$trial" > "$out/STAGE"
  printf '%q ' "${cmd[@]}" > "$name.command";printf '\n' >> "$name.command"
  if { time "${cmd[@]}" > "$name.json" 2> "$name.stderr"; } 2> "$name.time"; then
   printf '0\n' > "$name.exit"
  else
   rc=$?;printf '%s\n' "$rc" > "$name.exit";failed=1
  fi
 done
done
guards after
/usr/bin/python3 "$job/phase-diagnostic-check.py" "$out"
exit "$failed"
