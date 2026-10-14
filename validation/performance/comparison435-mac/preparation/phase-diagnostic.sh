#!/usr/bin/env bash
# Exact434 baseline versus qualified435; never builds or changes source.
set -euo pipefail
job=${MATCHED_JOB:?prepared identity/driver directory required}
config=${1:?CONFIG_PAIR.json required}
out=${2:?fresh diagnostic directory required}
cell=${3:?bulk or singleton cell required}
[[ "$cell" == bulk || "$cell" == singleton ]] || exit 103
mkdir "$out"
cp "$config" "$out/CONFIG_PAIR.json"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
guards(){ python3 "$job/artifact-guard.py" "$out/CONFIG_PAIR.json" > "$out/identity-$1.log"; }
guards before
baseline=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["baseline"]["executable"])' "$out/CONFIG_PAIR.json")
candidate=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["candidate"]["executable"])' "$out/CONFIG_PAIR.json")
failed=0
for selected in "$cell"; do
  clients=4096; batch=4; operations=20000000
  if [[ "$cell" == singleton ]]; then clients=1; batch=1; operations=1048576; fi
  for trial in 1 2 3; do
    variants=(baseline candidate on openraft)
    if (( trial == 2 )); then variants=(openraft baseline candidate on); fi
    if (( trial == 3 )); then variants=(on openraft baseline candidate); fi
    for variant in "${variants[@]}"; do
      binary=$candidate; engine=voteboat; attribution=false
      if [[ "$variant" == baseline ]]; then binary=$baseline; fi
      if [[ "$variant" == openraft ]]; then engine=openraft; fi
      if [[ "$variant" == on ]]; then attribution=true; fi
      name="$out/$variant-c$clients-b$batch-t$trial"
      cmd=("$binary" --engine "$engine" --clients "$clients" --batch "$batch" --operations "$operations" --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution "$attribution" --campaign-timeout-secs 3600)
      printf '%s\n' "$variant-c$clients-b$batch-t$trial" > "$out/STAGE"
      printf '%q ' "${cmd[@]}" > "$name.command"; printf '\n' >> "$name.command"
      if { time "${cmd[@]}" > "$name.json" 2> "$name.stderr"; } 2> "$name.time"; then
        printf '0\n' > "$name.exit"
      else
        rc=$?; printf '%s\n' "$rc" > "$name.exit"; failed=1
      fi
    done
  done
done
guards after
python3 "$job/phase-diagnostic-check.py" "$out" "$cell"
exit "$failed"
