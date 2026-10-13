#!/usr/bin/env bash
set -euo pipefail
job=/tmp/voteboat-mac415416-8a5840d
out=${1:?fresh paired result directory required}
mkdir "$out"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
printf 'bash_builtin_time; RSS unavailable\n' > "$out/timer.txt"
guards(){ /usr/bin/python3 "$job/paired-framework-check.py" guards "$job/FRAMEWORK_IDENTITY.json" > "$out/identity-$1.log"; }
guards before
failed=0
for trial in 1 2 3; do
 implementations=(baseline candidate untouched)
 if (( trial == 2 )); then implementations=(untouched candidate baseline); fi
 for cell in '1 1' '4096 4'; do
  read -r clients batch <<< "$cell"
  for implementation in "${implementations[@]}"; do
   name="$out/$implementation-c$clients-b$batch-t$trial"
   binary=$(/usr/bin/python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["variants"][sys.argv[2]]["executable"])' "$job/FRAMEWORK_IDENTITY.json" "$implementation")
   if [[ "$implementation" == untouched ]]; then
    cmd=("$binary" --client-workers 1 --server-workers 16 --clients "$clients" --batch "$batch" --operations 131072 --members 3 --warmup-writes 1024 --latency)
   else
    cmd=("$binary" --engine voteboat --clients "$clients" --batch "$batch" --operations 131072 --warmup 1024 --maintenance default-retention --envelopes upstream-default)
   fi
   printf '%s\n' "$implementation-c$clients-b$batch-t$trial" > "$out/STAGE"
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
/usr/bin/python3 "$job/paired-framework-check.py" outcomes "$out"
exit "$failed"
