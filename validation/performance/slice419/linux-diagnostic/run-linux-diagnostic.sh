#!/usr/bin/env bash
set -euo pipefail
root=/home/karllivesey/Documents/ChatGPT/VoteBoat
old="$root/target/p9/matched-artifacts418/linux"
new="$root/target/p9/matched-artifacts419/linux-qualified"
baseline="$old/target-harness/release/voteboat-matched-framework"
candidate="$new/target-harness/release/voteboat-matched-framework"
out=${1:?fresh diagnostic directory required}
mkdir "$out"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
printf 'bash_builtin_time; RSS unavailable; diagnostic not sustained capacity\n' > "$out/timer.txt"
guards() {
 local stage=$1
 sha256sum -c "$new/diagnostic-artifacts.sha256" > "$out/artifacts-$stage.log"
 (cd "$old/source" && sha256sum -c "$old/source.sha256") > "$out/baseline-source-$stage.log"
 (cd "$new/source" && sha256sum -c "$new/source.sha256") > "$out/candidate-source-$stage.log"
}
guards before
failed=0
for trial in 1 2 3; do
 implementations=(baseline off on)
 if (( trial == 2 )); then implementations=(on baseline off); fi
 if (( trial == 3 )); then implementations=(off on baseline); fi
 for implementation in "${implementations[@]}"; do
  name="$out/$implementation-c4096-b4-t$trial"
  binary="$candidate"
  if [[ "$implementation" == baseline ]]; then binary="$baseline"; fi
  cmd=("$binary" --engine voteboat --clients 4096 --batch 4 --operations 1048576 --warmup 1024 --maintenance default-retention --envelopes upstream-default)
  if [[ "$implementation" == on ]]; then cmd+=(--attribution true); fi
  if [[ "$implementation" == off ]]; then cmd+=(--attribution false); fi
  printf '%q ' "${cmd[@]}" > "$name.command"
  printf '\n' >> "$name.command"
  if { time "${cmd[@]}" > "$name.json" 2> "$name.stderr"; } 2> "$name.time"; then
   printf '0\n' > "$name.exit"
  else
   rc=$?
   printf '%s\n' "$rc" > "$name.exit"
   failed=1
  fi
 done
done
guards after
exit "$failed"
