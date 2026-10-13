#!/usr/bin/env bash
set -euo pipefail
root=/home/karllivesey/Documents/ChatGPT/VoteBoat
old="$root/target/p9/matched-artifacts230"
new="$root/target/p9/matched-artifacts414"
baseline="$old/linux/target-harness/release/voteboat-matched-framework"
candidate="$new/linux/target-harness/release/voteboat-matched-framework"
upstream="$old/linux/target-upstream/release/bench"
out=${1:?fresh paired result directory required}
mkdir "$out"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
printf 'bash_builtin_time; RSS unavailable\n' > "$out/timer.txt"
guards(){
 local stage=$1
 printf '%s  %s\n' 83a3e9ccafbb2f48276320b0a8b8ab55177d44bd5b357ec7a02221b8ab12582e "$baseline" 334b2aaec04cf14877af756710b17ddaf405ba8497dab5da76d57de4ef44e79f "$candidate" 30146a5688ea4c287f17dfa5c4926ea8212f0b2a11524bf1b2df24e236a0e7dd "$upstream" | sha256sum -c > "$out/artifacts-$stage.log"
 (cd "$old/baseline8d59724" && sha256sum -c "$old/baseline8d59724-source.sha256") > "$out/baseline-source-$stage.log"
 (cd "$new/linux/source" && sha256sum -c "$new/linux/source.sha256") > "$out/candidate-source-$stage.log"
 (cd /tmp/voteboat-openraft230-linux && sha256sum -c "$old/linux/upstream-source.sha256") > "$out/upstream-source-$stage.log"
}
guards before
failed=0
for trial in 1 2 3; do
 implementations=(baseline candidate untouched)
 if (( trial == 2 )); then implementations=(untouched candidate baseline); fi
 for cell in '1 1' '4096 4'; do
  read -r clients batch <<< "$cell"
  for implementation in "${implementations[@]}"; do
   name="$out/$implementation-c$clients-b$batch-t$trial"
   if [[ "$implementation" == untouched ]]; then
    cmd=("$upstream" --client-workers 1 --server-workers 16 --clients "$clients" --batch "$batch" --operations 131072 --members 3 --warmup-writes 1024 --latency)
   else
    binary="$baseline"
    if [[ "$implementation" == candidate ]]; then binary="$candidate"; fi
    cmd=("$binary" --engine voteboat --clients "$clients" --batch "$batch" --operations 131072 --warmup 1024 --maintenance default-retention --envelopes upstream-default)
   fi
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
exit "$failed"
