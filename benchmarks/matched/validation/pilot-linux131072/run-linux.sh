#!/usr/bin/env bash
set -euo pipefail
base=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/matched-artifacts230/linux
harness="$base/target-harness/release/voteboat-matched-framework"
upstream="$base/target-upstream/release/bench"
out=${1:?new diagnostic result directory required}
mkdir "$out"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
printf 'bash_builtin_time; RSS unavailable\n' > "$out/timer.txt"
printf '%s  %s\n' 83a3e9ccafbb2f48276320b0a8b8ab55177d44bd5b357ec7a02221b8ab12582e "$harness" 30146a5688ea4c287f17dfa5c4926ea8212f0b2a11524bf1b2df24e236a0e7dd "$upstream" | sha256sum -c > "$out/artifacts-before.log"
(cd "$base/source" && sha256sum -c "$base/harness-source.sha256") > "$out/source-before.log"
(cd /tmp/voteboat-openraft230-linux && sha256sum -c "$base/upstream-source.sha256") > "$out/upstream-before.log"
failed=0
for trial in 1 2 3; do
 implementations=(voteboat openraft untouched)
 if (( trial == 2 )); then implementations=(untouched openraft voteboat); fi
 for cell in '1 1' '4096 4'; do
  read -r clients batch <<< "$cell"
  for implementation in "${implementations[@]}"; do
   name="$out/$implementation-c$clients-b$batch-t$trial"
   if [[ "$implementation" == untouched ]]; then
    cmd=("$upstream" --client-workers 1 --server-workers 16 --clients "$clients" --batch "$batch" --operations 131072 --members 3 --warmup-writes 1024 --latency)
   else
    cmd=("$harness" --engine "$implementation" --clients "$clients" --batch "$batch" --operations 131072 --warmup 1024 --maintenance default-retention --envelopes upstream-default)
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
printf '%s  %s\n' 83a3e9ccafbb2f48276320b0a8b8ab55177d44bd5b357ec7a02221b8ab12582e "$harness" 30146a5688ea4c287f17dfa5c4926ea8212f0b2a11524bf1b2df24e236a0e7dd "$upstream" | sha256sum -c > "$out/artifacts-after.log"
(cd "$base/source" && sha256sum -c "$base/harness-source.sha256") > "$out/source-after.log"
(cd /tmp/voteboat-openraft230-linux && sha256sum -c "$base/upstream-source.sha256") > "$out/upstream-after.log"
exit "$failed"
