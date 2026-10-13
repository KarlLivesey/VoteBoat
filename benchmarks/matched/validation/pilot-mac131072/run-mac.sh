#!/bin/bash
set -euo pipefail
base=/tmp/voteboat-matched230-mac
harness="$base/target-harness/release/voteboat-matched-framework"
upstream="$base/target-upstream/release/bench"
out="$base/pilot-primary131072"
mkdir "$out"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
printf 'bash_builtin_time; RSS unavailable\n' > "$out/timer.txt"
printf '%s  %s\n' 7c20fa0a740b375a4a1b2aab1cba303843fdd1204fc147f52f61ead9867580ec "$harness" b3f2878f4146b4cf03174617a82c979bb9aec8558f3ab8fb2e45918691be8a6f "$upstream" | shasum -a 256 -c > "$out/artifacts-before.log"
(cd "$base/source" && shasum -a 256 -c "$base/harness-source.sha256") > "$out/source-before.log"
(cd /tmp/voteboat-openraft230-mac && shasum -a 256 -c "$base/upstream-source.sha256") > "$out/upstream-before.log"
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
printf '%s  %s\n' 7c20fa0a740b375a4a1b2aab1cba303843fdd1204fc147f52f61ead9867580ec "$harness" b3f2878f4146b4cf03174617a82c979bb9aec8558f3ab8fb2e45918691be8a6f "$upstream" | shasum -a 256 -c > "$out/artifacts-after.log"
(cd "$base/source" && shasum -a 256 -c "$base/harness-source.sha256") > "$out/source-after.log"
(cd /tmp/voteboat-openraft230-mac && shasum -a 256 -c "$base/upstream-source.sha256") > "$out/upstream-after.log"
exit "$failed"
