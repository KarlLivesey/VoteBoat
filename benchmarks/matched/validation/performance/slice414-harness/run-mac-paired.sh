#!/usr/bin/env bash
set -euo pipefail
old=/tmp/voteboat-matched230-mac
new=/tmp/voteboat-matched414-mac
baseline="$old/target-harness/release/voteboat-matched-framework"
candidate="$new/target-harness/release/voteboat-matched-framework"
upstream="$old/target-upstream/release/bench"
out="$new/paired-primary131072"
mkdir "$out"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
printf 'bash_builtin_time; RSS unavailable\n' > "$out/timer.txt"
guards(){
 local stage=$1
 printf '%s  %s\n' 7c20fa0a740b375a4a1b2aab1cba303843fdd1204fc147f52f61ead9867580ec "$baseline" df6f1fc5c122a30034ec5d6ba0fd42ca1099121f1558392700698072ba9ee9fd "$candidate" b3f2878f4146b4cf03174617a82c979bb9aec8558f3ab8fb2e45918691be8a6f "$upstream" | shasum -a 256 -c > "$out/artifacts-$stage.log"
 (cd "$old/source" && shasum -a 256 -c "$old/harness-source.sha256") > "$out/baseline-source-$stage.log"
 (cd "$new/source-final" && shasum -a 256 -c "$new/source-final.sha256") > "$out/candidate-source-$stage.log"
 (cd /tmp/voteboat-openraft230-mac && shasum -a 256 -c "$old/upstream-source.sha256") > "$out/upstream-source-$stage.log"
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
