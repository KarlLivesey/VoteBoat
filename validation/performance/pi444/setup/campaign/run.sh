#!/usr/bin/env bash
set -euo pipefail
job=$1
out="$job/campaign-results"
mkdir "$out"
trap 'rc=$?; printf "%s\n" "$rc" > "$out/STATUS"' EXIT
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
export RUSTUP_TOOLCHAIN=1.98.1
export PATH="$HOME/.rustup/toolchains/1.98.1-aarch64-unknown-linux-gnu/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
python3 "$job/guard.py" "$job" source > "$out/source-before.log"
python3 - "$job" > "$out/artifact-before.log" <<'IDENTITY'
from pathlib import Path
import sys,json,hashlib
j=Path(sys.argv[1]);m=json.loads((j/'ARTIFACTS.json').read_text());s=json.loads((j/'SYMBOLS.json').read_text())
for row in m+[s]:
 for field in ['original','copy']:
  assert hashlib.sha256(Path(row[field]).read_bytes()).hexdigest()==row['sha256'],row[field]
print('all4original/copyartifact+DWPhashes exact')
IDENTITY
cp "$job/campaign/SCHEMA.json" "$out/SCHEMA.json"
cp "$job/campaign/CONFIG.json" "$out/CONFIG.json"
for name in measure_temp get_throttled; do vcgencmd "$name" > "$out/before-$name.log"; done
vcgencmd measure_clock arm > "$out/before-frequency.log"
free -h > "$out/before-memory.log"
uptime > "$out/before-load.log"
cat /sys/devices/system/cpu/cpufreq/policy0/scaling_governor > "$out/before-governor.log"
if systemctl is-active apcupsd docker containerd > "$out/before-services.log"; then service_status=0; else service_status=$?; fi
printf '%s\n' "$service_status" > "$out/before-services.exit"
binary=$(python3 -c 'import json,sys;print(next(r["copy"] for r in json.load(open(sys.argv[1])) if r["target"]=="voteboat-matched-framework"))' "$job/ARTIFACTS.json")
failed=0
for cell in '1 1' '64 1' '256 1' '512 1' '1024 1' '4096 1' '4096 4'; do
 read -r clients batch <<< "$cell"
 for trial in 1 2 3; do
  variants=(off on openraft)
  if ((trial==2)); then variants=(openraft off on); fi
  if ((trial==3)); then variants=(on openraft off); fi
  for variant in "${variants[@]}"; do
   engine=voteboat; attribution=false
   if [[ "$variant" == openraft ]]; then engine=openraft; fi
   if [[ "$variant" == on ]]; then attribution=true; fi
   label="$variant-c$clients-b$batch-t$trial";name="$out/$label"
   printf '%s\n' "$label" > "$out/STAGE";printf 'CASE_START=%s\n' "$label"
   cmd=("$binary" --engine "$engine" --clients "$clients" --batch "$batch" --operations 1048576 --warmup 1024 --maintenance default-retention --envelopes upstream-default --attribution "$attribution" --campaign-timeout-secs 3600)
   printf '%q ' "${cmd[@]}" > "$name.command";printf '\n' >> "$name.command"
   if { time "${cmd[@]}" > "$name.json" 2> "$name.stderr"; } 2> "$name.time"; then result=0; else result=$?;failed=1; fi
   printf '%s\n' "$result" > "$name.exit"
   if [[ "$result" == 0 ]]; then
    if python3 "$job/campaign/check-case.py" "$name.json" "$variant" "$clients" "$batch" > "$name.guard.log" 2> "$name.guard.stderr"; then guard=0; else guard=$?;failed=1; fi
    printf '%s\n' "$guard" > "$name.guard.exit"
   fi
   printf 'CASE_END=%s exit=%s\n' "$label" "$result"
  done
 done
done
python3 "$job/guard.py" "$job" source > "$out/source-after.log"
python3 - "$job" > "$out/artifact-after.log" <<'IDENTITY'
from pathlib import Path
import sys,json,hashlib
j=Path(sys.argv[1]);m=json.loads((j/'ARTIFACTS.json').read_text());s=json.loads((j/'SYMBOLS.json').read_text())
for row in m+[s]:
 for field in ['original','copy']:
  assert hashlib.sha256(Path(row[field]).read_bytes()).hexdigest()==row['sha256'],row[field]
print('all4original/copyartifact+DWPhashes exact')
IDENTITY
for name in measure_temp get_throttled; do vcgencmd "$name" > "$out/after-$name.log"; done
vcgencmd measure_clock arm > "$out/after-frequency.log"
free -h > "$out/after-memory.log"
uptime > "$out/after-load.log"
cat /sys/devices/system/cpu/cpufreq/policy0/scaling_governor > "$out/after-governor.log"
if systemctl is-active apcupsd docker containerd > "$out/after-services.log"; then service_status=0; else service_status=$?; fi
printf '%s\n' "$service_status" > "$out/after-services.exit"
printf '%s\n' DONE > "$out/STAGE"
exit "$failed"
