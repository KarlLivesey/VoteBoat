#!/usr/bin/env bash
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice431/candidate-pressure
frozen=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice431
trap 'rc=$?; printf "%s\n" "$rc" > "$job/PRESSURE_STATUS"' EXIT
export TMPDIR="$job/test-tmp" RUST_BACKTRACE=1
mkdir -p "$TMPDIR"
cd "$frozen/source"
exe=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice429/checks/debug/deps/effect_owner-3b9732cae5c45d5b
snapshot(){
 python3 - "$job" "$1" <<'PY'
from pathlib import Path
import datetime,json,os,sys
j=Path(sys.argv[1]);side=sys.argv[2];tmp=Path(os.environ['TMPDIR']);baseline=j.parents[1]/'baseline430-pressure/test-tmp'
# Source/runtime environment observations only: no kernel policy writes or inferred causes.
data={'utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':os.getpid(),'environment':{k:os.environ.get(k) for k in ['TMPDIR','RUST_BACKTRACE','RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','RUSTC_WRAPPER','RUSTC_WORKSPACE_WRAPPER','LD_LIBRARY_PATH','PATH']},'tmp_device':tmp.stat().st_dev,'proc':{}}
for p in ['/proc/loadavg','/proc/meminfo','/proc/stat','/proc/diskstats','/proc/pressure/cpu','/proc/pressure/memory','/proc/pressure/io']:
 try:data['proc'][p]=Path(p).read_text()
 except OSError as e:data['proc'][p]={'error':str(e)}
(j/'logs'/('environment-'+side+'.json')).write_text(json.dumps(data,indent=2)+'\n')
PY
}
printf '%s\n' BEFORE > "$job/PRESSURE_STAGE"
sha256sum -c "$frozen/source.sha256" > "$job/logs/pressure-source-before.log"
printf '%s  %s\n' 855e553ffb7e5d53874c8d03f699d06763bdf37207fe1a9426369238449a1e2c "$exe" | sha256sum -c > "$job/logs/pressure-artifact-before.log"
snapshot before
cmd=("$exe" native::native_shared_peer_quota_pressure_reconnect_restart_and_retry --exact --nocapture --test-threads=1)
printf '%q ' "${cmd[@]}" > "$job/logs/pressure.command"; printf '\n' >> "$job/logs/pressure.command"
printf '%s\n' RUNNING > "$job/PRESSURE_STAGE"
TIMEFORMAT=$'elapsed_seconds=%R\nuser_seconds=%U\nsystem_seconds=%S'
set +e
{ time "${cmd[@]}" > "$job/logs/pressure.log" 2> "$job/logs/pressure.stderr"; } 2> "$job/logs/pressure.time"
rc=$?
set -e
printf '%s\n' "$rc" > "$job/logs/pressure.status"
snapshot after
sha256sum -c "$frozen/source.sha256" > "$job/logs/pressure-source-after.log"
printf '%s  %s\n' 855e553ffb7e5d53874c8d03f699d06763bdf37207fe1a9426369238449a1e2c "$exe" | sha256sum -c > "$job/logs/pressure-artifact-after.log"
printf '%s\n' DONE > "$job/PRESSURE_STAGE"
exit "$rc"
