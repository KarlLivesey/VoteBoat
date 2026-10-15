#!/usr/bin/env bash
# Read-only disclosure outside campaign timing; no policy/settings changes.
set -euo pipefail
stage=${1:?before or after}
case "$stage" in before|after) ;; *) exit 2;; esac
job="$PWD/target/p9/native475-linux/sweep"
out="$job/host-$stage"
test ! -e "$out"
mkdir "$out"
python3 - "$out" <<'PY'
from pathlib import Path
import collections,datetime,json,sys
out=Path(sys.argv[1]);base=Path('/sys/devices/system/cpu');fields=['scaling_driver','scaling_governor','energy_performance_preference','scaling_min_freq','scaling_max_freq'];values={}
for name in fields:
 counts=collections.Counter(p.read_text().strip()for p in base.glob('cpu[0-9]*/cpufreq/'+name));values[name]=dict(sorted(counts.items()))
values['online']=(base/'online').read_text().strip();values['time_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();values['scope']='Read-only policy observations; frequencies are configured kHz bounds, not sampled workload frequencies. No settings changed.'
(out/'CPU.json').write_text(json.dumps(values,indent=2)+'\n')
PY
ps -eo pid,comm,%cpu,%mem,etime --sort=-%cpu | head -25 > "$out/processes.txt"
findmnt -T "$job/campaign-wal" -o TARGET,SOURCE,FSTYPE,OPTIONS --json > "$out/filesystem.json"
stat -f "$job/campaign-wal" > "$out/filesystem-stat.txt"
lsattr -d "$job/campaign-wal" > "$out/directory-attributes.txt"
python3 - "$out" <<'PY'
from pathlib import Path
import hashlib,json,sys
out=Path(sys.argv[1]);h=lambda p:hashlib.sha256(p.read_bytes()).hexdigest();(out/'RECEIPT.json').write_text(json.dumps({'files':{p.name:{'sha256':h(p),'bytes':p.stat().st_size}for p in sorted(out.iterdir())if p.is_file()},'scope':'Read-only snapshot outside campaign timing; no settings, source, artifacts or case configuration changed. Process CPU percentages are lifetime observations, not attribution to measured cases.'},indent=2)+'\n')
PY
