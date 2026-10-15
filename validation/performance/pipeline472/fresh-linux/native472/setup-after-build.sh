#!/usr/bin/env bash
# Actual14 functional controls only. No paired/throughput campaign.
set -euo pipefail
job=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/native472-linux
trap 'rc=$?; printf "%s\n" "$rc" > "$job/SETUP_STATUS"' EXIT
python3 "$job/guard.py" "$job" qualification > "$job/logs/setup-qualification-preflight.log"
python3 "$job/guard.py" "$job" source > "$job/logs/setup-source-before.log"
python3 - <<'PY'
from pathlib import Path
import hashlib,json,re,subprocess
root=Path.cwd();job=root/'target/p9/native472-linux';controls=job/'controls';h=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest();write=lambda p,v:Path(p).write_text(json.dumps(v,indent=2)+'\n')
assert (job/'STATUS').read_text().strip()=='0' and (job/'STAGE').read_text().strip()=='DONE'
assert json.loads((job/'BUILD_TERMINAL.json').read_text())['actual_exit']==0
assert not (controls/'RUN.json').exists()
commit=(job/'REQUIRED_COMMIT.txt').read_text().strip();tools=job/'control-tools';assert not tools.exists();tools.mkdir()
queue=['validation/check-native-benchmark.mjs','validation/check-journal-timings.mjs'];copied={}
while queue:
 relative=queue.pop()
 if relative in copied:continue
 data=subprocess.check_output(['git','show',commit+':'+relative]);target=tools/Path(relative).name;target.write_bytes(data);target.chmod(0o644)
 copied[Path(relative).name]={'sha256':h(target),'mode':'0o644','committed_path':relative}
 queue.extend(str(Path(relative).parent/ref) for ref in re.findall(r"(?:from\s*|import\s*)['\"](\./[^'\"]+)['\"]",data.decode()))
write(tools/'SOURCE.json',{'commit':commit,'files':dict(sorted(copied.items()))})
artifact=next(r for r in json.loads((job/'ARTIFACTS.json').read_text()) if r['target']=='native_benchmark')
assert all(h(artifact[key])==artifact['sha256'] for key in ['original','copy']);write(controls/'ARTIFACT.json',artifact)
source=json.loads((job/'SOURCE.json').read_text());paths=[Path(source['source_root'])/name for name in source['sha256']]+[controls/'run.py',controls/'check-selection.mjs',*tools.iterdir()]
write(controls/'BEFORE.json',{'files':{str(p):{'sha256':h(p),'mode':oct(p.stat().st_mode&0o7777)} for p in paths}})
write(controls/'PLAN.json',{'source_commit':commit,'case_count':4,'actual_command_count':14,'originals_per_case':128,'warmup':64,'recovered_value':192,'append_window_units':4,'scope':'Fresh assertednative472 startup/shared TCP/QUIC functional activation controls; not paired throughput or physical memory acceptance.'})
PY
python3 -u "$job/controls/run.py"
python3 "$job/guard.py" "$job" source > "$job/logs/setup-source-after.log"
printf 'ALL_SETUP_END_NO_TIMING\n' > "$job/SETUP_STAGE"
