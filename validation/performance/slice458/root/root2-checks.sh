#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice458/root2
mkdir -p "$job"
export CARGO_TARGET_DIR=/home/karllivesey/Documents/ChatGPT/VoteBoat/target/p9/slice455/padded-target-checks
trap 'rc=$?;printf "%s\n" "$rc" > "$job/STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/PID"
run example-build cargo +stable build --locked --offline --all-features --example native_benchmark
run pre-root-controls python3 - <<'PY'
from pathlib import Path
import subprocess,json,hashlib
j=Path('target/p9/slice458/root2');exe=Path('target/p9/slice455/padded-target-checks/debug/examples/native_benchmark').resolve();cases=[('zero',['10','1','2','--offered','1','--group-weights','0,1']),('count',['10','1','2','--offered','1','--group-weights','1,1,1']),('total',['10','1','2','--offered','1','--group-weights','256,1']),('missing',['10','1','2','--offered','1','--group-weights']),('duplicate',['10','1','2','--offered','1','--group-weights','1,3','--group-weights','1,3']),('mode',['10','1','2','--group-weights','1,3']),('hot-capacity',['20000','32','2','--offered','100','--group-weights','255,1'])];out=[]
for label,args in cases:
 root=(j/('must-not-exist-'+label)).resolve();assert not root.exists();cmd=[str(exe),str(root),'tcp',*args];p=subprocess.run(cmd,capture_output=True,text=True);(j/(label+'.command.json')).write_text(json.dumps(cmd)+'\n');(j/(label+'.stdout')).write_text(p.stdout);(j/(label+'.stderr')).write_text(p.stderr);assert p.returncode!=0,label;assert not root.exists(),label
 if label=='hot-capacity':assert 'default per-group log capacity' in p.stderr,p.stderr
 out.append({'case':label,'exit':p.returncode,'root_created':False,'stderr':p.stderr.strip()})
(j/'PRE_ROOT.json').write_text(json.dumps({'binary':str(exe),'sha256':hashlib.sha256(exe.read_bytes()).hexdigest(),'scope':'Debug command validation only; no capacity measurement','cases':out},indent=2)+'\n');print('7 invalid-weight/capacity/mode CLI cases reject before root creation')
PY
run enabled-hook .githooks/pre-push
printf 'DONE\n' > "$job/STAGE"
