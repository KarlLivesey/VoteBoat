#!/usr/bin/env bash
set -euo pipefail
job=target/p9/slice454/main
trap 'rc=$?; printf "%s\n" "$rc" > "$job/FOCUSED_STATUS"' EXIT
run(){ name=$1;shift;printf '%q ' "$@" > "$job/$name.command";printf '\n' >> "$job/$name.command";set +e;"$@" > "$job/$name.log" 2> "$job/$name.stderr";rc=$?;set -e;printf '%s\n' "$rc" > "$job/$name.status";printf 'COMMAND_END=%s exit=%s\n' "$name" "$rc";return "$rc"; }
printf '%s\n' "$BASHPID" > "$job/FOCUSED_PID"
printf 'format_write\n' > "$job/STAGE"
run format-write cargo +stable fmt --all
python3 - <<'PY'
from pathlib import Path
import hashlib,json,subprocess,shutil
j=Path('target/p9/slice454/main');b=json.loads((j/'SOURCE_BEFORE.json').read_text());v=dict(b);v['sha256']={r:hashlib.sha256(Path(r).read_bytes()).hexdigest() for r in b['sha256']};v['physical_modes']={r:oct(Path(r).stat().st_mode&0o777) for r in b['sha256']};assert v['physical_modes']==b['physical_modes'];v['changed']=[r for r,h in v['sha256'].items() if h!=b['sha256'][r]];v['new']=[];(j/'SOURCE.json').write_text(json.dumps(v,indent=2)+'\n');(j/'PATCH.diff').write_bytes(subprocess.check_output(['git','diff','--',*v['changed']]))
for r in v['changed']:
 p=j/'source-final'/r;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(r,p)
print('source frozen',v['inputs'],len(v['changed']),'changed')
PY
printf 'focused_transitions\n' > "$job/STAGE"
run focused cargo +stable test --locked --offline --all-features --lib log::transition:: -- --test-threads=1
printf 'FOCUSED_DONE\n' > "$job/STAGE"
